/*
   Copyright 2018 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

// Package appstats provides an adaptable wrapper for stats libraries.
package appstats

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"
	"unicode"
)

type (
	// Service models something that allows stats to be sent.
	Service interface {
		io.Closer
		// Flush indicates that any buffered data should be sent asap.
		Flush() error
		// Bucket allows access to send stats for a given key or identifier.
		Bucket(bucket interface{}) Bucket
	}

	// Bucket models where to send some stats.
	Bucket interface {
		// Tag can be used to aggregate stats, and returns a new bucket appended with the desired tags.
		Tag(key interface{}, values ...interface{}) Bucket
		// Count models stats in the form of a running total, e.g. number of errors, which could be used to calculate
		// number of errors in the last 5 minutes, for example, n should be a number.
		Count(n interface{})
		// Increment is shorthand for Count(1).
		Increment()
		// Gauge models a statistic that can be represented as a current value within a range of expected values,
		// relevant for the current time, e.g. the current memory usage.
		Gauge(value interface{})
		// Histogram models time series numeric data, e.g. the number of connections over time.
		Histogram(value interface{})
		// Unique models values that should be logged as occurrences, e.g. unique UUID values representing users.
		Unique(value interface{})
		// Timing models the time something takes, e.g. from the start to the end of a specific operation.
		Timing(value interface{})
	}

	// StatsDClient matches the implementation provided by github.com/alexcesaro/statsd
	StatsDClient interface {
		Close()
		Count(bucket string, n interface{})
		Flush()
		Gauge(bucket string, value interface{})
		Histogram(bucket string, value interface{})
		Increment(bucket string)
		Timing(bucket string, value interface{})
		Unique(bucket string, value string)
	}

	// BucketInfo provides a store for bucket and tag key-values where they all must be normalised to strings anyway.
	BucketInfo struct {
		Bucket string
		Tags   map[string][]string
	}

	// BucketKeyFunc is used to generate a bucket key string for actually sending the metrics, note that while
	// this implementation provides DefaultBucketKeyFunc and NewBucketKeyFunc, it is completely acceptable to simply
	// implement your own.
	BucketKeyFunc func(info BucketInfo) (name string, ok bool)

	// Tagger models something that may apply additional tags to a Bucket, and is intended to be used to provide
	// optional / generic tag / externally validated tag configuration, when implementing your own stats utilities.
	Tagger func(bucket Bucket) (Bucket, error)
)

// TagMapStringInterface returns a new Tagger that will apply all keys and values to a bucket.
func TagMapStringInterface(m map[string]interface{}) Tagger {
	return func(bucket Bucket) (Bucket, error) {
		for k, v := range m {
			bucket = bucket.Tag(k, v)
		}
		return bucket, nil
	}
}

// TagMapInterfaceInterface returns a new Tagger that will apply all keys and values to a bucket.
func TagMapInterfaceInterface(m map[interface{}]interface{}) Tagger {
	return func(bucket Bucket) (Bucket, error) {
		for k, v := range m {
			bucket = bucket.Tag(k, v)
		}
		return bucket, nil
	}
}

// TagMapStringString returns a new Tagger that will apply all keys and values to a bucket.
func TagMapStringString(m map[string]string) Tagger {
	return func(bucket Bucket) (Bucket, error) {
		for k, v := range m {
			bucket = bucket.Tag(k, v)
		}
		return bucket, nil
	}
}

// Apply will pass bucket to the tagger, and will error if the bucket is nil, and simply return the original bucket
// if the receiver is nil, note that the returned bucket is itself validated to be non-nil, so this method will
// never return (nil, nil).
func (t Tagger) Apply(bucket Bucket) (Bucket, error) {
	if bucket == nil {
		return nil, errors.New("appstats.Tagger.Apply nil bucket")
	}
	if t == nil {
		return bucket, nil
	}
	bucket, err := t(bucket)
	if err != nil {
		return nil, fmt.Errorf("appstats.Tagger.Apply tagger error: %s", err.Error())
	}
	if bucket == nil {
		name := "unknown"
		ptr := reflect.ValueOf(t).Pointer()
		if ptr != 0 {
			name = runtime.FuncForPC(ptr).Name()
		}
		return nil, fmt.Errorf("appstats.Tagger.Apply nil bucket for tagger: %s", name)
	}
	return bucket, nil
}

// ApplyTaggers will pass bucket through all taggers provided, and will return an error if bucket is nil, or applying
// any of the taggers failed.
func ApplyTaggers(bucket Bucket, taggers ...Tagger) (Bucket, error) {
	if bucket == nil {
		return nil, errors.New("appstats.ApplyTaggers nil bucket")
	}
	for i, tagger := range taggers {
		var err error
		bucket, err = tagger.Apply(bucket)
		if err != nil {
			return nil, fmt.Errorf("appstats.ApplyTaggers tagger error at index %d: %s", i, err.Error())
		}
	}
	return bucket, nil
}

// DefaultBucketKeyFunc is the default func used to generate bucket keys, it applies SanitiseKey to the bucket,
// tag keys, and tag values, ensuring there is a non-empty bucket, and filtering any empty tags and values, note
// that only the LAST value for each tag BEFORE FILTERING, so a tag with key "key" and  values ("value", "123") would
// not be appended, as SanitiseKey("123") returns "".
// The output format is like "bucket,tag1=value,tag2=id_value", which aligns with InfluxDB's line protocol spec
// https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_tutorial
func DefaultBucketKeyFunc(info BucketInfo) (string, bool) {
	return defaultBucketKeyFunc(info)
}

var defaultBucketKeyFunc = NewBucketKeyFunc(SanitiseKey)

// NewBucketKeyFunc provides the same implementation as DefaultBucketKeyFunc, but with the ability to specify a
// custom key sanitiser, note that it will panic if keySanitiser is nil.
func NewBucketKeyFunc(keySanitiser func(value string) string) BucketKeyFunc {
	if keySanitiser == nil {
		panic(errors.New("appstats.NewBucketKeyFunc nil key sanitiser"))
	}

	return func(info BucketInfo) (name string, ok bool) {
		bucket := bytes.NewBufferString(keySanitiser(info.Bucket))

		if bucket.Len() == 0 {
			return "", false
		}

		tags := make(sortStringsBytesCompare, 0, len(info.Tags))
		values := make(map[string][]string)

		for tag, tagValues := range info.Tags {
			tag = keySanitiser(tag)

			if tag == "" {
				continue
			}

			if _, ok := values[tag]; !ok {
				tags = append(tags, tag)
				values[tag] = nil
			}

			values[tag] = append(values[tag], tagValues...)
		}

		sort.Sort(tags)

		for _, tag := range tags {
			if numValues := len(values[tag]); numValues > 0 {
				if value := keySanitiser(values[tag][numValues-1]); value != "" {
					bucket.WriteRune(',')
					bucket.WriteString(tag)
					bucket.WriteRune('=')
					bucket.WriteString(value)
				}
			}
		}

		return bucket.String(), true
	}
}

// NewStatsDService wraps https://github.com/alexcesaro/statsd, note both args may be nil, defaults will be used.
func NewStatsDService(
	client StatsDClient,
	keyFunc BucketKeyFunc,
) Service {
	if client == nil {
		client = statsDClientStub{}
	}
	if keyFunc == nil {
		keyFunc = DefaultBucketKeyFunc
	}
	return statsDService{
		client:  client,
		keyFunc: keyFunc,
	}
}

// Tag values to a key (or just ensures the key exists, if there are no values), note that the returned value will
// not modify the value of the source but MAY NOT be a complete deep copy.
func (b *BucketInfo) Tag(key interface{}, values ...interface{}) *BucketInfo {
	keyStr := fmt.Sprint(key)

	r := &BucketInfo{
		Tags: map[string][]string{
			keyStr: nil,
		},
	}

	if b != nil {
		r.Bucket = b.Bucket

		for k, v := range b.Tags {
			r.Tags[k] = v
		}
	}

	vn := make([]string, 0, len(r.Tags[keyStr])+len(values))

	vn = append(vn, r.Tags[keyStr]...)

	for _, v := range values {
		vn = append(vn, fmt.Sprint(v))
	}

	r.Tags[keyStr] = vn

	return r
}

// SanitiseKey sanitises a string key according to the best practice for tags provided by datadog, see
// https://docs.datadoghq.com/getting_started/tagging/#tags-best-practices
func SanitiseKey(value string) string {
	b := new(bytes.Buffer)

	for _, r := range []rune(value) {
		if b.Len() >= 200 {
			break
		}

		r = unicode.ToLower(r)

		if b.Len() == 0 {
			if unicode.IsLetter(r) {
				b.WriteRune(r)
			}
			continue
		}

		if unicode.IsLetter(r) ||
			unicode.IsNumber(r) ||
			r == '_' ||
			r == '-' ||
			r == ':' ||
			r == '.' ||
			r == '/' ||
			r == '\\' {
			b.WriteRune(r)
			continue
		}

		b.WriteRune('_')
	}

	for runes := []rune(b.String()); b.Len() > 200 || (len(runes) > 0 && runes[len(runes)-1] == ':'); runes = []rune(b.String()) {
		b.Truncate(b.Len() - len([]byte(string([]rune{runes[len(runes)-1]}))))
	}

	return b.String()
}

// TimingToDuration attempts to convert a value to a duration to be used in timing calls, normalising various data
// types, supporting a multiplier for types without clearly defined units, and takes advantage of the
// trunc-towards-zero behavior of the big.Float.Int64 method, it also supports strings generated from time.Duration.
func TimingToDuration(value interface{}, multi time.Duration) (d time.Duration, ok bool) {
	if multi <= 0 {
		return
	}

	switch value := value.(type) {
	case time.Time:
		d, ok = timeNow().Sub(value), true
		return

	case time.Duration:
		d, ok = value, true
		return
	}

	// attempt to normalise the value via string using math/big
	var (
		s = fmt.Sprint(value)
		r *big.Rat
	)
	if r, ok = stringToRat(s); !ok {
		// fallback to parsing as time.Duration
		var err error
		d, err = time.ParseDuration(s)
		ok = err == nil
		return
	}

	// apply the multiplier
	r.Mul(r, new(big.Rat).SetInt64(int64(multi)))
	// convert to big.Float then to an int64 (trucs towards zero)
	i, _ := new(big.Float).SetRat(r).Int64()
	// and that's our timing
	d = time.Duration(i)
	return
}

func stringToRat(s string) (*big.Rat, bool) {
	return new(big.Rat).SetString(strings.Map(
		func(r rune) rune {
			if r == ',' || unicode.IsSpace(r) {
				return -1
			}
			return r
		},
		s,
	))
}

var (
	timeNow = time.Now
)

// QuoteString will surround a string in double quotes, and escape all double quotes within the string with a
// backslash, and all backslashes with a backslash, as well.
func QuoteString(s string) string {
	// backslashes must be escaped first
	s = strings.Replace(s, `\`, `\\`, -1)
	// then double quotes
	s = strings.Replace(s, `"`, `\"`, -1)
	// and quote the lot
	return `"` + s + `"`
}

type sortStringsBytesCompare []string

func (s sortStringsBytesCompare) Less(i, j int) bool {
	if bytes.Compare([]byte(s[i]), []byte(s[j])) < 0 {
		return true
	}
	return false
}

func (s sortStringsBytesCompare) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortStringsBytesCompare) Len() int {
	return len(s)
}
