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
	"io"
	"fmt"
	"time"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"math"
	"bytes"
	"sort"
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
		Tag(key interface{}, values ... interface{}) Bucket
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

	// BucketKeyFunc is used to generate a bucket key string for actually sending the metrics.
	BucketKeyFunc func(info BucketInfo) (name string, ok bool)
)

// DefaultBucketKeyFunc is the default func used to generate bucket keys, it applies SanitiseKey to the bucket,
// tag keys, and tag values, ensuring there is a non-empty bucket, and filtering any empty tags and values, note
// that only the LAST value for each tag BEFORE FILTERING, so a tag with key "key" and  values ("value", "123") would
// not be appended, as SanitiseKey("123") returns "".
// The output format is like "bucket,tag1=value,tag2=id_value", which aligns with InfluxDB's line protocol spec
// https://docs.influxdata.com/influxdb/v1.4/write_protocols/line_protocol_tutorial
func DefaultBucketKeyFunc(info BucketInfo) (string, bool) {
	bucket := bytes.NewBufferString(SanitiseKey(info.Bucket))

	if bucket.Len() == 0 {
		return "", false
	}

	tags := make(sortStringsBytesCompare, 0, len(info.Tags))
	values := make(map[string][]string)

	for tag, tagValues := range info.Tags {
		tag = SanitiseKey(tag)

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
			if value := SanitiseKey(values[tag][numValues-1]); value != "" {
				bucket.WriteRune(',')
				bucket.WriteString(tag)
				bucket.WriteRune('=')
				bucket.WriteString(value)
			}
		}
	}

	return bucket.String(), true
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
func (b *BucketInfo) Tag(key interface{}, values ... interface{}) *BucketInfo {
	keyStr := fmt.Sprintf("%v", key)

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
		vn = append(vn, fmt.Sprintf("%v", v))
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
// types, supporting a multiplier for types without clearly defined units, note that time.Time values will be
// calculated as now - value, and a multi <= 0 will always return (0, false)
func TimingToDuration(value interface{}, multi time.Duration) (d time.Duration, ok bool) {
	if multi <= 0 {
		return
	}

	switch v := value.(type) {
	case time.Time:
		d, ok = timeNow().Sub(v), true

		return
	case time.Duration:
		d, ok = v, true

		return
	}

	var (
		integer     int64
		fractional  float64
		exponential int
	)

	integer, fractional, exponential, ok = StringToNumber(fmt.Sprintf("%v", value))

	if !ok {
		return
	}

	var (
		a int64
		b int64
	)

	if fractional == 0 && exponential == 0 {
		a, b = integer, int64(multi)
	} else {
		float := (float64(integer) + fractional) * math.Pow10(exponential)

		if float > float64(mostPositive) || float < float64(mostNegative) {
			ok = false

			return
		}

		if i := int64(float); float64(i) == float {
			a, b = i, int64(multi)
		} else if signedMulOverflows(i, int64(multi)) {
			ok = false

			return
		} else if f := float * float64(multi); f <= float64(mostPositive) && f >= float64(mostNegative) {
			a, b = int64(f), 1
		} else {
			ok = false

			return
		}

		if float >= 0 {
			if a < 0 {
				ok = false

				return
			}
		} else {
			if a >= 0 {
				ok = false

				return
			}
		}
	}

	ok = !signedMulOverflows(a, b)

	if !ok {
		return
	}

	d = time.Duration(a * b)

	return
}

// StringToNumber converts a string to a number, separating out like integer.fractional x 10 ^ exponential, where
// any sign will be included on both integer and fractional, and (x10^, e, *10^) are all supported (case insensitive),
// note that any commas or whitespace will be stripped before parsing. The result for ok will be false if parsing
// failed - e.g. if the integer segment was too large for an int64, or it did not match the expected format.
func StringToNumber(s string) (integer int64, fractional float64, exponential int, ok bool) {
	s = strings.Map(
		func(r rune) rune {
			if unicode.IsSpace(r) || r == ',' {
				return -1
			}
			return r
		},
		s,
	)
	sm := regexStringToNumber.FindStringSubmatch(s)
	smLen := len(sm)
	if smLen == 0 {
		return
	}
	ok = true
	var sign string
	if smLen > 1 {
		sign = sm[1]
	}
	if smLen > 2 && sm[2] != "" {
		if v, err := strconv.ParseInt(sign+sm[2], 10, 64); err != nil {
			ok = false
		} else {
			integer = v
		}
	}
	if smLen > 3 && sm[3] != "" {
		if v, err := strconv.ParseFloat(sign+"0."+sm[3], 64); err != nil {
			ok = false
		} else {
			fractional = v
		}
	}
	if smLen > 4 && sm[4] != "" {
		if v, err := strconv.Atoi(sm[4]); err != nil {
			ok = false
		} else {
			exponential = v
		}
	}
	return
}

const (
	mostPositive = 1<<63 - 1
	mostNegative = -(mostPositive + 1)
)

var (
	regexStringToNumber *regexp.Regexp
	timeNow             = time.Now
)

func init() {
	regexStringToNumber = regexp.MustCompile(`^(?i:((?:)|(?:\+)|(?:-))(\d+)(?:(?:)|(?:\.(\d+)))(?:(?:)|(?:(?:(?:x10\^)|(?:\*10\^)|(?:e))((?:(?:)|(?:\+)|(?:-))\d+))))$`)
}

// http://grokbase.com/p/gg/golang-nuts/148wvnxk76/go-nuts-re-test-for-an-integer-overflow
func signedMulOverflows(a, b int64) bool {
	if a == 0 || b == 0 || a == 1 || b == 1 {
		return false
	}
	if a == mostNegative || b == mostNegative {
		return true
	}
	c := a * b
	return c/b != a
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
