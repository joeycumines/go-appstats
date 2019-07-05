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

package appstats

import (
	"fmt"
	"time"
)

type (
	statsDService struct {
		client  StatsDClient
		keyFunc BucketKeyFunc
	}

	statsDBucket struct {
		service statsDService
		bucket  *BucketInfo
	}

	statsDClientStub struct{}
)

func (statsDClientStub) Close() {
}

func (statsDClientStub) Count(bucket string, n interface{}) {
}

func (statsDClientStub) Flush() {
}

func (statsDClientStub) Gauge(bucket string, value interface{}) {
}

func (statsDClientStub) Histogram(bucket string, value interface{}) {
}

func (statsDClientStub) Increment(bucket string) {
}

func (statsDClientStub) Timing(bucket string, value interface{}) {
}

func (statsDClientStub) Unique(bucket string, value string) {
}

// Close calls statsd.Client.Close.
func (s statsDService) Close() error {
	s.client.Close()
	return nil
}

// Flush calls statsd.Client.Flush.
func (s statsDService) Flush() error {
	s.client.Flush()
	return nil
}

// Bucket returns a new bucket with no tags, string formatting the bucket value with `%v`.
func (s statsDService) Bucket(b interface{}) Bucket {
	return statsDBucket{
		service: s,
		bucket: &BucketInfo{
			Bucket: fmt.Sprint(b),
		},
	}
}

// Tag returns a bucket with the tag and possibly values appended, string formatting all args with `%v`, note that
// this WILL NOT modify the original bucket.
func (b statsDBucket) Tag(key interface{}, values ...interface{}) Bucket {
	return statsDBucket{
		service: b.service,
		bucket:  b.bucket.Tag(key, values...),
	}
}

// Count passes through directly to statsd.Client.Count.
func (b statsDBucket) Count(n interface{}) {
	if bucket := b.bucketKey(); bucket != "" {
		b.service.client.Count(bucket, n)
	}
}

// Increment passes through directly to statsd.Client.Increment.
func (b statsDBucket) Increment() {
	if bucket := b.bucketKey(); bucket != "" {
		b.service.client.Increment(bucket)
	}
}

// Gauge passes through directly to statsd.Client.Gauge.
func (b statsDBucket) Gauge(value interface{}) {
	if bucket := b.bucketKey(); bucket != "" {
		b.service.client.Gauge(bucket, value)
	}
}

// Histogram passes through directly to statsd.Client.Histogram.
func (b statsDBucket) Histogram(value interface{}) {
	if bucket := b.bucketKey(); bucket != "" {
		b.service.client.Histogram(bucket, value)
	}
}

// Unique sends the value to the bucket by passing through to statsd.Client.Unique after converting it to a string,
// applying QuoteString to it, in order to ensure that it parses properly.
func (b statsDBucket) Unique(value interface{}) {
	if bucket := b.bucketKey(); bucket != "" {
		b.service.client.Unique(bucket, QuoteString(fmt.Sprint(value)))
	}
}

// Timing connects to statsd.Client.Timing, which expects a numeric value in millisecond granularity, and accepts
// time.Duration, time.Time (to now), and any other nanosecond values that can be parsed by TimingToDuration (e.g.
// raw ints, strings like "12315213.0", etc).
// Invalid values will be ignored.
func (b statsDBucket) Timing(value interface{}) {
	if bucket := b.bucketKey(); bucket != "" {
		if d, ok := TimingToDuration(value, time.Nanosecond); ok {
			b.service.client.Timing(bucket, int(d/time.Millisecond))
		}
	}
}

func (b statsDBucket) bucketKey() string {
	if b.service.client == nil {
		return ""
	}
	if b.service.keyFunc == nil {
		return ""
	}
	if b.bucket == nil {
		return ""
	}
	v, ok := b.service.keyFunc(*b.bucket)
	if !ok {
		return ""
	}
	return v
}
