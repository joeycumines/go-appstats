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
	"testing"
	"fmt"
	"time"
)

type mockStatsDClient struct {
	close     func()
	count     func(bucket string, n interface{})
	flush     func()
	gauge     func(bucket string, value interface{})
	histogram func(bucket string, value interface{})
	increment func(bucket string)
	timing    func(bucket string, value interface{})
	unique    func(bucket string, value string)
}

func (c mockStatsDClient) Close() {
	if c.close != nil {
		c.close()
		return
	}
	panic("implement me")
}

func (c mockStatsDClient) Count(bucket string, n interface{}) {
	if c.count != nil {
		c.count(bucket, n)
		return
	}
	panic("implement me")
}

func (c mockStatsDClient) Flush() {
	if c.flush != nil {
		c.flush()
		return
	}
	panic("implement me")
}

func (c mockStatsDClient) Gauge(bucket string, value interface{}) {
	if c.gauge != nil {
		c.gauge(bucket, value)
		return
	}
	panic("implement me")
}

func (c mockStatsDClient) Histogram(bucket string, value interface{}) {
	if c.histogram != nil {
		c.histogram(bucket, value)
		return
	}
	panic("implement me")
}

func (c mockStatsDClient) Increment(bucket string) {
	if c.increment != nil {
		c.increment(bucket)
		return
	}
	panic("implement me")
}

func (c mockStatsDClient) Timing(bucket string, value interface{}) {
	if c.timing != nil {
		c.timing(bucket, value)
		return
	}
	panic("implement me")
}

func (c mockStatsDClient) Unique(bucket string, value string) {
	if c.unique != nil {
		c.unique(bucket, value)
		return
	}
	panic("implement me")
}

func TestStatsDClientStub(t *testing.T) {
	s := NewStatsDService(
		nil,
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	s.Close()
	s.Flush()
	s.Bucket(nil).Tag(nil)
	s.Bucket(nil).Count(1)
	s.Bucket(nil).Increment()
	s.Bucket(nil).Gauge(1)
	s.Bucket(nil).Histogram(1)
	s.Bucket(nil).Unique(1)
	s.Bucket(nil).Timing(1)
}

func TestStatsDService_emptyBucket(t *testing.T) {
	s := statsDService{
		client: mockStatsDClient{},
		keyFunc: func(info BucketInfo) (name string, ok bool) {
			return "", true
		},
	}
	s.Bucket(nil).Tag(nil)
	s.Bucket(nil).Count(1)
	s.Bucket(nil).Increment()
	s.Bucket(nil).Gauge(1)
	s.Bucket(nil).Histogram(1)
	s.Bucket(nil).Unique(1)
	s.Bucket(nil).Timing(1)
}

func TestStatsDBucket_bucketKey(t *testing.T) {
	testCases := []struct {
		B statsDBucket
		K string
	}{
		{
			B: statsDBucket{},
			K: "",
		},
		{
			B: statsDBucket{
				service: statsDService{
					client: statsDClientStub{},
				},
			},
			K: "",
		},
		{
			B: statsDBucket{
				service: statsDService{
					client: statsDClientStub{},
					keyFunc: func(info BucketInfo) (name string, ok bool) {
						return "bucket_key", true
					},
				},
			},
			K: "",
		},
		{
			B: statsDBucket{
				service: statsDService{
					client: statsDClientStub{},
					keyFunc: func(info BucketInfo) (name string, ok bool) {
						if info.Tags["a"][0] != "b" {
							t.Fatal("bad tags")
						}
						return "bucket_key", true
					},
				},
				bucket: &BucketInfo{
					Tags: map[string][]string{
						"a": {"b"},
					},
				},
			},
			K: "bucket_key",
		},
		{
			B: statsDBucket{
				service: statsDService{
					client: statsDClientStub{},
					keyFunc: func(info BucketInfo) (name string, ok bool) {
						if info.Tags["a"][0] != "b" {
							t.Fatal("bad tags")
						}
						return "bucket_key", false
					},
				},
				bucket: &BucketInfo{
					Tags: map[string][]string{
						"a": {"b"},
					},
				},
			},
			K: "",
		},
	}
	for i, testCase := range testCases {
		name := fmt.Sprintf("TestStatsDBucket_bucketKey_#%d", i+1)

		k := testCase.B.bucketKey()

		if k != testCase.K {
			t.Error(name, "k", "expected =", testCase.K, "actual =", k)
		}
	}
}

func TestStatsDService_Close(t *testing.T) {
	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			close: func() {
				calls++
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	if err := s.Close(); err != nil {
		t.Error("bad err", err)
	}
	if calls != 1 {
		t.Error("bad calls", calls)
	}
}

func TestStatsDService_Flush(t *testing.T) {
	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			flush: func() {
				calls++
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	if err := s.Flush(); err != nil {
		t.Error("bad err", err)
	}
	if calls != 1 {
		t.Error("bad calls", calls)
	}
}

func TestStatsDBucket_Count(t *testing.T) {
	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			count: func(bucket string, n interface{}) {
				calls++
				if bucket != "bucket_1,tag_2=value" {
					t.Error("unexpected bucket", bucket)
				}
				if n != 15 {
					t.Error("unexpected n", n)
				}
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	s.Bucket("bucket_1").
		Tag("tag_1").
		Tag("   tag!2", "value").
		Count(15)
	if calls != 1 {
		t.Error("bad calls", calls)
	}
}

func TestStatsDBucket_Increment(t *testing.T) {
	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			increment: func(bucket string) {
				calls++
				if bucket != "bucket_1,tag_2=value" {
					t.Error("unexpected bucket", bucket)
				}
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	s.Bucket("bucket_1").
		Tag("tag_1").
		Tag("   tag!2", "value").
		Increment()
	if calls != 1 {
		t.Error("bad calls", calls)
	}
}

func TestStatsDBucket_Gauge(t *testing.T) {
	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			gauge: func(bucket string, n interface{}) {
				calls++
				if bucket != "bucket_1,tag_2=value" {
					t.Error("unexpected bucket", bucket)
				}
				if n != 15 {
					t.Error("unexpected n", n)
				}
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	s.Bucket("bucket_1").
		Tag("tag_1").
		Tag("   tag!2", "value").
		Gauge(15)
	if calls != 1 {
		t.Error("bad calls", calls)
	}
}

func TestStatsDBucket_Histogram(t *testing.T) {
	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			histogram: func(bucket string, n interface{}) {
				calls++
				if bucket != "bucket_1,tag_2=value" {
					t.Error("unexpected bucket", bucket)
				}
				if n != 15 {
					t.Error("unexpected n", n)
				}
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	s.Bucket("bucket_1").
		Tag("tag_1").
		Tag("   tag!2", "value").
		Histogram(15)
	if calls != 1 {
		t.Error("bad calls", calls)
	}
}

func TestStatsDBucket_Unique(t *testing.T) {
	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			unique: func(bucket string, v string) {
				calls++
				if bucket != "bucket_1,tag_2=value" {
					t.Error("unexpected bucket", bucket)
				}
				if v != `"15"` {
					t.Error("unexpected v", v)
				}
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}
	s.Bucket("bucket_1").
		Tag("tag_1").
		Tag("   tag!2", "value").
		Unique(15)
	if calls != 1 {
		t.Error("bad calls", calls)
	}
}

func TestStatsDBucket_Timing(t *testing.T) {
	var (
		now      = time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)
		_timeNow = timeNow
	)
	timeNow = func() time.Time {
		return now
	}
	defer func() {
		timeNow = _timeNow
	}()

	var calls int
	s := NewStatsDService(
		mockStatsDClient{
			timing: func(bucket string, n interface{}) {
				calls++
				if bucket != "bucket_1,tag_2=value" {
					t.Error("unexpected bucket", bucket)
				}
				if n != int(time.Second*5/time.Millisecond) {
					t.Error("unexpected n", n)
				}
			},
		},
		nil,
	)
	if s == nil {
		t.Fatal("nil service")
	}

	func() {
		defer s.Bucket("bucket_1").
			Tag("tag_1").
			Tag("   tag!2", "value").
			Timing(now.Add(time.Second * -5))

		// do some work...
	}()

	if calls != 1 {
		t.Error("bad calls", calls)
	}
}
