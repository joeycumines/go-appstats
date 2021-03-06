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
	"errors"
	"fmt"
	"github.com/go-test/deep"
	"math"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestBucketInfo_Tag_nilReceiver(t *testing.T) {
	var b *BucketInfo

	r := b.Tag("key", 1, "2", true)

	if diff := deep.Equal(r, &BucketInfo{
		Bucket: "",
		Tags: map[string][]string{
			"key": {
				"1",
				"2",
				"true",
			},
		},
	}); diff != nil {
		t.Fatal("unexpected result", diff)
	}
}

func TestBucketInfo_Tag_noValues(t *testing.T) {
	b := new(BucketInfo)

	r := b.Tag("key")

	if r == b {
		t.Fatal("bad copy")
	}

	if diff := deep.Equal(r, &BucketInfo{
		Bucket: "",
		Tags: map[string][]string{
			"key": {},
		},
	}); diff != nil {
		t.Fatal("unexpected result", diff)
	}

	if 0 != len(b.Tags) {
		t.Fatal("unexpected source", b)
	}
}

func TestBucketInfo_Tag_existingValues(t *testing.T) {
	b := &BucketInfo{
		Bucket: "ONE",
		Tags: map[string][]string{
			"key": {
				"a",
				"b",
				"c",
			},
			"A": {"123", "", ""},
			"B": {"123", "", "2"},
			"Z": nil,
		},
	}

	r := b.Tag("key", "x", "y", "z")

	if diff := deep.Equal(r, &BucketInfo{
		Bucket: "ONE",
		Tags: map[string][]string{
			"key": {
				"a",
				"b",
				"c",
				"x",
				"y",
				"z",
			},
			"A": {"123", "", ""},
			"B": {"123", "", "2"},
			"Z": nil,
		},
	}); diff != nil {
		t.Fatal("unexpected result", diff)
	}

	if diff := deep.Equal(b, &BucketInfo{
		Bucket: "ONE",
		Tags: map[string][]string{
			"key": {
				"a",
				"b",
				"c",
			},
			"A": {"123", "", ""},
			"B": {"123", "", "2"},
			"Z": nil,
		},
	}); diff != nil {
		t.Fatal("unexpected source", diff)
	}
}

func TestStringToRat(t *testing.T) {
	if r, ok := stringToRat(`    ,  999,9 12,,,,37  , 2, 1    e -,8917,236 ,,,,       `); !ok {
		t.Error(r, ok)
	} else if e, _ := new(big.Rat).SetString(`9999123721e-8917236`); e.Cmp(r) != 0 {
		t.Error(e, r)
	}
	if r, ok := stringToRat(``); ok || r != nil {
		t.Error(r, ok)
	}
}

func TestTimingToDuration(t *testing.T) {
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

	testCases := []struct {
		Value  interface{}
		Multi  time.Duration
		Result time.Duration
		Delta  time.Duration
		Ok     bool
	}{
		{
			Value:  time.Minute * 30,
			Multi:  time.Hour,
			Result: time.Minute * 30,
			Ok:     true,
		},
		{
			Value:  now.Add(time.Minute * -5),
			Multi:  time.Hour,
			Result: time.Minute * 5,
			Ok:     true,
		},
		{
			Value:  now,
			Multi:  time.Hour,
			Result: 0,
			Ok:     true,
		},
		{
			Value:  time.Duration(0),
			Multi:  time.Hour,
			Result: 0,
			Ok:     true,
		},
		{
			Value:  time.Minute * -60,
			Multi:  time.Hour,
			Result: time.Minute * -60,
			Ok:     true,
		},
		{
			Value:  now.Add(time.Minute * 5),
			Multi:  time.Hour,
			Result: time.Minute * -5,
			Ok:     true,
		},
		{
			Value:  int(915134),
			Multi:  time.Minute,
			Result: time.Minute * 915134,
			Ok:     true,
		},
		{
			Value:  int32(915134),
			Multi:  time.Minute,
			Result: time.Minute * 915134,
			Ok:     true,
		},
		{
			Value:  uint(915134),
			Multi:  time.Minute,
			Result: time.Minute * 915134,
			Ok:     true,
		},
		{
			Value:  uint32(915134),
			Multi:  time.Minute,
			Result: time.Minute * 915134,
			Ok:     true,
		},
		{
			Value:  byte(200),
			Multi:  time.Minute,
			Result: time.Minute * 200,
			Ok:     true,
		},
		{
			Value:  "no",
			Multi:  time.Minute,
			Result: 0,
			Ok:     false,
		},
		{
			Value:  float64(915134),
			Multi:  time.Minute,
			Result: time.Minute * 915134,
			Ok:     true,
		},
		{ // + just fits into an int64
			Value:  "9223372036854775807",
			Multi:  time.Nanosecond,
			Result: 9223372036854775807,
			Ok:     true,
		},
		{ // - just fits into an int64
			Value:  "-9223372036854775808",
			Multi:  time.Nanosecond,
			Result: -9223372036854775808,
			Ok:     true,
		},
		{ // + overflow
			Value:  "9223372036854775808",
			Multi:  time.Nanosecond,
			Result: math.MaxInt64,
			Ok:     true,
		},
		{ // - overflow
			Value:  "-9223372036854775809",
			Multi:  time.Nanosecond,
			Result: math.MinInt64,
			Ok:     true,
		},
		{ // multiplier overflows
			Value:  "999999999999",
			Multi:  time.Hour,
			Result: math.MaxInt64,
			Ok:     true,
		},
		{ // overflows int64 on multi but passes number parse
			Value:  "512438192184912 E 3",
			Multi:  time.Minute,
			Result: math.MaxInt64,
			Ok:     true,
		},
		{ // this one passes through
			Value:  "512438192184912.1 e 3",
			Multi:  time.Nanosecond,
			Result: 512438192184912100,
			Ok:     true,
			Delta:  50,
		},
		{
			Value:  914.12341259,
			Multi:  time.Millisecond,
			Result: 914123412,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  float64(9223372036854775807),
			Multi:  time.Nanosecond,
			Result: math.MaxInt64,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  math.MaxInt64 + 0.0001,
			Multi:  time.Nanosecond,
			Result: math.MaxInt64,
			Ok:     true,
			Delta:  0,
		},
		{ // very large number requiring parsing
			Value:  `9223372036844775807.01`,
			Multi:  time.Nanosecond,
			Result: 9223372036844775807,
			Ok:     true,
			Delta:  1000,
		},
		{ // very small number requiring parsing
			Value:  `-9223372036844775808.01`,
			Multi:  time.Nanosecond,
			Result: -9223372036844775808,
			Ok:     true,
			Delta:  1000,
		},
		{ // + detected out of bounds
			Value:  `9223372036844775807.01E100`,
			Multi:  time.Nanosecond,
			Result: math.MaxInt64,
			Ok:     true,
			Delta:  0,
		},
		{ // - detected out of bounds
			Value:  `-9223372036844775808.01E100`,
			Multi:  time.Nanosecond,
			Result: math.MinInt64,
			Ok:     true,
			Delta:  0,
		},
		{ // bad multi
			Value:  time.Second,
			Multi:  0,
			Result: 0,
			Ok:     false,
			Delta:  0,
		},
		{ // bad multi
			Value:  time.Second,
			Multi:  -1,
			Result: 0,
			Ok:     false,
			Delta:  0,
		},
		{ // edge case test...
			Value:  "-922337203685477580 E 1",
			Multi:  time.Nanosecond,
			Result: -9223372036854775800,
			Ok:     true,
		},
		{
			Value:  "-922337203685477580 E -4",
			Multi:  time.Nanosecond,
			Result: -92233720368547,
			Ok:     true,
		},
		{
			Value:  213214.321,
			Multi:  time.Second,
			Result: 2.13214321e+14,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  -213214.321,
			Multi:  time.Second,
			Result: -2.13214321e+14,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  float64(math.MinInt64) - 1222.1,
			Multi:  time.Nanosecond,
			Result: math.MinInt64,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  float64(math.MinInt64) + 122.1,
			Multi:  time.Nanosecond,
			Result: math.MinInt64,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  `0.0000 E 3`,
			Multi:  time.Nanosecond,
			Result: 0,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  `0.000`,
			Multi:  time.Nanosecond,
			Result: 0,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  `0.000 E 99`,
			Multi:  time.Hour,
			Result: 0,
			Ok:     true,
			Delta:  0,
		},
		{
			Value:  "924310ms",
			Multi:  time.Hour,
			Result: time.Millisecond * 924310,
			Ok:     true,
		},
	}

	for i, testCase := range testCases {
		name := fmt.Sprintf("TestTimingToDuration_#%d", i+1)

		result, ok := TimingToDuration(testCase.Value, testCase.Multi)

		if testCase.Delta == 0 {
			if result != testCase.Result {
				t.Error(name, "result", "expected =", int64(testCase.Result), "actual =", int64(result))
			}
		} else {
			diff := result - testCase.Result
			if diff < 0 {
				diff *= -1
			}
			if diff > testCase.Delta {
				t.Error(name, "result", "expected =", int64(testCase.Result), "actual =", int64(result), "delta =", testCase.Delta)
			}
		}

		if ok != testCase.Ok {
			t.Error(name, "ok", "expected =", testCase.Ok, "actual =", ok)
		}
	}
}

func TestSanitiseKey(t *testing.T) {
	testCases := []struct {
		Input  string
		Output string
	}{
		{
			Input:  "",
			Output: "",
		},
		{
			Input:  "one_two_three",
			Output: "one_two_three",
		},
		{
			Input:  "ONE_TWO_THREE",
			Output: "one_two_three",
		},
		{
			Input:  "abcdefghijklmnopqrstuvwxyzAZ0123535943795_-:./\\",
			Output: "abcdefghijklmnopqrstuvwxyzaz0123535943795_-:./\\",
		},
		{
			Input:  " !123#$sda sad:2u/.U*@*#)$)-*SDAIJ   sDSK-o262  ",
			Output: "sda_sad:2u/.u_______-_sdaij___sdsk-o262__",
		},
		{
			Input:  "value::::::::::",
			Output: "value",
		},
		{
			Input:  "a::::::::::",
			Output: "a",
		},
		{
			Input:  "::::::::::",
			Output: "",
		},
		{
			Input:  "::::::a:z:::",
			Output: "a:z",
		},
		{
			Input:  "a1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
			Output: "a1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
		},
		{
			Input:  "a1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789willbecutoff",
			Output: "a1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789",
		},
		{
			Input:  "Ñ",
			Output: "ñ",
		},
		{
			Input:  "a1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456ąñ",
			Output: "a1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456ą",
		},
	}

	for i, testCase := range testCases {
		name := fmt.Sprintf("TestSanitiseKey_#%d", i+1)

		output := SanitiseKey(testCase.Input)

		if output != testCase.Output {
			t.Error(name, "output", "expected =", testCase.Output, "actual =", output)
		}
	}
}

func TestDefaultBucketKeyFunc(t *testing.T) {
	testCases := []struct {
		Info BucketInfo
		Key  string
		Ok   bool
	}{
		{
			Info: BucketInfo{},
			Key:  "",
			Ok:   false,
		},
		{
			Info: BucketInfo{
				Bucket: "bucket",
			},
			Key: "bucket",
			Ok:  true,
		},
		{
			Info: BucketInfo{
				Bucket: "bucket",
				Tags: map[string][]string{
				},
			},
			Key: "bucket",
			Ok:  true,
		},
		{
			Info: BucketInfo{
				Bucket: "bucket",
				Tags: map[string][]string{
					"": {``, ``, ``},
				},
			},
			Key: "bucket",
			Ok:  true,
		},
		{
			Info: BucketInfo{
				Bucket: "bucket",
				Tags: map[string][]string{
					"tag": {``},
				},
			},
			Key: "bucket",
			Ok:  true,
		},
		{
			Info: BucketInfo{
				Bucket: "bucket",
				Tags: map[string][]string{
					"tag": nil,
				},
			},
			Key: "bucket",
			Ok:  true,
		},
		{
			Info: BucketInfo{
				Bucket: "!BUCKET_one.TWO-tree",
				Tags: map[string][]string{
					"  ONE!two": {"12vaLue_1!"},
					"tag_2":     nil,
					"tag_3":     {"one", "   TWO"},
					"A1231":     {"", "  "},
					"ñ":         {"", "adads:"},
					"Z":         {"a", ""},
					"":          {"one", "   TWO"},
					"12313":     nil,
				},
			},
			Key: "bucket_one.two-tree,one_two=value_1_,tag_3=two,ñ=adads",
			Ok:  true,
		},
	}
	for i, testCase := range testCases {
		name := fmt.Sprintf("TestDefaultBucketKeyFunc_#%d", i+1)

		key, ok := DefaultBucketKeyFunc(testCase.Info)

		if key != testCase.Key {
			t.Error(name, "key", "expected =", testCase.Key, "actual =", key)
		}

		if ok != testCase.Ok {
			t.Error(name, "ok", "expected =", testCase.Ok, "actual =", ok)
		}
	}
}

func TestNewStatsDService_defaults(t *testing.T) {
	service := NewStatsDService(nil, nil)
	if service == nil {
		t.Fatal("expected a non-nil service")
	}
	s, ok := service.(statsDService)
	if !ok {
		t.Fatal("expected a statsDService")
	}
	if _, ok := s.client.(statsDClientStub); !ok {
		t.Fatal("expected a statsDClientStub")
	}
	if s.keyFunc == nil || reflect.ValueOf(s.keyFunc).Pointer() != reflect.ValueOf(DefaultBucketKeyFunc).Pointer() {
		t.Fatal("unexpected key func")
	}
}

func TestNewStatsDService_keyFunc(t *testing.T) {
	service := NewStatsDService(nil, func(info BucketInfo) (name string, ok bool) {
		return "some_value", true
	})
	if service == nil {
		t.Fatal("expected a non-nil service")
	}
	s, ok := service.(statsDService)
	if !ok {
		t.Fatal("expected a statsDService")
	}
	key, ok := s.keyFunc(BucketInfo{})
	if key != "some_value" || !ok {
		t.Fatal("unexpected key", key, ok)
	}
}

func TestQuoteString(t *testing.T) {
	testCases := []struct {
		Input  string
		Output string
	}{
		{
			Input:  ``,
			Output: `""`,
		},
		{
			Input:  `  DS asdas !$@s `,
			Output: `"  DS asdas !$@s "`,
		},
		{
			Input:  ` "as\"d  DS as/d\a\\s \\\!$@s `,
			Output: `" \"as\\\"d  DS as/d\\a\\\\s \\\\\\!$@s "`,
		},
	}

	for i, testCase := range testCases {
		name := fmt.Sprintf("TestQuoteString_%d", i+1)

		output := QuoteString(testCase.Input)

		if output != testCase.Output {
			t.Errorf("%s output '%s' != expected '%s'", name, output, testCase.Output)
		}
	}
}

func TestTagger_Apply_nilBucket(t *testing.T) {
	var tagger Tagger = func(bucket Bucket) (Bucket, error) {
		panic("no")
	}
	b, err := tagger.Apply(nil)
	if err == nil || err.Error() != "appstats.Tagger.Apply nil bucket" {
		t.Fatal(b, err)
	}
}

func TestTagger_Apply_nilReceiver(t *testing.T) {
	var tagger Tagger

	in := &mockBucket{}

	out, err := tagger.Apply(in)

	if out != in || err != nil {
		t.Fatal(out, err)
	}
}

func TestTagger_Apply_error(t *testing.T) {
	in := &mockBucket{}

	var tagger Tagger = func(bucket Bucket) (Bucket, error) {
		if bucket != in {
			t.Error(bucket)
		}

		return new(mockBucket), errors.New("some_error")
	}

	out, err := tagger.Apply(in)

	if out != nil || err == nil || err.Error() != "appstats.Tagger.Apply tagger error: some_error" {
		t.Error(out, err)
	}
}

func debugTaggerReturnNil(bucket Bucket) (Bucket, error) {
	if bucket == nil {
		panic("should not be nil but this is for unique")
	}
	return nil, nil
}

func TestTagger_Apply_nilReturn(t *testing.T) {
	out, err := Tagger(debugTaggerReturnNil).Apply(new(mockBucket))

	if out != nil || err == nil || err.Error() != "appstats.Tagger.Apply nil bucket for tagger: github.com/joeycumines/go-appstats.debugTaggerReturnNil" {
		t.Error(out, err)
	}
}

func TestTagger_Apply_success(t *testing.T) {
	in := &mockBucket{}

	expected := &mockBucket{}

	var tagger Tagger = func(bucket Bucket) (Bucket, error) {
		if bucket != in {
			t.Error(bucket)
		}

		return expected, nil
	}

	out, err := tagger.Apply(in)

	if out != expected || err != nil {
		t.Error(out, err)
	}
}

func TestApplyTaggers_nilBucket(t *testing.T) {
	b, err := ApplyTaggers(nil)
	if b != nil || err == nil || err.Error() != "appstats.ApplyTaggers nil bucket" {
		t.Error(b, err)
	}
}

func TestApplyTaggers_error(t *testing.T) {
	var called bool

	one := new(mockBucket)
	two := new(mockBucket)

	out, err := ApplyTaggers(
		one,
		func(bucket Bucket) (Bucket, error) {
			if called {
				t.Error("called more than once")
			}
			called = true
			if bucket != one {
				t.Error(bucket)
			}
			return two, nil
		},
		func(bucket Bucket) (Bucket, error) {
			if bucket != two {
				t.Error(bucket)
			}
			return nil, errors.New("some_error")
		},
		func(bucket Bucket) (Bucket, error) {
			panic("no")
		},
	)

	if out != nil || err == nil || err.Error() != "appstats.ApplyTaggers tagger error at index 1: appstats.Tagger.Apply tagger error: some_error" {
		t.Error(out, err)
	}

	if !called {
		t.Error(called)
	}
}

func TestApplyTaggers_success(t *testing.T) {
	var count int

	one := new(mockBucket)
	two := new(mockBucket)
	three := new(mockBucket)
	four := new(mockBucket)

	out, err := ApplyTaggers(
		one,
		func(bucket Bucket) (Bucket, error) {
			if count != 0 {
				t.Error(count)
			}
			count++
			if bucket != one {
				t.Error(bucket)
			}
			return two, nil
		},
		func(bucket Bucket) (Bucket, error) {
			if count != 1 {
				t.Error(count)
			}
			count++
			if bucket != two {
				t.Error(bucket)
			}
			return three, nil
		},
		func(bucket Bucket) (Bucket, error) {
			if count != 2 {
				t.Error(count)
			}
			count++
			if bucket != three {
				t.Error(bucket)
			}
			return four, nil
		},
	)

	if out != four || err != nil {
		t.Error(out, err)
	}

	if count != 3 {
		t.Error(count)
	}
}

func TestTagMap_all(t *testing.T) {
	result := make(map[interface{}][]interface{})

	var bucket *mockBucket

	bucket = &mockBucket{
		tag: func(key interface{}, values ...interface{}) Bucket {
			if _, ok := result[key]; !ok {
				result[key] = nil
			}
			result[key] = append(result[key], values...)
			return bucket
		},
	}

	randoStruct := new(struct{})

	b, err := ApplyTaggers(
		bucket,
		TagMapStringString(
			map[string]string{
				"one": "1",
				"two": "1",
			},
		),
		TagMapStringInterface(
			map[string]interface{}{
				"two":   2,
				"three": "1",
			},
		),
		TagMapInterfaceInterface(
			map[interface{}]interface{}{
				"one": randoStruct,
				4:     true,
			},
		),
	)
	if b != bucket || err != nil {
		t.Fatal(b, err)
	}

	if diff := deep.Equal(
		result,
		map[interface{}][]interface{}{
			"one": {
				"1",
				randoStruct,
			},
			"two": {
				"1",
				2,
			},
			"three": {
				"1",
			},
			4: {
				true,
			},
		},
	); diff != nil {
		t.Fatal(diff)
	}
}

type mockBucket struct {
	tag func(key interface{}, values ...interface{}) Bucket
}

func (m *mockBucket) Tag(key interface{}, values ...interface{}) Bucket {
	if m != nil && m.tag != nil {
		return m.tag(key, values...)
	}
	panic("implement me")
}

func (m *mockBucket) Count(n interface{}) {
	panic("implement me")
}

func (m *mockBucket) Increment() {
	panic("implement me")
}

func (m *mockBucket) Gauge(value interface{}) {
	panic("implement me")
}

func (m *mockBucket) Histogram(value interface{}) {
	panic("implement me")
}

func (m *mockBucket) Unique(value interface{}) {
	panic("implement me")
}

func (m *mockBucket) Timing(value interface{}) {
	panic("implement me")
}

func TestNewBucketKeyFunc_nil(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected a panic")
		}
	}()
	NewBucketKeyFunc(nil)
	t.Error("should not reach here")
}

func benchmarkTimingToDuration(b *testing.B, fn func(value interface{}, multi time.Duration) (d time.Duration, ok bool)) {
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		f := new(big.Float).
			SetPrec(124).
			SetInt64(rand.Int63())
		f.Mul(f, new(big.Float).SetInt64(rand.Int63()))
		f.Mul(f, new(big.Float).Mul(f, new(big.Float).SetFloat64(rand.Float64())))
		if rand.Int63()%2 == 0 {
			f.Neg(f)
		}
		b.StartTimer()
		d, ok := fn(fmt.Sprintf(",,,,,   %v    ,,,", f), time.Nanosecond)
		b.StopTimer()
		if !ok {
			b.Error(n, f, d, ok)
		} else if i, _ := f.Int64(); i != int64(d) {
			b.Error(n, f, d, time.Duration(i))
		}
		b.StartTimer()
	}
}

func BenchmarkTimingToDuration(b *testing.B) {
	benchmarkTimingToDuration(b, TimingToDuration)
}
