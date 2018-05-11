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
	"math"
	"time"
	"github.com/go-test/deep"
	"reflect"
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

func TestStringToNumber(t *testing.T) {
	testCases := []struct {
		Value       string
		Integer     int64
		Fractional  float64
		Exponential int
		Ok          bool
	}{
		{
			Value:       ``,
			Integer:     0,
			Fractional:  0,
			Exponential: 0,
			Ok:          false,
		},
		{
			Value:       `0`,
			Integer:     0,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `1`,
			Integer:     1,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `     1234    `,
			Integer:     1234,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `   1,23 ,,,, 4   `,
			Integer:     1234,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `-1234`,
			Integer:     -1234,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `+1234`,
			Integer:     1234,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `,  -  1  23 ,,,, 4   ,`,
			Integer:     -1234,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `  +   ,1   2  34,`,
			Integer:     1234,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `1234.5678`,
			Integer:     1234,
			Fractional:  0.5678,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `-1234.5678`,
			Integer:     -1234,
			Fractional:  -0.5678,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `+1234.5678`,
			Integer:     1234,
			Fractional:  0.5678,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `1234.-5678`,
			Integer:     0,
			Fractional:  0,
			Exponential: 0,
			Ok:          false,
		},
		{
			Value:       `   1234.56,78   `,
			Integer:     1234,
			Fractional:  0.5678,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `1234.0`,
			Integer:     1234,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `0.5678`,
			Integer:     0,
			Fractional:  0.5678,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `-0.5678`,
			Integer:     0,
			Fractional:  -0.5678,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `-0.0`,
			Integer:     0,
			Fractional:  0,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `10.`,
			Integer:     0,
			Fractional:  0,
			Exponential: 0,
			Ok:          false,
		},
		{
			Value:       `10.000000000000000000000001`,
			Integer:     10,
			Fractional:  0.000000000000000000000001,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `-10.000000000000000000000001`,
			Integer:     -10,
			Fractional:  -0.000000000000000000000001,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `+10.000000000000000000000001`,
			Integer:     10,
			Fractional:  0.000000000000000000000001,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `   ,  ,,,,, +, 10  . 0 0 0 00 010 00 000 00000,0 0 00001   ,,`,
			Integer:     10,
			Fractional:  0.0000001000000000000000001,
			Exponential: 0,
			Ok:          true,
		},
		{
			Value:       `10e+17`,
			Integer:     10,
			Fractional:  0,
			Exponential: 17,
			Ok:          true,
		},
		{
			Value:       `10E17`,
			Integer:     10,
			Fractional:  0,
			Exponential: 17,
			Ok:          true,
		},
		{
			Value:       `-213148214123.213   x 10 ^  -12`,
			Integer:     -213148214123,
			Fractional:  -0.213,
			Exponential: -12,
			Ok:          true,
		},
		{
			Value:       `-213148214123.213   * 10 ^  -12`,
			Integer:     -213148214123,
			Fractional:  -0.213,
			Exponential: -12,
			Ok:          true,
		},
		{
			Value:       `,  - , 10.  0 1,E,1 ,7 ,, `,
			Integer:     -10,
			Fractional:  -0.01,
			Exponential: 17,
			Ok:          true,
		},
		{
			Value:       `,  - , 10.  0 1,E,-1 ,7 ,, `,
			Integer:     -10,
			Fractional:  -0.01,
			Exponential: -17,
			Ok:          true,
		},
		{
			Value:       `10e+17.01`,
			Integer:     0,
			Fractional:  0,
			Exponential: 0,
			Ok:          false,
		},
	}

	// add some more ok cases via fmt.Sprintf
	addNum := func(n interface{}, integer int64, fractional float64, exponential int) {
		testCases = append(
			testCases,
			struct {
				Value       string
				Integer     int64
				Fractional  float64
				Exponential int
				Ok          bool
			}{
				Value:       fmt.Sprintf("%v", n),
				Integer:     integer,
				Fractional:  fractional,
				Exponential: exponential,
				Ok:          true,
			},
		)
	}
	addNum(uint32(123), 123, 0, 0)
	addNum(float64(0.00000000000000000000000000000000001), 1, 0, -35)
	if math.Pow10(-35) != 0.00000000000000000000000000000000001 {
		t.Fatal("bad test...")
	}
	addNum(float64(98912315772500247140000120371765768091432.90133312), 9, 0.891231577250024, 40)
	if diff := math.Abs(98912315772500247140000120371765768091432.90133312 - (float64(9.891231577250024) * math.Pow10(40))); diff > 0 {
		t.Fatal("bad diff", diff)
	}
	addNum(float64(-98912315772500247140000120371765768091432.90133312), -9, -0.891231577250024, 40)

	for i, testCase := range testCases {
		name := fmt.Sprintf("TestStringToNumber_#%d", i+1)

		integer, fractional, exponential, ok := StringToNumber(testCase.Value)

		if integer != testCase.Integer {
			t.Error(name, "integer", "expected =", testCase.Integer, "actual =", integer)
		}

		if fractional != testCase.Fractional {
			t.Error(name, "fractional", "expected =", testCase.Fractional, "actual =", fractional)
		}

		if exponential != testCase.Exponential {
			t.Error(name, "exponential", "expected =", testCase.Exponential, "actual =", exponential)
		}

		if ok != testCase.Ok {
			t.Error(name, "ok", "expected =", testCase.Ok, "actual =", ok)
		}
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
			Value: "9223372036854775807",
			Multi: time.Nanosecond,
			Result: 9223372036854775807,
			Ok: true,
		},
		{ // - just fits into an int64
			Value: "-9223372036854775808",
			Multi: time.Nanosecond,
			Result: -9223372036854775808,
			Ok: true,
		},
		{ // + overflow
			Value: "9223372036854775808",
			Multi: time.Nanosecond,
			Result: 0,
			Ok: false,
		},
		{ // - overflow
			Value: "-9223372036854775809",
			Multi: time.Nanosecond,
			Result: 0,
			Ok: false,
		},
		{ // multiplier overflows
			Value: "999999999999",
			Multi: time.Hour,
			Result: 0,
			Ok: false,
		},
		{ // overflows int64 on multi but passes number parse
			Value: "512438192184912 x 10 ^ 3",
			Multi: time.Minute,
			Result: 0,
			Ok: false,
		},
		{ // this one passes through
			Value: "512438192184912.1 x 10 ^ 3",
			Multi: time.Nanosecond,
			Result: 512438192184912100,
			Ok: true,
			Delta: 50,
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
			Result: 0,
			Ok:     false,
			Delta:  0,
		},
		{
			Value:  float64(mostPositive) + 0.0001,
			Multi:  time.Nanosecond,
			Result: 0,
			Ok:     false,
			Delta:  0,
		},
		{ // very large number requiring parsing
			Value: `9223372036844775807.01`,
			Multi: time.Nanosecond,
			Result: 9223372036844775807,
			Ok: true,
			Delta: 1000,
		},
		{ // very small number requiring parsing
			Value: `-9223372036844775808.01`,
			Multi: time.Nanosecond,
			Result: -9223372036844775808,
			Ok: true,
			Delta: 1000,
		},
		{ // + detected out of bounds
			Value: `9223372036844775807.01E100`,
			Multi: time.Nanosecond,
			Result: 0,
			Ok: false,
			Delta: 0,
		},
		{ // - detected out of bounds
			Value: `-9223372036844775808.01E100`,
			Multi: time.Nanosecond,
			Result: 0,
			Ok: false,
			Delta: 0,
		},
		{ // bad multi
			Value: time.Second,
			Multi: 0,
			Result: 0,
			Ok: false,
			Delta: 0,
		},
		{ // bad multi
			Value: time.Second,
			Multi: -1,
			Result: 0,
			Ok: false,
			Delta: 0,
		},
		{ // edge case test...
			Value: "-922337203685477580 E 1",
			Multi: time.Nanosecond,
			Result: -9223372036854775808,
			Ok: true,
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
			Value:  float64(mostNegative) - 1222.1,
			Multi:  time.Nanosecond,
			Result: 0,
			Ok:     false,
			Delta:  0,
		},
		{
			Value:  float64(mostNegative) + 122.1,
			Multi:  time.Nanosecond,
			Result: -9223372036854775808,
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

func TestMostPositive(t *testing.T) {
	var value int64 = mostPositive
	if value <= 0 {
		t.Fatal("unexpected value", value)
	}
	overflow := value + 1
	if overflow >= 0 {
		t.Fatal("unexpected overflow", overflow)
	}
}

func TestMostNegative(t *testing.T) {
	var value int64 = mostNegative
	if value >= 0 {
		t.Fatal("unexpected value", value)
	}
	overflow := value - 1
	if overflow <= 0 {
		t.Fatal("unexpected overflow", overflow)
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
