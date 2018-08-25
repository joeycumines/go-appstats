# go-appstats

> Test Coverage: 97.8%

What started as just a very simple interface wrapper for
[github.com/alexcesaro/statsd](https://github.com/alexcesaro/statsd), to provide proper support for InfluxDB tags, and
to make it easier to swap out for another stats implementation / extend as necessary, now also
includes some very useful parsing and sanitisation tools, and is designed to be extensible and unobtrusive, with the
`appstats.Tagger` definition providing an option pattern very useful for implementing custom tag values in a way that
doesn't require frequent breaking changes.

[Have a look at the godoc for more information.](https://godoc.org/github.com/joeycumines/go-appstats)

- `appstats.Service` and `appstats.Bucket` are the core interfaces, the latter being the interesting one
- `appstats.Tagger` is entirely optional, but it builds on the behavior defined by `appstats.Bucket`
- `appstats.BucketInfo` is used internally but also exposed for extensions
- `appstats.StatsDClient` is an interface matching the API provided by `github.com/alexcesaro/statsd`, and used
  by the `appstats.Bucket` and `appstats.Service` implementations for that library
- InfluxDB support (the tag building part) is provided by `appstats.DefaultBucketKeyFunc` which uses
  `appstats.NewBucketKeyFunc`, and _afaik/imo_ matches their best practices as well as possible
- Datadog's "tagging best practices" are used as part of the InfluxDB tagging support, and implemented by
  `appstats.SanitiseKey`
- `appstats.StringToNumber` and `appstats.TimingToDuration` are provided to deal with the surprisingly very tricky
  problem of supporting time series data in generic formats
