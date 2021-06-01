module github.com/instana/go-sensor/instrumentation/instagin

go 1.16

require (
	github.com/gin-gonic/gin v1.7.2
	github.com/instana/go-sensor v1.29.0
	github.com/instana/testify v1.6.2-0.20200721153833-94b1851f4d65
	github.com/opentracing/opentracing-go v1.2.0
)

// todo: remove before release
replace github.com/instana/go-sensor => ../../
