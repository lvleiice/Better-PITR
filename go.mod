module github.com/lvleiice/Better-PITR

go 1.13

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/WangXiangUSTC/tidb-lite v0.0.0-20191217032011-70d76f82e589
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/opentracing/opentracing-go v1.1.0 // indirect
	github.com/pingcap/check v0.0.0-20191107115940-caf2b9e6ccf4
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/log v0.0.0-20191012051959-b742a5d432e9
	github.com/pingcap/parser v0.0.0-20191210055545-753e13bfdbf0
	github.com/pingcap/tidb v1.1.0-beta.0.20191211070559-a94cff903cd1
	github.com/pingcap/tidb-binlog v0.0.0-20191206040428-628c34a97114
	github.com/pingcap/tipb v0.0.0-20191209145133-44f75c9bef33
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/sirupsen/logrus v1.8.3 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/uber/jaeger-client-go v2.19.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	go.uber.org/zap v1.12.0
	gotest.tools v2.2.0+incompatible
)

replace github.com/pingcap/tidb => github.com/AilinKid/tidb v1.1.0-beta.0.20191213100648-826c5faecfb9
