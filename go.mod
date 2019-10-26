module github.com/tsthght/PITR

go 1.12

require (
	github.com/WangXiangUSTC/tidb-lite v0.0.0-20190718135959-4a72c54defd9
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8 // indirect
	github.com/juju/errors v0.0.0-20190930114154-d42613fe1ab9 // indirect
	github.com/pingcap/check v0.0.0-20190102082844-67f458068fc8
	github.com/pingcap/errors v0.11.4
	github.com/pingcap/log v0.0.0-20190307075452-bd41d9273596
	github.com/pingcap/parser v0.0.0-20190910041007-2a177b291004
	github.com/pingcap/tidb v0.0.0-20190917133016-45d7da02f66e
	github.com/pingcap/tidb-binlog v0.0.0-20191010021753-8e49c63b7528
	github.com/pingcap/tipb v0.0.0-20190428032612-535e1abaa330
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/uber/jaeger-client-go v2.19.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.2.0+incompatible // indirect
	go.uber.org/zap v1.10.0
	gotest.tools v2.2.0+incompatible
)

replace github.com/pingcap/tidb v0.0.0-20190917133016-45d7da02f66e => github.com/WangXiangUSTC/tidb v1.0.1-0.20191026052544-7423f064b9f0
