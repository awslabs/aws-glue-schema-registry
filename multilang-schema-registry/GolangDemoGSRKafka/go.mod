module github.com/awslabs/aws-glue-schema-registry/native-schema-registry/GolangDemoGSRKafka

go 1.20

require (
	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.48
	google.golang.org/protobuf v1.36.5
)

require (
	github.com/hamba/avro/v2 v2.25.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/xeipuuv/gojsonpointer v0.0.0-20180127040702-4e3ac2762d5f // indirect
	github.com/xeipuuv/gojsonreference v0.0.0-20180127040603-bd5ef7bd5415 // indirect
	github.com/xeipuuv/gojsonschema v1.2.0 // indirect
)

replace github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang => ../golang
