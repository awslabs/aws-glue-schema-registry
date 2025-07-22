module github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests

go 1.20

require (
	github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang v0.0.0
	github.com/segmentio/kafka-go v0.4.42
	github.com/stretchr/testify v1.10.0
	google.golang.org/protobuf v1.28.1
)

replace github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang => ../

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/hamba/avro/v2 v2.25.1 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
