### Connect to Protobuf Conversion

#### Data types

|Connect Schema Type  |  Protobuf Type                                                    |
|:----------------------|:------------------------------------------------------------------|
|INT8, INT16        |int32                                                              |
|INT32              |int32 (default), SINT32, SFIXED32, INT32                           |
|INT64              |int64 (default), SINT64, UINT64, SFIXED64, FIXED64, FIXED32, UINT32|
|FLOAT32            |float                                                              |
|FLOAT64            |double                                                             |
|BOOLEAN            |bool                                                               |
|STRING             |string (default), enum                                             |
|BYTES              |bytes (bytestring)                                                 |
|ARRAY              |repeated                                                           |
|MAP                |map                                                                |
|STRUCT             |message                                                            |
|TIME               |google.type.TimeOfDay                                              |
|TIMESTAMP          |google.protobuf.Timestamp                                          |
|DATE               |google.type.Date                                                   |
|DECIMAL            |[Custom Decimal.proto implementation](https://github.com/protocolbuffers/protobuf/issues/4406)                               |


#### Conversion metadata

Following optional properties can be used to change Protobuf schema inference from Connect schemas.

| Connect metadata | Usage |
|:--------------------------|:------------------|
|protobuf.tag | Specify the Protobuf tag number to use for a field. Fields are generated sequentially otherwise.
|protobuf.type | Specify the Protobuf data type to use for converting a certain field. Default mappings are used otherwise. |