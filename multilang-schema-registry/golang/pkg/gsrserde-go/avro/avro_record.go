// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package avro

import "fmt"

// AvroRecord represents an AVRO record with embedded schema string and Go struct data
// This is designed to work directly with goavro for serialization/deserialization
type AvroRecord struct {
	Schema string      // AVRO schema JSON string
	Data   interface{} // Go struct to be serialized/deserialized
}

// NewAvroRecord creates a new AvroRecord with the given schema and data
func NewAvroRecord(schema string, data interface{}) *AvroRecord {
	return &AvroRecord{
		Schema: schema,
		Data:   data,
	}
}

// GetSchema returns the schema string for compatibility
func (r *AvroRecord) GetSchema() string {
	return r.Schema
}

// GetData returns the data for compatibility
func (r *AvroRecord) GetData() interface{} {
	return r.Data
}

// String returns a string representation of the record
func (r *AvroRecord) String() string {
	return fmt.Sprintf("AvroRecord{schema: %s, data: %v}", r.Schema, r.Data)
}
