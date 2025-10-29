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

package common

// AvroRecordType represents the type of Avro record.
type AvroRecordType int

const (
	// AvroRecordTypeUnknown represents an unknown Avro record type.
	AvroRecordTypeUnknown AvroRecordType = iota
	// AvroRecordTypeSpecific represents a specific Avro record type.
	AvroRecordTypeSpecific
	// AvroRecordTypeGeneric represents a generic Avro record type.
	AvroRecordTypeGeneric
)

// String returns the string representation of the AvroRecordType.
func (a AvroRecordType) String() string {
	switch a {
	case AvroRecordTypeSpecific:
		return "SpecificRecord"
	case AvroRecordTypeGeneric:
		return "GenericRecord"
	default:
		return "Unknown"
	}
}
