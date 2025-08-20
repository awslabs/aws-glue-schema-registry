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

import (
	"reflect"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// Configuration holds the Glue Schema Registry configuration.
type Configuration struct {
	DataFormat                DataFormat
	AvroRecordType            AvroRecordType
	ProtobufMessageDescriptor protoreflect.MessageDescriptor
	JsonObjectType            reflect.Type
	GsrConfigPath			  string
	AdditionalProperties      map[string]interface{}
}

// NewConfiguration creates a new Configuration from a map of configuration values.
func NewConfiguration(configs map[string]interface{}) *Configuration {
	c := &Configuration{
		DataFormat:           DataFormatUnknown,
		AvroRecordType:       AvroRecordTypeUnknown,
		AdditionalProperties: make(map[string]interface{}),
	}
	c.buildConfigs(configs)
	return c
}


func (c *Configuration) buildConfigs(configs map[string]interface{}) {
	c.validateAndSetAvroRecordType(configs)
	c.validateAndSetProtobufMessageDescriptor(configs)
	c.validateAndSetDataFormat(configs)
	c.validateAndSetJSONObjectType(configs)
	c.validateAndSetGsrConfigPath(configs)
}
func (c *Configuration) validateAndSetGsrConfigPath(configs map[string]interface{}){
	if val, ok := configs[GSRConfigPathKey]; ok {
		if gsrConfigPath, ok := val.(string); ok {
			c.GsrConfigPath = gsrConfigPath
		}
	} 
}

func (c *Configuration) validateAndSetDataFormat(configs map[string]interface{}) {
	if val, ok := configs[DataFormatTypeKey]; ok {
		if dataFormat, ok := val.(DataFormat); ok {
			c.DataFormat = dataFormat
		}
	}
}

func (c *Configuration) validateAndSetAvroRecordType(configs map[string]interface{}) {
	if val, ok := configs[AvroRecordTypeKey]; ok {
		if avroRecordType, ok := val.(AvroRecordType); ok {
			c.AvroRecordType = avroRecordType
		}
	}
}

func (c *Configuration) validateAndSetProtobufMessageDescriptor(configs map[string]interface{}) {
	if val, ok := configs[ProtobufMessageDescriptorKey]; ok {
		if messageDescriptor, ok := val.(protoreflect.MessageDescriptor); ok {
			c.ProtobufMessageDescriptor = messageDescriptor
		}
	}
}

func (c *Configuration) validateAndSetJSONObjectType(configs map[string]interface{}) {
	if val, ok := configs[JSONObjectTypeKey]; ok {
		if jsonType, ok := val.(reflect.Type); ok {
			c.JsonObjectType = jsonType
		}
	}
}
