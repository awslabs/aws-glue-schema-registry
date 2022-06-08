/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;

public class TimeSchemaTypeConverter implements SchemaTypeConverter {

    private static final String DATE_PROTO_IMPORT = "google/type/date.proto";
    private static final String TIMESTAMP_PROTO_IMPORT = "google/protobuf/timestamp.proto";
    private static final String TIMEOFDAY_PROTO_IMPORT = "google/type/timeofday.proto";

    @Override
    public DescriptorProtos.FieldDescriptorProto.Builder toProtobufSchema(
            Schema schema, DescriptorProtos.DescriptorProto.Builder descriptorProto,
            DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder) {

        String typename = ".";
        if (Date.SCHEMA.name().equals(schema.name())) {
            typename += com.google.type.Date.getDescriptor().getFullName();
            addImportToProtobufSchema(fileDescriptorProtoBuilder, DATE_PROTO_IMPORT);
        } else if (Timestamp.SCHEMA.name().equals(schema.name())) {
            typename += com.google.protobuf.Timestamp.getDescriptor().getFullName();
            addImportToProtobufSchema(fileDescriptorProtoBuilder, TIMESTAMP_PROTO_IMPORT);
        } else if (Time.SCHEMA.name().equals(schema.name())) {
            typename += com.google.type.TimeOfDay.getDescriptor().getFullName();
            addImportToProtobufSchema(fileDescriptorProtoBuilder, TIMEOFDAY_PROTO_IMPORT);
        }


        return DescriptorProtos.FieldDescriptorProto
                .newBuilder()
                .setType(TYPE_MESSAGE)
                .setTypeName(typename)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }
}
