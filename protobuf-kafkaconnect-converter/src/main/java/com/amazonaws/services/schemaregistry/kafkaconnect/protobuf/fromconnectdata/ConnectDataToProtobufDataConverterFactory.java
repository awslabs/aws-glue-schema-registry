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

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isEnumType;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isTimeType;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectDataToProtobufDataConverterFactory {
    public static DataConverter get(final Schema connectSchema) {
        final Schema.Type connectType = connectSchema.type();

        if (isEnumType(connectSchema)) {
            return new EnumDataConverter();
        } else if (isTimeType(connectSchema)) {
            return new TimeDataConverter();
        } else if (Decimal.schema(DECIMAL_DEFAULT_SCALE).name().equals(connectSchema.name())) {
            return new DecimalDataConverter();
        } else if (connectType.isPrimitive()) {
            return new PrimitiveDataConverter();
        } else if (connectType.equals(Schema.Type.ARRAY)) {
            return new ArrayDataConverter();
        } else if (connectType.equals(Schema.Type.STRUCT)) {
            return new StructDataConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + connectType);
    }
}
