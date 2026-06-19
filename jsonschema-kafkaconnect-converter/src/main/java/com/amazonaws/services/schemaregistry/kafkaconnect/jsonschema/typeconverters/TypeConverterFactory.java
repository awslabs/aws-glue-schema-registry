/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
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
package com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.typeconverters;

import lombok.NonNull;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.StringSchema;
import org.json.JSONObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory to create a new instance of TypeConverter.
 */
public class TypeConverterFactory {
    private final Map<String, TypeConverter> typeConverterMap = new ConcurrentHashMap<>();

    /**
     * Returns a specific TypeConverter instance based on type of connect type property or type of jsonschema
     *
     * @param jsonSchema Json schema
     * @return specific TypeConverter instance.
     */
    public TypeConverter get(@NonNull org.everit.json.schema.Schema jsonSchema,
                             String connectType) {
        TypeConverter typeConverter = null;

        if (jsonSchema instanceof BooleanSchema) {
            typeConverter = get(Schema.Type.BOOLEAN);
        } else if (jsonSchema instanceof NumberSchema) {
            // If no connect type passed then assume that connect schema is for FLOAT64 type data
            JSONObject parsedSchema = new JSONObject(jsonSchema.toString());
            if (parsedSchema.get("type") == "integer") {
                typeConverter = get(Schema.Type.INT64);
            } else if (connectType == null) {
                typeConverter = get(Schema.Type.valueOf("FLOAT64"));
            } else {
                typeConverter = get(Schema.Type.valueOf(connectType.toUpperCase()));
            }
        } else if (jsonSchema instanceof StringSchema) {
            typeConverter = "bytes".equals(connectType) ? get(Schema.Type.BYTES) : get(Schema.Type.STRING);
        } else if (jsonSchema instanceof EnumSchema) {
            typeConverter = get(Schema.Type.STRING);
        } else if (jsonSchema instanceof ArraySchema) {
            if ("map".equals(connectType)) {
                typeConverter = get(Schema.Type.MAP);
            } else {
                typeConverter = get(Schema.Type.ARRAY);
            }
        } else if (jsonSchema instanceof ObjectSchema) {
            if ("map".equals(connectType)) {
                typeConverter = get(Schema.Type.MAP);
            } else {
                typeConverter = get(Schema.Type.STRUCT);
            }
        }

        return typeConverter;
    }

    /**
     * Returns a specific TypeConverter instance based on logical type from schema name or type of schema
     *
     * @param schema Connect schema
     * @return specific TypeConverter instance.
     */
    public TypeConverter get(@NonNull Schema schema) {
        if (schema.name() != null && get(schema.name()) != null) {
            return get(schema.name());
        } else {
            return get(schema.type());
        }
    }

    /**
     * Lazy initializes and returns a specific TypeConverter instance.
     *
     * @param connectType logicalName for kafka connect logical type
     * @return specific TypeConverter instance.
     */
    public TypeConverter get(@NonNull Schema.Type connectType) {
        switch (connectType) {
            case INT8:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new Int8TypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case INT16:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new Int16TypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case INT32:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new Int32TypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case INT64:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new Int64TypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case FLOAT32:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new Float32TypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case FLOAT64:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new Float64TypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case BOOLEAN:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new BooleanTypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case BYTES:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new BytesTypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case STRING:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new StringTypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case ARRAY:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new ArrayTypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case MAP:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new MapTypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            case STRUCT:
                this.typeConverterMap.computeIfAbsent(connectType.getName(), key -> new StructTypeConverter());

                return this.typeConverterMap.get(connectType.getName());
            default:
                String message = String.format("Unsupported connect type: %s", connectType.getName());
                throw new DataException(message);
        }
    }

    /**
     * Lazy initializes and returns a specific TypeConverter instance.
     *
     * @param logicalName logicalName for kafka connect logical type
     * @return specific TypeConverter instance.
     */
    public TypeConverter get(@NonNull String logicalName) {
        switch (logicalName) {
            case Decimal.LOGICAL_NAME:
                this.typeConverterMap.computeIfAbsent(logicalName, key -> new DecimalLogicalTypeConverter());

                return this.typeConverterMap.get(logicalName);
            case Date.LOGICAL_NAME:
                this.typeConverterMap.computeIfAbsent(logicalName, key -> new DateLogicalTypeConverter());

                return this.typeConverterMap.get(logicalName);
            case Time.LOGICAL_NAME:
                this.typeConverterMap.computeIfAbsent(logicalName, key -> new TimeLogicalTypeConverter());

                return this.typeConverterMap.get(logicalName);
            case Timestamp.LOGICAL_NAME:
                this.typeConverterMap.computeIfAbsent(logicalName, key -> new TimestampLogicalTypeConverter());

                return this.typeConverterMap.get(logicalName);
            default:
                return null;
        }
    }
}
