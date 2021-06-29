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

import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaConverterConstants;
import com.amazonaws.services.schemaregistry.kafkaconnect.jsonschema.JsonSchemaDataConfig;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.DecimalFormat;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.StringSchema;

import java.math.BigDecimal;
import java.util.Map;

public class DecimalLogicalTypeConverter implements TypeConverter {
    /**
     * DecimalFormat is a Connect logical type that can be represented in Numerical or Base64 encoded
     * binary.
     * <p>
     * https://cwiki.apache.org/confluence/display/KAFKA/KIP-481%3A+SerDe+Improvements+for+Connect+Decimal+type+in+JSON
     * <p>
     * As per KIP-481 :
     * <p>
     * Most JSON data that utilizes precise decimal data represents it as a decimal number. Connect, on the other
     * hand, only supports a binary BASE64 string encoding (see example below). This KIP intends to support both
     * representations so that it can better integrate with legacy systems (and make the internal topic data easier
     * to read/debug):
     * <p>
     * serialize the decimal field "foo" with value "10.2345" with the BASE64 setting: {"foo": "D3J5"}
     * serialize the decimal field "foo" with value "10.2345" with the NUMERIC setting: {"foo": 10.2345}
     *
     * @param schema connectSchema
     * @param value  connectValue
     * @param config jsonSchemaDataConfig
     * @return JsonNode converted json Object
     */
    @Override
    public JsonNode toJson(final Schema schema,
                           final Object value,
                           final JsonSchemaDataConfig config) {
        if (!(value instanceof BigDecimal)) {
            throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
        }

        final BigDecimal decimal = (BigDecimal) value;
        switch (config.getDecimalFormat()) {
            case NUMERIC:
                return JSON_NODE_FACTORY.numberNode(decimal);
            case BASE64:
                return JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
            default:
                throw new DataException(
                        "Unexpected " + JsonSchemaDataConfig.DECIMAL_FORMAT_CONFIG + ": " + config.getDecimalFormat());
        }
    }

    @Override
    public org.everit.json.schema.Schema.Builder toJsonSchema(final Schema schema,
                                                              final Map<String, Object> unprocessedProperties,
                                                              final JsonSchemaDataConfig jsonSchemaDataConfig) {
        org.everit.json.schema.Schema.Builder baseSchemaBuilder;
        String connectType = schema.type()
                .getName()
                .toLowerCase();
        if (DecimalFormat.NUMERIC.equals(jsonSchemaDataConfig.getDecimalFormat())) {
            baseSchemaBuilder = NumberSchema.builder();
        } else {
            baseSchemaBuilder = StringSchema.builder();
        }
        unprocessedProperties.put(JsonSchemaConverterConstants.CONNECT_TYPE_PROP, connectType);
        return baseSchemaBuilder;
    }

    @Override
    public Object toConnect(final Schema schema,
                            final JsonNode value,
                            final JsonSchemaDataConfig jsonSchemaDataConfig) {
        if (value.isNumber()) {
            BigDecimal connectValue = value.decimalValue();
            return connectValue;
        }
        if (value.isBinary() || value.isTextual()) {
            try {
                return Decimal.toLogical(schema, value.binaryValue());
            } catch (Exception e) {
                throw new DataException("Invalid bytes for Decimal field", e);
            }
        }

        throw new DataException(
                "Invalid type for Decimal, underlying representation should be numeric or bytes but was "
                + value.getNodeType());
    }

    @Override
    public SchemaBuilder toConnectSchema(org.everit.json.schema.Schema jsonSchema,
                                         JsonSchemaDataConfig jsonSchemaDataConfig) {
        throw new DataException("Invalid type for Decimal, underlying type should be numeric or bytes.");
    }
}
