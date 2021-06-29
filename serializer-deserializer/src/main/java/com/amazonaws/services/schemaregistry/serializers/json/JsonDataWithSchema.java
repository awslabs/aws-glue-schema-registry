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
package com.amazonaws.services.schemaregistry.serializers.json;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;


/**
 * Wrapper object that contains schema string and json data string
 * This works similar to the notion of GenericRecord in Avro.
 * This can be passed as an input to the serializer for json data format.
 */
@Getter
@ToString
@Builder
@EqualsAndHashCode
public class JsonDataWithSchema {
    /**
     * Json Schema string
     */
    private final String schema;
    /**
     * json data/payload/document to be serialized
     */
    private final String payload;

    /**
     * Builder method
     *
     * @param schema  json schema string
     * @param payload json schema data
     * @return JsonDataWithSchemaBuilder builder objcect
     */
    public static JsonDataWithSchemaBuilder builder(final String schema,
                                                    final String payload) {
        if (StringUtils.isBlank(schema)) {
            throw new IllegalArgumentException("schema can't be blank/empty/null");
        }
        return new JsonDataWithSchemaBuilder().schema(schema)
                .payload(payload);
    }

    // To keep JavaDoc plugin happy
    public static class JsonDataWithSchemaBuilder {
    }
}
