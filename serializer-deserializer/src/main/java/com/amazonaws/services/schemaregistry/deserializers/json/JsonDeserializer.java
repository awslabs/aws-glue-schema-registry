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
package com.amazonaws.services.schemaregistry.deserializers.json;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Json specific de-serializer responsible for handling the Json data format
 * specific deserialization behavior.
 */
@Slf4j
@Data
public class JsonDeserializer implements GlueSchemaRegistryDataFormatDeserializer {
    private static final GlueSchemaRegistryDeserializerDataParser DESERIALIZER_DATA_PARSER =
            GlueSchemaRegistryDeserializerDataParser.getInstance();
    /**
     * Upper bound on {@link #warnedClassNames}. The dedup key comes from the schema, which a
     * producer controls, so an unbounded set would grow for the lifetime of the deserializer.
     * <p>
     * The count of distinct class names one deserializer can legitimately see is bounded by the
     * distinct schemas across the topics its consumer reads — typically a handful, and at most
     * tens for a large fan-in consumer. This cap therefore sits above any realistic
     * configuration: reaching it means the allowlist is wrong, or a producer is supplying class
     * names in bulk. Either way 100 warnings have already been emitted, so further ones add no
     * signal and {@link #warnOnceForDisallowedClassName} stops logging.
     */
    static final int MAX_WARNED_CLASS_NAMES = 100;
    private final ObjectMapper objectMapper;
    /** Class names already warned about, so the allowlist-miss warning is not logged per record. */
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final Set<String> warnedClassNames = Collections.newSetFromMap(new ConcurrentHashMap<>());
    /** Guards the one-time notice that warning has stopped, so suppression is never silent. */
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean warnCapNoticeEmitted = new AtomicBoolean(false);
    @Getter
    @Setter
    private GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;

    /**
     * Constructor accepting various dependencies.
     *
     * @param configs configuration elements
     */
    @Builder
    public JsonDeserializer(GlueSchemaRegistryConfiguration configs) {
        this.schemaRegistrySerDeConfigs = configs;
        JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(true);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setNodeFactory(jsonNodeFactory);
        if (configs != null) {
            if (!CollectionUtils.isEmpty(configs.getJacksonSerializationFeatures())) {
                configs.getJacksonSerializationFeatures()
                        .forEach(this.objectMapper::enable);
            }
            if (!CollectionUtils.isEmpty(configs.getJacksonDeserializationFeatures())) {
                configs.getJacksonDeserializationFeatures()
                        .forEach(this.objectMapper::enable);
            }
        }
    }

    /**
     * Deserialize the bytes to the original JSON message given the schema retrieved
     * from the schema registry.
     *
     * @param buffer data to be de-serialized
     * @param schemaObject JSONSchema
     * @return de-serialized object
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    @Override
    public Object deserialize(@NonNull ByteBuffer buffer,
                              @NonNull Schema schemaObject) {
        try {
            String schema = schemaObject.getSchemaDefinition();
            byte[] data = DESERIALIZER_DATA_PARSER.getPlainData(buffer);

            log.debug("Length of actual message: {}", data.length);

            Object deserializedObject;

            JsonNode schemaNode = objectMapper.readTree(schema);
            JsonNode classNameNode = schemaNode.get("className");

            boolean classNameResolutionEnabled = schemaRegistrySerDeConfigs != null
                    && schemaRegistrySerDeConfigs.isJsonClassNameResolutionEnabled();

            if (classNameResolutionEnabled && classNameNode != null) {
                String className = classNameNode.asText();
                if (schemaRegistrySerDeConfigs.isClassNameAllowed(className)) {
                    deserializedObject = objectMapper.readValue(data, Class.forName(className));
                } else {
                    warnOnceForDisallowedClassName(className);
                    JsonNode dataNode = objectMapper.readTree(data);
                    deserializedObject = JsonDataWithSchema.builder(schemaNode.toString(), dataNode.toString())
                            .build();
                }
            } else {
                JsonNode dataNode = objectMapper.readTree(data);
                deserializedObject = JsonDataWithSchema.builder(schemaNode.toString(), dataNode.toString())
                        .build();
            }

            return deserializedObject;
        } catch (IOException | ClassNotFoundException e) {
            String message = String.format("Exception occurred while de-serializing JSON message.");
            throw new AWSSchemaRegistryException(message, e);
        }
    }

    /**
     * Warns that a schema's className was not allowlisted, normally once per distinct class name.
     * The condition is configuration-scoped rather than record-scoped, so logging it on every
     * record would flood the logs at message throughput rate.
     * <p>
     * Dedup state is capped at {@link #MAX_WARNED_CLASS_NAMES} entries so that a stream of
     * distinct schema-supplied class names cannot grow it without bound. On reaching the cap,
     * warning stops rather than falling back to once per record, which would flood the log at
     * message throughput rate. Suppression is announced once so that it is not silent; by that
     * point the cap's worth of warnings has already named the problem.
     * <p>
     * The cap is approximate under concurrency, since the size check and the insert are not
     * atomic with respect to each other. It can be exceeded by roughly the number of threads
     * deserializing at once, which does not affect the ceiling in any meaningful way.
     *
     * @param className the class name named by the schema but absent from the allowlist
     */
    private void warnOnceForDisallowedClassName(String className) {
        if (warnedClassNames.size() >= MAX_WARNED_CLASS_NAMES) {
            if (warnCapNoticeEmitted.compareAndSet(false, true)) {
                log.warn("Reached {} distinct class names outside the allowlist; suppressing "
                         + "further warnings. Review {}.",
                         MAX_WARNED_CLASS_NAMES, AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST);
            }
            return;
        }
        if (warnedClassNames.add(className)) {
            log.warn("className '{}' is not in the configured allowlist. "
                     + "Returning JsonDataWithSchema instead. "
                     + "Add the class to {} to enable typed deserialization.",
                     className, AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST);
        }
    }
}
