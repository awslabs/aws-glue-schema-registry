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

package com.amazonaws.services.schemaregistry.integrationtests.compat;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGenerator;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorFactory;
import com.amazonaws.services.schemaregistry.integrationtests.generators.TestDataGeneratorType;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerFactory;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Compatibility;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.DeleteSchemaRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.SchemaId;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Backward-compatibility integration test for the injectable HTTP client change (task ant-tfc-mast-297).
 *
 * <p>The change lets callers inject a custom {@code SdkHttpClient.Builder} (e.g. Apache) instead of the
 * hard-coded {@code UrlConnectionHttpClient}. The concern this test addresses is whether a consumer of a
 * released ("old") version can still read data produced by a build carrying this change ("new"), and vice
 * versa, for every supported data format.
 *
 * <p>The change only affects how the internal Glue client's HTTP transport is built; it does not touch the
 * serialized wire format. This test proves that empirically against real Glue for <b>AVRO, JSON, and
 * PROTOBUF</b>: for each format it serializes the same record twice, once with the default (unchanged) HTTP
 * client and once with an injected Apache client, and asserts that <b>both the schema and the data</b> are
 * encoded identically. Because the default path is unchanged from the released version, byte-identical
 * output means anything a released consumer could read before it can still read now, regardless of which
 * HTTP client the producer used. Round-trips through both a default and an Apache-injected deserializer
 * confirm the schema and data decode back to the original in every producer/consumer combination.
 *
 * <p>{@link #injectedClientBuildFailure_surfacesError()} is the negative case: an injected client pointed at
 * an unreachable endpoint must surface an error rather than silently succeeding.
 *
 * <p>Requires real AWS credentials for Glue (default region us-east-2). Run under the {@code surefire}
 * profile of this module. Schemas created here are deleted in {@link #cleanUpSchemas()}.
 */
public class CrossClientWireCompatIntegrationTest {

    private static final String REGION = "us-east-2";
    private static final String REGISTRY_NAME = "default-registry";
    private static final String TRANSPORT_NAME = "xver-compat";

    private final DefaultCredentialsProvider credentials = DefaultCredentialsProvider.create();
    private final GlueSchemaRegistrySerializerFactory serializerFactory = new GlueSchemaRegistrySerializerFactory();
    private final TestDataGeneratorFactory testDataGeneratorFactory = new TestDataGeneratorFactory();

    // Schema names created by this test, deleted in @AfterAll so each run leaves the registry clean.
    private static final Set<String> SCHEMAS_TO_CLEAN_UP = ConcurrentHashMap.newKeySet();

    private Map<String, Object> baseConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, REGION);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, REGISTRY_NAME);
        return configs;
    }

    private GlueSchemaRegistryConfiguration defaultConfig() {
        return new GlueSchemaRegistryConfiguration(baseConfigs());
    }

    private GlueSchemaRegistryConfiguration apacheConfig() {
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(baseConfigs());
        config.setHttpClientBuilder(ApacheHttpClient.builder());
        return config;
    }

    /**
     * For each supported format: the default HTTP client and an injected Apache HTTP client must produce
     * byte-identical schema and data, and every producer/consumer client combination must round-trip the
     * schema and data back to the original.
     */
    @ParameterizedTest(name = "{0}")
    @EnumSource(value = DataFormat.class, names = {"AVRO", "JSON", "PROTOBUF"})
    public void injectedApacheClient_producesIdenticalSchemaAndData_andRoundTrips(DataFormat dataFormat)
            throws Exception {
        // A generic, no-compatibility record for this format, reusing the shared integration-test generators.
        TestDataGeneratorType generatorType =
                TestDataGeneratorType.valueOf(dataFormat, AvroRecordType.GENERIC_RECORD, Compatibility.NONE);
        TestDataGenerator<?> generator = testDataGeneratorFactory.getInstance(generatorType);
        Object record = generator.createRecords().get(0);

        String schemaName = "xver-wire-compat-" + dataFormat.name() + "-" + UUID.randomUUID();
        SCHEMAS_TO_CLEAN_UP.add(schemaName);

        // Derive the schema definition and the pre-GSR serialized payload from the record (format-specific,
        // HTTP-client-independent).
        GlueSchemaRegistryDataFormatSerializer formatSerializer =
                serializerFactory.getInstance(dataFormat, defaultConfig());
        String schemaDefinition = formatSerializer.getSchemaDefinition(record);
        byte[] payload = formatSerializer.serialize(record);
        Schema schema = new Schema(schemaDefinition, dataFormat.name(), schemaName);

        // Producers: default (released behavior) vs injected Apache client (new behavior).
        GlueSchemaRegistrySerializerImpl defaultSerializer =
                new GlueSchemaRegistrySerializerImpl(credentials, defaultConfig());
        GlueSchemaRegistrySerializerImpl apacheSerializer =
                new GlueSchemaRegistrySerializerImpl(credentials, apacheConfig());

        byte[] encodedByDefault = defaultSerializer.encode(TRANSPORT_NAME, schema, payload);
        byte[] encodedByApache = apacheSerializer.encode(TRANSPORT_NAME, schema, payload);

        // Core backward-compat assertion: the injected HTTP client changes neither the schema nor the data
        // on the wire, so a consumer of any version reads exactly the same bytes it always would.
        assertArrayEquals(encodedByDefault, encodedByApache,
                dataFormat + ": injecting an Apache HTTP client must not change the serialized wire bytes");

        // Consumers: default vs injected Apache client.
        GlueSchemaRegistryDeserializerImpl defaultDeserializer =
                new GlueSchemaRegistryDeserializerImpl(credentials, defaultConfig());
        GlueSchemaRegistryDeserializerImpl apacheDeserializer =
                new GlueSchemaRegistryDeserializerImpl(credentials, apacheConfig());

        // Data must round-trip in every producer/consumer combination.
        assertArrayEquals(payload, defaultDeserializer.getData(encodedByDefault),
                dataFormat + ": default consumer must read default-produced data");
        assertArrayEquals(payload, defaultDeserializer.getData(encodedByApache),
                dataFormat + ": default consumer must read Apache-produced data");
        assertArrayEquals(payload, apacheDeserializer.getData(encodedByDefault),
                dataFormat + ": Apache consumer must read default-produced data");
        assertArrayEquals(payload, apacheDeserializer.getData(encodedByApache),
                dataFormat + ": Apache consumer must read Apache-produced data");

        // Schema must also resolve identically regardless of which client produced or consumed the bytes.
        String schemaFromDefault = defaultDeserializer.getSchema(encodedByDefault).getSchemaDefinition();
        String schemaFromApache = apacheDeserializer.getSchema(encodedByApache).getSchemaDefinition();
        assertEquals(schemaDefinition, schemaFromDefault,
                dataFormat + ": schema resolved from default-produced data must match the registered definition");
        assertEquals(schemaDefinition, schemaFromApache,
                dataFormat + ": schema resolved from Apache-produced data must match the registered definition");
        assertEquals(schemaFromDefault, schemaFromApache,
                dataFormat + ": the resolved schema must be identical across default and injected clients");
    }

    /**
     * Negative case: an injected HTTP client that cannot reach Glue (unresolvable endpoint) must surface an
     * error on use rather than silently succeeding. This confirms failures from the injected transport
     * propagate to the caller.
     */
    @Test
    public void injectedClientBuildFailure_surfacesError() throws Exception {
        DataFormat dataFormat = DataFormat.AVRO;
        TestDataGeneratorType generatorType =
                TestDataGeneratorType.valueOf(dataFormat, AvroRecordType.GENERIC_RECORD, Compatibility.NONE);
        Object record = testDataGeneratorFactory.getInstance(generatorType).createRecords().get(0);

        GlueSchemaRegistryConfiguration badEndpointConfig = new GlueSchemaRegistryConfiguration(baseConfigs());
        // Inject an Apache client with a short timeout and point Glue at an unresolvable endpoint so the call
        // fails fast rather than hanging.
        badEndpointConfig.setHttpClientBuilder(
                ApacheHttpClient.builder().connectionTimeout(Duration.ofSeconds(2)));
        badEndpointConfig.setEndPoint("https://glue.this-endpoint-does-not-exist.aws.invalid");

        GlueSchemaRegistryDataFormatSerializer formatSerializer =
                serializerFactory.getInstance(dataFormat, defaultConfig());
        Schema schema = new Schema(formatSerializer.getSchemaDefinition(record), dataFormat.name(),
                "xver-wire-compat-negative-" + UUID.randomUUID());
        byte[] payload = formatSerializer.serialize(record);

        GlueSchemaRegistrySerializerImpl badSerializer =
                new GlueSchemaRegistrySerializerImpl(credentials, badEndpointConfig);

        // Registration against the unreachable endpoint must throw; the injected client does not mask it.
        assertThrows(Exception.class, () -> badSerializer.encode(TRANSPORT_NAME, schema, payload),
                "an injected client pointed at an unreachable endpoint must surface an error");
    }

    @AfterAll
    public static void cleanUpSchemas() {
        if (SCHEMAS_TO_CLEAN_UP.isEmpty()) {
            return;
        }
        try (GlueClient glueClient = GlueClient.builder()
                .region(Region.of(REGION))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build()) {
            for (String schemaName : SCHEMAS_TO_CLEAN_UP) {
                try {
                    glueClient.deleteSchema(DeleteSchemaRequest.builder()
                            .schemaId(SchemaId.builder()
                                    .registryName(REGISTRY_NAME)
                                    .schemaName(schemaName)
                                    .build())
                            .build());
                } catch (EntityNotFoundException ignored) {
                    // Already gone (e.g. never registered) - nothing to clean up.
                }
            }
        }
        SCHEMAS_TO_CLEAN_UP.clear();
    }
}
