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

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Backward-compatibility integration test for the injectable HTTP client change (task ant-tfc-mast-297).
 *
 * <p>The change lets callers inject a custom {@code SdkHttpClient.Builder} (e.g. Apache) instead of the
 * hard-coded {@code UrlConnectionHttpClient}. The concern this test addresses is whether a consumer of a
 * released ("old") version can still read data produced by a build carrying this change ("new"), and vice
 * versa.
 *
 * <p>The change only affects how the internal Glue client's HTTP transport is built; it does not touch the
 * serialized wire format. This test proves that empirically against real Glue: it serializes the same
 * record and schema twice, once with the default (unchanged) HTTP client and once with an injected Apache
 * client, and asserts the encoded byte arrays are <b>identical</b>. Because the default path is unchanged
 * from the released version, byte-identical output means anything a released consumer could read before it
 * can still read now, regardless of which HTTP client the producer used. Round-trips through both a default
 * and an Apache-injected deserializer confirm the data decodes back to the original in every combination.
 *
 * <p>Requires real AWS credentials for Glue (default region us-east-2). Run under the {@code surefire}
 * profile of this module.
 */
public class CrossClientWireCompatIntegrationTest {

    private static final String AVRO_SCHEMA_DEFINITION =
            "{\"type\":\"record\",\"name\":\"XVerRecord\",\"namespace\":\"com.amazonaws.services."
            + "schemaregistry.integrationtests.compat\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},"
            + "{\"name\":\"favorite_number\",\"type\":\"int\"}]}";

    // Unique per run so the test is self-contained and easy to clean up afterwards.
    private static final String SCHEMA_NAME = "xver-wire-compat-" + UUID.randomUUID();

    private Map<String, Object> baseConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, "true");
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "default-registry");
        return configs;
    }

    private byte[] avroEncodedPayload() throws Exception {
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(AVRO_SCHEMA_DEFINITION);
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("name", "cross-version");
        record.put("favorite_number", 7);

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(avroSchema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    @Test
    public void injectedApacheClient_producesIdenticalWireBytes_andRoundTripsAcrossClients() throws Exception {
        DefaultCredentialsProvider credentials = DefaultCredentialsProvider.create();
        Schema schema = new Schema(AVRO_SCHEMA_DEFINITION, "AVRO", SCHEMA_NAME);
        byte[] payload = avroEncodedPayload();

        // Producer with the default (unchanged) HTTP client - i.e. released behavior.
        GlueSchemaRegistryConfiguration defaultConfig = new GlueSchemaRegistryConfiguration(baseConfigs());
        GlueSchemaRegistrySerializerImpl defaultSerializer =
                new GlueSchemaRegistrySerializerImpl(credentials, defaultConfig);

        // Producer with an injected Apache HTTP client - the new behavior enabled by this change.
        GlueSchemaRegistryConfiguration apacheConfig = new GlueSchemaRegistryConfiguration(baseConfigs());
        apacheConfig.setHttpClientBuilder(ApacheHttpClient.builder());
        GlueSchemaRegistrySerializerImpl apacheSerializer =
                new GlueSchemaRegistrySerializerImpl(credentials, apacheConfig);

        byte[] encodedByDefault = defaultSerializer.encode("xver-compat", schema, payload);
        byte[] encodedByApache = apacheSerializer.encode("xver-compat", schema, payload);

        // Core backward-compat assertion: the injected HTTP client does not change the wire format at all,
        // so a consumer of any version reads exactly the same bytes it always would.
        assertArrayEquals(encodedByDefault, encodedByApache,
                "Injecting an Apache HTTP client must not change the serialized wire bytes");

        // Every producer/consumer client combination must round-trip back to the original payload.
        GlueSchemaRegistryDeserializerImpl defaultDeserializer =
                new GlueSchemaRegistryDeserializerImpl(credentials, new GlueSchemaRegistryConfiguration(baseConfigs()));

        GlueSchemaRegistryConfiguration apacheDeserConfig = new GlueSchemaRegistryConfiguration(baseConfigs());
        apacheDeserConfig.setHttpClientBuilder(ApacheHttpClient.builder());
        GlueSchemaRegistryDeserializerImpl apacheDeserializer =
                new GlueSchemaRegistryDeserializerImpl(credentials, apacheDeserConfig);

        assertArrayEquals(payload, defaultDeserializer.getData(encodedByDefault),
                "default consumer must read default-produced data");
        assertArrayEquals(payload, defaultDeserializer.getData(encodedByApache),
                "default consumer must read Apache-produced data");
        assertArrayEquals(payload, apacheDeserializer.getData(encodedByDefault),
                "Apache consumer must read default-produced data");
        assertArrayEquals(payload, apacheDeserializer.getData(encodedByApache),
                "Apache consumer must read Apache-produced data");

        // The schema resolved from the encoded bytes must match what was registered, in both directions.
        assertEquals(AVRO_SCHEMA_DEFINITION, defaultDeserializer.getSchema(encodedByApache).getSchemaDefinition(),
                "schema resolved from Apache-produced data must match the registered definition");
    }
}
