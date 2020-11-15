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

package com.amazonaws.services.schemaregistry.caching;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AWSSchemaRegistrySerializerCacheTest {

    private static final UUID TEST_GENERIC_SCHEMA_VERSION_ID = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private static final UUID TEST_GENERIC_SCHEMA_VERSION_ID_2 = UUID.fromString("0266b958-cba5-11ea-87d0-0242ac130003");
    private static final UUID TEST_GENERIC_SCHEMA_VERSION_ID_3 = UUID.fromString("5e801d44-5bf7-4053-9820-f540bd733ad8");
    public static final String AVRO_USER_MIXED_TYPE_SCHEMA_FILE = "src/test/java/resources/avro/user3.avsc";
    public static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";
    private final Map<String, Object> configs = new HashMap<>();
    private GlueSchemaRegistryConfiguration serdeConfigs;
    private AWSSchemaRegistrySerializerCache awsSchemaRegistrySerializerCache;

    @BeforeEach
    public void setup() {
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "http://test");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.CACHE_SIZE, "2");
        configs.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "5000");
        serdeConfigs = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistrySerializerCache = AWSSchemaRegistrySerializerCache.getInstance(serdeConfigs);
    }

    @AfterEach
    public void tearDown() {
        awsSchemaRegistrySerializerCache.flushCache();
    }

    @Test
    public void testGet_putKeyValue_keyRetrievedFromCache() throws Exception {
        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey(), TEST_GENERIC_SCHEMA_VERSION_ID);

        org.apache.avro.Schema schemaToTest = getSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE);
        String matchingSchemaDefinitionToTest = schemaToTest.toString();

        Schema keyObjToTest = new Schema(matchingSchemaDefinitionToTest, DataFormat.AVRO.name(),
                "test-schema");

        assertNotNull(awsSchemaRegistrySerializerCache.get(keyObjToTest));
        assertEquals(TEST_GENERIC_SCHEMA_VERSION_ID, awsSchemaRegistrySerializerCache.get(keyObjToTest));
    }

    @Test
    public void testGet_putDifferentKeyValue_keyNotFoundInCache() throws Exception {
        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey(), TEST_GENERIC_SCHEMA_VERSION_ID);

        org.apache.avro.Schema schemaToTest = getSchema(AVRO_USER_SCHEMA_FILE);
        String differentSchemaDefinitionToTest = schemaToTest.toString();

        Schema keyObjToTest = new Schema(differentSchemaDefinitionToTest, DataFormat.AVRO.name(),
                "test-schema");

        assertNull(awsSchemaRegistrySerializerCache.get(keyObjToTest));
    }

    @Test
    public void testCacheSize_exceedCacheSizeLimit_cacheSizeInLimit() throws Exception {
        int cacheSizeLimit = serdeConfigs.getCacheSize();

        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey(), TEST_GENERIC_SCHEMA_VERSION_ID);
        assertEquals(1, awsSchemaRegistrySerializerCache.getCacheSize());

        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey2(), TEST_GENERIC_SCHEMA_VERSION_ID_2);
        assertEquals(cacheSizeLimit, awsSchemaRegistrySerializerCache.getCacheSize());

        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey3(), TEST_GENERIC_SCHEMA_VERSION_ID_3);
        assertEquals(cacheSizeLimit, awsSchemaRegistrySerializerCache.getCacheSize());
    }

    @Test
    public void testLRU_putMultipleKeyValues_keysAndSizeMatchByLRURules() throws Exception {
        int cacheSizeLimit = serdeConfigs.getCacheSize();

        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey(), TEST_GENERIC_SCHEMA_VERSION_ID);
        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey2(), TEST_GENERIC_SCHEMA_VERSION_ID_2);
        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey3(), TEST_GENERIC_SCHEMA_VERSION_ID_3);

        assertNull(awsSchemaRegistrySerializerCache.get(getAWSSchemaRegistryMetaDataKey()));
        assertEquals(cacheSizeLimit, awsSchemaRegistrySerializerCache.getCacheSize());

        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey(), TEST_GENERIC_SCHEMA_VERSION_ID);
        assertNull(awsSchemaRegistrySerializerCache.get(getAWSSchemaRegistryMetaDataKey2()));
        assertEquals(cacheSizeLimit, awsSchemaRegistrySerializerCache.getCacheSize());

        awsSchemaRegistrySerializerCache.get(getAWSSchemaRegistryMetaDataKey3());
        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey2(), TEST_GENERIC_SCHEMA_VERSION_ID_2);
        assertNull(awsSchemaRegistrySerializerCache.get(getAWSSchemaRegistryMetaDataKey()));
        assertEquals(cacheSizeLimit, awsSchemaRegistrySerializerCache.getCacheSize());
    }

    @Test
    public void testSerializerCache_flushCache_cacheIsEmpty() throws Exception {
        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey(), TEST_GENERIC_SCHEMA_VERSION_ID);
        awsSchemaRegistrySerializerCache.flushCache();

        assertEquals(0, awsSchemaRegistrySerializerCache.getCacheSize());
    }

    @Test
    public void testSerializerCache_deleteFromCache_deletesSuccessfully() throws Exception {
        awsSchemaRegistrySerializerCache.put(getAWSSchemaRegistryMetaDataKey(), TEST_GENERIC_SCHEMA_VERSION_ID);

        awsSchemaRegistrySerializerCache.delete(getAWSSchemaRegistryMetaDataKey());

        assertNull(awsSchemaRegistrySerializerCache.get(getAWSSchemaRegistryMetaDataKey()));
    }

    @Test
    public void testConfigs_invalidCacheSize_throwsException() {
        String invalidSize = "xc";
        configs.put(AWSSchemaRegistryConstants.CACHE_SIZE, invalidSize);

        AWSSchemaRegistryException awsSchemaRegistryException = Assertions.assertThrows(AWSSchemaRegistryException.class, () ->
                new GlueSchemaRegistryConfiguration(configs));

        String expectedMsg = String.format("Cache size property is not a valid size : %s", invalidSize);
        assertEquals(expectedMsg, awsSchemaRegistryException.getMessage());
    }

    @Test
    public void testConfigs_invalidCacheTTL_throwsException() {
        String invalidCacheTTL = "xbcvnxb";
        configs.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, invalidCacheTTL);

        AWSSchemaRegistryException awsSchemaRegistryException = Assertions.assertThrows(AWSSchemaRegistryException.class, () ->
                new GlueSchemaRegistryConfiguration(configs));

        String expectedMsg = String.format("Time to live cache property is not a valid time : %s", invalidCacheTTL);
        assertEquals(awsSchemaRegistryException.getMessage(), expectedMsg);
    }

    @Test
    public void testCacheStats_getKey_hitCountMatches() throws Exception {
        Schema objToTest = getAWSSchemaRegistryMetaDataKey();
        awsSchemaRegistrySerializerCache.put(objToTest, TEST_GENERIC_SCHEMA_VERSION_ID);

        long expectedNumOfHits = 10;
        for(long  i=0; i < expectedNumOfHits; i++) {
            assertNotNull(awsSchemaRegistrySerializerCache.get(objToTest));
        }

        assertEquals(expectedNumOfHits, awsSchemaRegistrySerializerCache.getCacheStats().hitCount());
    }

    @Test
    public void testGetInstance_nullConfigs_throwsException() {
        assertThrows(IllegalArgumentException.class, () -> AWSSchemaRegistrySerializerCache.getInstance(null));
    }

    private Schema getAWSSchemaRegistryMetaDataKey() throws Exception {
        org.apache.avro.Schema schema = getSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);

        return new Schema(schemaDefinition, DataFormat.AVRO.name(),
                "test-schema");
    }

    private Schema getAWSSchemaRegistryMetaDataKey2() throws Exception {
        org.apache.avro.Schema schema = getSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);

        return new Schema(schemaDefinition, "AVRO1",
                "test-schema");
    }

    private Schema getAWSSchemaRegistryMetaDataKey3() throws Exception {
        org.apache.avro.Schema schema = getSchema(AVRO_USER_MIXED_TYPE_SCHEMA_FILE);

        GenericData.EnumSymbol k = new GenericData.EnumSymbol(schema, "ONE");
        ArrayList<Integer> al = new ArrayList<>();
        al.add(1);

        GenericData.Record genericRecordWithAllTypes = new GenericData.Record(schema);
        Map<String, Long> map = new HashMap<>();
        map.put("test", 1L);

        genericRecordWithAllTypes.put("name", "Joe");
        genericRecordWithAllTypes.put("favorite_number", 1);
        genericRecordWithAllTypes.put("meta", map);
        genericRecordWithAllTypes.put("listOfColours", al);
        genericRecordWithAllTypes.put("integerEnum", k);

        String schemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericRecordWithAllTypes);

        return new Schema(schemaDefinition, "Protobuff",
                "test-schema");
    }

    private org.apache.avro.Schema getSchema(String fileName) throws IOException {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return parser.parse(new File(fileName));
    }
}