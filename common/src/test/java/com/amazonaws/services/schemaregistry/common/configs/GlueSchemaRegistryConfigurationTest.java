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

package com.amazonaws.services.schemaregistry.common.configs;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.services.glue.model.Compatibility;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for testing configuration elements.
 */
public class GlueSchemaRegistryConfigurationTest {

    HashMap<String, Object> configs = new HashMap<>();

    /**
     * Sets up test data before each test is run.
     */
    @BeforeEach
    public void setup() {
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
    }


    /**
     * Tears down test data after each test is run.
     */
    @AfterEach
    public void tearDown() {
        configs.clear();
    }

    /**
     * Tests building configuration and checking for values.
     */
    @Test
    public void testBuildConfig_fromMap_succeeds() {
        Map<String, Object> configs = new HashMap<>();
        Map<String, String> metadata = new HashMap<>();
        metadata.put(AWSSchemaRegistryConstants.TRANSPORT_METADATA_KEY, "default-topic");

        configs.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name());
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test/");
        configs.put(AWSSchemaRegistryConstants.CACHE_SIZE, "1000");
        configs.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "100");
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        configs.put(AWSSchemaRegistryConstants.METADATA, metadata);

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);

        assertEquals(glueSchemaRegistryConfiguration.getCompressionType().name(), AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name());
        assertEquals("US-West-1", glueSchemaRegistryConfiguration.getRegion());
        assertEquals("https://test/", glueSchemaRegistryConfiguration.getEndPoint());
        assertEquals(1000, glueSchemaRegistryConfiguration.getCacheSize());
        assertEquals(100, glueSchemaRegistryConfiguration.getTimeToLiveMillis());
        assertEquals(AvroRecordType.GENERIC_RECORD, glueSchemaRegistryConfiguration.getAvroRecordType());
        assertEquals(metadata, glueSchemaRegistryConfiguration.getMetadata());
    }

    /**
     * Tests configuration for region value
     */
    @Test
    public void testBuildConfig_noRegionConfigsSupplied_throwsException() {
        Map<String, Object> configWithoutRegion = new HashMap<>();
        configWithoutRegion.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test/");
        System.setProperty("aws.profile", "");

        Exception exception = assertThrows(AWSSchemaRegistryException.class,
                () -> new GlueSchemaRegistryConfiguration(configWithoutRegion));

        assertEquals("Region is not defined in the properties", exception.getMessage());
    }

    /**
     * Tests configuration for region value via default AWS region provider chain
     */
    @Test
    public void testBuildConfig_regionConfigsSuppliedUsingAwsProvider_thenUseDefaultAwsRegionProviderChain() {
        Map<String, Object> configWithoutRegion = new HashMap<>();
        configWithoutRegion.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test/");
        System.setProperty("aws.region", "us-west-2");

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configWithoutRegion);

        assertEquals("us-west-2", glueSchemaRegistryConfiguration.getRegion());

        System.clearProperty("aws.region");
    }

    /**
     * Tests configuration for region value
     */
    @Test
    public void testBuildConfig_withRegionConfig_Instantiates() {
        assertDoesNotThrow(() -> new GlueSchemaRegistryConfiguration("us-west-1"));
    }

    /**
     * Tests building configuration and checking for values.
     */
    @Test
    public void testBuildConfig_fromProperties_succeeds() {
        Properties props = createTestProperties();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertEquals("US-West-1", glueSchemaRegistryConfiguration.getRegion());
        assertEquals(1000, glueSchemaRegistryConfiguration.getCacheSize());
        assertEquals(100, glueSchemaRegistryConfiguration.getTimeToLiveMillis());
        assertEquals(AvroRecordType.GENERIC_RECORD, glueSchemaRegistryConfiguration.getAvroRecordType());
    }

    /**
     * Helper method to setup base configuration property elements.
     *
     * @return Properties instance
     */
    private Properties createTestProperties() {
        Properties props = new Properties();
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        props.put(AWSSchemaRegistryConstants.CACHE_SIZE, "1000");
        props.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "100");
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        return props;
    }

    /**
     * Tests invalid cacheTTL value.
     */
    @Test
    public void testBuildConfig_cacheTTLAsString_throwsException() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.CACHE_TIME_TO_LIVE_MILLIS, "Random String");

        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> new GlueSchemaRegistryConfiguration(props));

        assertEquals("Time to live cache property is not a valid time : Random String", exception.getMessage());
    }

    /**
     * Tests invalid cacheSize value.
     */
    @Test
    public void testBuildConfig_cacheSizeAsString_throwsException() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.CACHE_SIZE, "Random String");

        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> new GlueSchemaRegistryConfiguration(props));

        assertEquals("Cache size property is not a valid size : Random String", exception.getMessage());
    }

    /**
     * Tests default values are used if not passed
     */
    @Test
    public void testBuildConfig_valuesNotPassed_usesDefault() {
        Properties props = new Properties();
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");

        GlueSchemaRegistryConfiguration serDeConfigs = new GlueSchemaRegistryConfiguration(props);
        assertNotNull(serDeConfigs.getCacheSize());
        assertNotNull(serDeConfigs.getTimeToLiveMillis());
        assertNotNull(serDeConfigs.getCompressionType().equals(AWSSchemaRegistryConstants.COMPRESSION.NONE));
        assertNotNull(serDeConfigs.getCompatibilitySetting().equals(Compatibility.NONE));
    }


    /**
     * Tests compatibility value
     */
    @ParameterizedTest
    @EnumSource(value = Compatibility.class, names = {"UNKNOWN_TO_SDK_VERSION"}, mode = EnumSource.Mode.EXCLUDE)
    public void testBuildConfig_validCompatibilitySetting_succeeds(Compatibility compatibility) {
        Properties props = new Properties();
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        props.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, compatibility.name());

        GlueSchemaRegistryConfiguration serDeConfigs = new GlueSchemaRegistryConfiguration(props);

        assertNotNull(serDeConfigs.getCompatibilitySetting());
        assertEquals(compatibility.name(), serDeConfigs.getCompatibilitySetting().name());


        props.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, compatibility.name().toLowerCase());
        serDeConfigs = new GlueSchemaRegistryConfiguration(props);

        assertNotNull(serDeConfigs.getCompatibilitySetting());
        assertEquals(compatibility.name(), serDeConfigs.getCompatibilitySetting().name());
    }

    /**
     * Tests invalid compatibility value
     */
    @Test
    public void testBuildConfig_invalidCompatibilitySetting_throwsException() {
        Properties props = new Properties();
        props.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        props.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "backwards_full"); //invalid compatibility type

        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> new GlueSchemaRegistryConfiguration(props));
        assertTrue(exception.getMessage().contains("Invalid compatibility setting : backwards_full"));
    }

    /**
     * Tests valid compression value.
     */
    @ParameterizedTest
    @EnumSource(AWSSchemaRegistryConstants.COMPRESSION.class)
    public void testBuildConfig_validCompressionType_succeeds(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name());
        GlueSchemaRegistryConfiguration serDeConfigs = new GlueSchemaRegistryConfiguration(props);

        assertNotNull(serDeConfigs.getCompressionType());
        assertEquals(compressionType.name(), serDeConfigs.getCompressionType().name());

        props.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, compressionType.name().toLowerCase());
        serDeConfigs = new GlueSchemaRegistryConfiguration(props);

        assertNotNull(serDeConfigs.getCompressionType());
        assertEquals(compressionType.name(), serDeConfigs.getCompressionType().name());
    }

    /**
     * Tests invalid compression value.
     */
    @Test
    public void testBuildConfig_invalidCompressionType_throwsException() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, "Random String");

        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> new GlueSchemaRegistryConfiguration(props));

        assertTrue(exception.getMessage().contains("Invalid Compression type"));
    }

    /**
     * Tests valid configuration tags value.
     */
    @Test
    public void testBuildConfig_validTags_succeeds() {
        Map<String, String> testTags = new HashMap<>();
        testTags.put("testTagKey","testTagValue");
        testTags.put("testTagKey2","testTagValue2");

        configs.put(AWSSchemaRegistryConstants.TAGS, testTags);
        GlueSchemaRegistryConfiguration serDeConfigs = new GlueSchemaRegistryConfiguration(configs);

        assertNotNull(serDeConfigs.getTags());
        assertTrue(serDeConfigs.getTags().containsKey("testTagKey"));
        assertTrue(serDeConfigs.getTags().containsKey("testTagKey2"));

        assertEquals("testTagValue", serDeConfigs.getTags().get("testTagKey"));
        assertEquals("testTagValue2", serDeConfigs.getTags().get("testTagKey2"));
    }

    /**
     * Tests valid configuration tags value by building config from properties.
     */
    @Test
    public void testBuildConfigWithProperties_validTags_succeeds() {
        Properties props = createTestProperties();
        HashMap<String, String> testTags = new HashMap<>();
        testTags.put("testTagKey","testTagValue");
        testTags.put("testTagKey2","testTagValue2");

        props.put(AWSSchemaRegistryConstants.TAGS, testTags);
        GlueSchemaRegistryConfiguration serDeConfigs = new GlueSchemaRegistryConfiguration(props);

        assertNotNull(serDeConfigs.getTags());

        assertTrue(serDeConfigs.getTags().containsKey("testTagKey"));
        assertTrue(serDeConfigs.getTags().containsKey("testTagKey2"));

        assertEquals("testTagValue", serDeConfigs.getTags().get("testTagKey"));
        assertEquals("testTagValue2", serDeConfigs.getTags().get("testTagKey2"));
    }

    /**
     * Tests invalid tag value.
     */
    @Test
    public void testBuildConfigWithProperties_invalidTags_throwsException() {
        String invalidMapString = "invalidTagString";
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.TAGS, invalidMapString);

        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> new GlueSchemaRegistryConfiguration(props));
        assertTrue(exception.getMessage().contains(AWSSchemaRegistryConstants.TAGS_CONFIG_NOT_HASHMAP_MSG));
    }

    /**
     * Tests invalid metadata value.
     */
    @Test
    public void testBuildAWSAvroSerializer_invalidMetadata_throwsException() {
        List<String> metadata = new ArrayList<>();
        metadata.add("default-topic");
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.METADATA, metadata);

        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> new GlueSchemaRegistryConfiguration(props));

        assertTrue(exception.getMessage().contains("The metadata instance is not a hash map"));
    }

    /**
     * Tests autogenerated description string
     */
    @Test
    public void testValidateAndSetDescription_withoutDescriptionConfig_succeeds() {
        Properties props = createTestProperties();
        props.remove(AWSSchemaRegistryConstants.DESCRIPTION);
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertNotNull(glueSchemaRegistryConfiguration.getDescription());
    }

    /**
     * Tests custom description string
     */
    @Test
    public void testValidateAndSetDescription_withDescriptionConfig_succeeds() {
        Properties props = createTestProperties();
        String expectedDescription = "test-description";
        props.put(AWSSchemaRegistryConstants.DESCRIPTION, expectedDescription);
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertEquals(expectedDescription, glueSchemaRegistryConfiguration.getDescription());
    }

    /**
     * Tests default registry name
     */
    @Test
    public void testValidateAndSetRegistryName_withoutRegistryConfig_throwsException() {
        Properties props = createTestProperties();
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertEquals(AWSSchemaRegistryConstants.DEFAULT_REGISTRY_NAME, glueSchemaRegistryConfiguration.getRegistryName());
    }

    /**
     * Tests custom registry name
     */
    @Test
    public void testValidateAndSetRegistryName_withRegistryConfig_throwsException() {
        String expectedRegistryName = "test-registry";
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, expectedRegistryName);
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertEquals(expectedRegistryName, glueSchemaRegistryConfiguration.getRegistryName());
    }

    /**
     * Tests valid proxy URL value.
     */
    @Test
    public void testBuildConfig_validProxyUrl_success() {
        Properties props = createTestProperties();
        String proxy = "http://proxy.servers.url:8080";
        props.put(AWSSchemaRegistryConstants.PROXY_URL, proxy);
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);
        assertEquals(URI.create(proxy), glueSchemaRegistryConfiguration.getProxyUrl());
    }

    /**
     * Tests invalid proxy URL value.
     */
    @Test
    public void testBuildConfig_invalidProxyUrl_throwsException() {
        Properties props = createTestProperties();
        String proxy = "http:// proxy.url: 8080";
        props.put(AWSSchemaRegistryConstants.PROXY_URL, "http:// proxy.url: 8080");
        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> new GlueSchemaRegistryConfiguration(props));
        assertEquals("Proxy URL property is not a valid URL: "+proxy, exception.getMessage());
    }

    /**
     * Tests that JSON class name resolution defaults to disabled (secure default) when not configured.
     */
    @Test
    public void testJsonClassNameResolution_withoutConfig_defaultsToDisabled() {
        Properties props = createTestProperties();
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertFalse(glueSchemaRegistryConfiguration.isJsonClassNameResolutionEnabled());
    }

    /**
     * Tests that customers can opt in to JSON class name resolution.
     */
    @Test
    public void testJsonClassNameResolution_setToTrue_isEnabled() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED, "true");
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertTrue(glueSchemaRegistryConfiguration.isJsonClassNameResolutionEnabled());
    }

    /**
     * Tests that JSON class name resolution can be explicitly disabled.
     */
    @Test
    public void testJsonClassNameResolution_setToFalse_isDisabled() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED, "false");
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(props);

        assertFalse(glueSchemaRegistryConfiguration.isJsonClassNameResolutionEnabled());
    }

    /**
     * Tests that the JSON class name allowlist is parsed correctly from a comma-separated string.
     */
    @Test
    public void testJsonClassNameAllowlist_commaSeparated_isParsedCorrectly() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST, "com.example.Foo, com.example.Bar");
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(props);

        assertEquals(2, config.getJsonClassNameAllowlist().size());
        assertTrue(config.getJsonClassNameAllowlist().contains("com.example.Foo"));
        assertTrue(config.getJsonClassNameAllowlist().contains("com.example.Bar"));
    }

    /**
     * Tests that the allowlist defaults to empty when not configured.
     */
    @Test
    public void testJsonClassNameAllowlist_notConfigured_defaultsToEmpty() {
        Properties props = createTestProperties();
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(props);

        assertNotNull(config.getJsonClassNameAllowlist());
        assertTrue(config.getJsonClassNameAllowlist().isEmpty());
    }

    /**
     * Tests that the allowlist handles a single class correctly.
     */
    @Test
    public void testJsonClassNameAllowlist_singleClass_isParsedCorrectly() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST, "com.example.SingleClass");
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(props);

        assertEquals(1, config.getJsonClassNameAllowlist().size());
        assertTrue(config.getJsonClassNameAllowlist().contains("com.example.SingleClass"));
    }

    /**
     * Tests that stray commas do not put an empty, never-matching entry into the allowlist.
     */
    @Test
    public void testJsonClassNameAllowlist_strayCommas_areIgnored() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST,
                  ",com.example.Foo,,com.example.Bar, ,");
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(props);

        assertEquals(2, config.getJsonClassNameAllowlist().size());
        assertTrue(config.getJsonClassNameAllowlist().contains("com.example.Foo"));
        assertTrue(config.getJsonClassNameAllowlist().contains("com.example.Bar"));
        assertFalse(config.getJsonClassNameAllowlist().contains(""));
    }

    /**
     * Tests that an allowlist of only separators and whitespace leaves the default empty allowlist.
     */
    @Test
    public void testJsonClassNameAllowlist_onlySeparators_defaultsToEmpty() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST, " , , ");
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(props);

        assertTrue(config.getJsonClassNameAllowlist().isEmpty());
    }

    /**
     * Tests that a value that is neither "true" nor "false" leaves resolution disabled, which is
     * the safe direction for a security opt-in.
     */
    @Test
    public void testJsonClassNameResolution_unrecognizedValue_isDisabled() {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_RESOLUTION_ENABLED, "ture");
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(props);

        assertFalse(config.isJsonClassNameResolutionEnabled());
    }

    /**
     * Builds a configuration whose allowlist is the given comma-separated value.
     */
    private GlueSchemaRegistryConfiguration configWithAllowlist(String allowlist) {
        Properties props = createTestProperties();
        props.put(AWSSchemaRegistryConstants.JSON_CLASS_NAME_ALLOWLIST, allowlist);
        return new GlueSchemaRegistryConfiguration(props);
    }

    /**
     * Tests that an exact allowlist entry allows that class and nothing else.
     */
    @Test
    public void testIsClassNameAllowed_exactEntry_matchesOnlyThatClass() {
        GlueSchemaRegistryConfiguration config = configWithAllowlist("com.example.pojos.Car");

        assertTrue(config.isClassNameAllowed("com.example.pojos.Car"));
        assertFalse(config.isClassNameAllowed("com.example.pojos.Truck"));
    }

    /**
     * Tests that a package entry allows classes directly in that package.
     */
    @Test
    public void testIsClassNameAllowed_packageEntry_matchesDirectMembers() {
        GlueSchemaRegistryConfiguration config = configWithAllowlist("com.example.pojos.*");

        assertTrue(config.isClassNameAllowed("com.example.pojos.Car"));
        assertTrue(config.isClassNameAllowed("com.example.pojos.Truck"));
    }

    /**
     * Tests that a package entry does not reach into sub-packages. A nested package is a
     * separate decision from the one the operator made.
     */
    @Test
    public void testIsClassNameAllowed_packageEntry_doesNotMatchSubPackages() {
        GlueSchemaRegistryConfiguration config = configWithAllowlist("com.example.pojos.*");

        assertFalse(config.isClassNameAllowed("com.example.pojos.nested.Car"));
    }

    /**
     * Tests that the trailing dot is part of the prefix, so a package entry cannot match a
     * sibling package that merely starts with the same characters.
     */
    @Test
    public void testIsClassNameAllowed_packageEntry_doesNotMatchPrefixSiblingPackage() {
        GlueSchemaRegistryConfiguration config = configWithAllowlist("com.example.pojos.*");

        assertFalse(config.isClassNameAllowed("com.example.pojosX.Car"));
        assertFalse(config.isClassNameAllowed("com.example.pojos"));
    }

    /**
     * Tests that a nested class of an allowed package matches, since it is declared inside a
     * class the entry already allows.
     */
    @Test
    public void testIsClassNameAllowed_packageEntry_matchesNestedClass() {
        GlueSchemaRegistryConfiguration config = configWithAllowlist("com.example.pojos.*");

        assertTrue(config.isClassNameAllowed("com.example.pojos.Car$Engine"));
    }

    /**
     * Tests that entries are matched literally rather than as regular expressions, so regex
     * metacharacters in an entry do not widen what it matches.
     */
    @Test
    public void testIsClassNameAllowed_entryIsNotTreatedAsRegex() {
        GlueSchemaRegistryConfiguration config = configWithAllowlist("com.example.pojos.Ca.");

        assertFalse(config.isClassNameAllowed("com.example.pojos.Car"));
    }

    /**
     * Tests that exact and package entries coexist in one allowlist.
     */
    @Test
    public void testIsClassNameAllowed_mixedEntries_bothKindsMatch() {
        GlueSchemaRegistryConfiguration config =
                configWithAllowlist("com.example.Legacy, com.example.pojos.*");

        assertTrue(config.isClassNameAllowed("com.example.Legacy"));
        assertTrue(config.isClassNameAllowed("com.example.pojos.Car"));
        assertFalse(config.isClassNameAllowed("com.example.other.Car"));
    }

    /**
     * Tests that the default empty allowlist permits nothing, and that a null class name is
     * rejected rather than throwing.
     */
    @Test
    public void testIsClassNameAllowed_emptyAllowlistAndNull_areRejected() {
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(createTestProperties());

        assertFalse(config.isClassNameAllowed("com.example.pojos.Car"));
        assertFalse(config.isClassNameAllowed(null));
    }

    /**
     * Tests that a bare wildcard is rejected. Allowing every class on the classpath is the
     * behavior the allowlist exists to prevent, so it must not be reachable in one character.
     */
    @Test
    public void testJsonClassNameAllowlist_bareWildcard_throwsException() {
        assertThrows(AWSSchemaRegistryException.class, () -> configWithAllowlist("*"));
        assertThrows(AWSSchemaRegistryException.class, () -> configWithAllowlist(".*"));
        assertThrows(AWSSchemaRegistryException.class,
                     () -> configWithAllowlist("com.example.pojos.Car,*"));
        // Whitespace must not smuggle a bare wildcard past the check.
        assertThrows(AWSSchemaRegistryException.class, () -> configWithAllowlist(" * "));
        assertThrows(AWSSchemaRegistryException.class, () -> configWithAllowlist("com.example.Car, .* "));
    }

    /**
     * Tests that wildcard-looking entries which are not a bare wildcard match nothing rather than
     * matching broadly. They are treated as literal class names, so they fail closed.
     */
    @Test
    public void testIsClassNameAllowed_malformedWildcards_matchNothing() {
        assertFalse(configWithAllowlist("**").isClassNameAllowed("com.example.Car"));
        assertFalse(configWithAllowlist("com.example.*.Car").isClassNameAllowed("com.example.pojos.Car"));
        assertFalse(configWithAllowlist("com.example*").isClassNameAllowed("com.example.Car"));
        assertFalse(configWithAllowlist("com.example.pojos*").isClassNameAllowed("com.example.pojos.Car"));
    }

    /**
     * Tests that even a top-level package entry stays narrow, since only direct members match.
     * {@code "com.*"} therefore cannot stand in for a bare wildcard.
     */
    @Test
    public void testIsClassNameAllowed_topLevelPackageEntry_staysNarrow() {
        GlueSchemaRegistryConfiguration config = configWithAllowlist("com.*");

        assertTrue(config.isClassNameAllowed("com.Car"));
        assertFalse(config.isClassNameAllowed("com.example.Car"));
        assertFalse(config.isClassNameAllowed("com.example.pojos.Car"));
    }

    /**
     * Tests that a bare wildcard installed through the generated setter matches nothing. The
     * parsing-time rejection does not cover this path, so the rule has to hold at the point of use
     * as well.
     */
    @Test
    public void testIsClassNameAllowed_bareWildcardViaSetter_matchesNothing() {
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(createTestProperties());

        config.setJsonClassNameAllowlist(new HashSet<>(Arrays.asList("*")));
        assertFalse(config.isClassNameAllowed("com.example.Car"));

        config.setJsonClassNameAllowlist(new HashSet<>(Arrays.asList(".*")));
        assertFalse(config.isClassNameAllowed("com.example.Car"));

        // A scoped package entry set the same way still works, so the guard is not over-broad.
        config.setJsonClassNameAllowlist(new HashSet<>(Arrays.asList("com.example.*")));
        assertTrue(config.isClassNameAllowed("com.example.Car"));
    }
}
