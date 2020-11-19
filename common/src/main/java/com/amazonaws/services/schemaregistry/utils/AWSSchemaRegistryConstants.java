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

package com.amazonaws.services.schemaregistry.utils;

import software.amazon.awssdk.services.glue.model.Compatibility;

public final class AWSSchemaRegistryConstants {
    /**
     * AWS endpoint to use while initializing the client for service.
     */
    public static final String AWS_ENDPOINT = "endpoint";
    /**
     * AWS region to use while initializing the client for service.
     */
    public static final String AWS_REGION = "region";
    /**
     * Header Version Byte.
     */
    public static final byte HEADER_VERSION_BYTE = (byte) 3;
    /**
     * Check for compression.
     */
    public static final String COMPRESSION_TYPE = "compression";
    /**
     * Compression byte.
     */
    public static final byte COMPRESSION_BYTE = (byte) 5;
    /**
     * Compression byte default when compression is not enabled.
     */
    public static final byte COMPRESSION_DEFAULT_BYTE = (byte) 0;
    /**
     * Header Version byte.
     */
    public static final int HEADER_VERSION_BYTE_SIZE = 1;
    /**
     * Compression byte.
     */
    public static final int COMPRESSION_BYTE_SIZE = 1;

    /**
     * * A buffer is allocated for the serialized message. A header of 18 bytes is
     * written. * Byte 0 is an 8 bit version number * Byte 1 is the compression *
     * Byte 2-17 is a 128 bit UUID representing the schema-version-id
     */
    /**
     * Schema Byte Size.
     */
    public static final int SCHEMA_VERSION_ID_SIZE = 16;
    /**
     * Schema name.
     */
    public static final String SCHEMA_NAME = "schemaName";
    /**
     * Schema Generation Class.
     */
    public static final String SCHEMA_NAMING_GENERATION_CLASS = "schemaNameGenerationClass";
    /**
     * Default schema strategy class.
     */
    public static final String DEFAULT_SCHEMA_STRATEGY = "AWSSchemaNamingStrategyDefaultImpl";
    /**
     * Cache time to live.
     */
    public static final String CACHE_TIME_TO_LIVE_MILLIS = "timeToLiveMillis";
    /**
     * Cache Size.
     */
    public static final String CACHE_SIZE = "cacheSize";
    /**
     * AVRO record type.
     */
    public static final String AVRO_RECORD_TYPE = "avroRecordType";
    /**
     * Registry Name.
     */
    public static final String REGISTRY_NAME = "registry.name";
    /**
     * Default registry name if not passed by the client.
     */
    public static final String DEFAULT_REGISTRY_NAME = "default-registry";
    /**
     * Compatibility setting, will be helpful at the time of schema evolution.
     */
    public static final String COMPATIBILITY_SETTING = "compatibility";
    /**
     * Description about the process.
     */
    public static final String DESCRIPTION = "description";
    /**
     * Default Compatibility setting if not passed by the client.
     */
    public static final Compatibility DEFAULT_COMPATIBILITY_SETTING = Compatibility.BACKWARD;
    /**
     * Secondary deserializer class name, which will be initialized by the
     * deserializer, if client passes and will be used if magic bytes does not
     * belong to AWS.
     */
    public static final String SECONDARY_DESERIALIZER = "secondaryDeserializer";
    /**
     * Schema version Id not found.
     */
    public static final String SCHEMA_VERSION_NOT_FOUND_MSG = "Schema version is not found.";
    /**
     * Tags config value is not a instance of HashMap
     */
    public static final String TAGS_CONFIG_NOT_HASHMAP_MSG = "The tag config is not a instance of HashMap.";
    /**
     * Schema not found.
     */
    public static final String SCHEMA_NOT_FOUND_MSG = "Schema is not found.";
    /**
     * Auto registration is disabled
     */
    public static final String AUTO_REGISTRATION_IS_DISABLED_MSG =
            "Failed to auto-register schema. Auto registration of schema is not enabled.";
    /**
     * Config to allow auto registrations of Schema.
     */
    public static final String SCHEMA_AUTO_REGISTRATION_SETTING = "schemaAutoRegistrationEnabled";
    /**
     * Tags for schema and registry.
     */
    public static final String TAGS = "tags";
    /**
     * Metadata.
     */
    public static final String METADATA = "metadata";
    /**
     * Default transport name metadata key.
     */
    public static final String TRANSPORT_METADATA_KEY = "x-amz-meta-transport";
    /**
     * Private constructor to avoid initialization of the class.
     */

    private AWSSchemaRegistryConstants() {

    }

    /**
     * Enums for the back end job, responsible for fetching schema version Id corresponding
     * to task id.
     */
    public enum SchemaVersionStatus {
        /**
         * Job successfully completed.
         */
        AVAILABLE,
        /**
         * Job still running.
         */
        PENDING,
        /**
         * Schema evolution failed
         */
        FAILURE,
        /**
         * Job still running.
         */
        DELETING
    }

    public enum COMPRESSION {
        /**
         * default no compression.
         */
        NONE,
        /**
         * ZLIB compression.
         */
        ZLIB
    }
}
