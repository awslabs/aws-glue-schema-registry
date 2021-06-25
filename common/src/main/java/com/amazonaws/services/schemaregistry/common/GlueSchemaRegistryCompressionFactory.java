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

package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;

/**
 * Factory to create the compression object.
 */
public class GlueSchemaRegistryCompressionFactory {

    private GlueSchemaRegistryDefaultCompression zlibCompression;

    public GlueSchemaRegistryCompressionFactory() {
    }

    /**
     * Get the respective compression handler based on the properties.
     *
     * @param compressionType compression algorithm to be used
     * @return GlueSchemaRegistryCompressionHandler {@link GlueSchemaRegistryCompressionHandler}
     */
    public GlueSchemaRegistryCompressionHandler getCompressionHandler(AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        if (compressionType != null && AWSSchemaRegistryConstants.COMPRESSION.ZLIB.name()
              .equalsIgnoreCase(compressionType.name())) {
            return getZlibCompression();
        }
        return null;
    }

    /**
     * return the compression handler based on the byte. different byte can
     * different compression algorithm implementation.
     *
     * @param compressionByte
     * @return GlueSchemaRegistryCompressionHandler {@link GlueSchemaRegistryCompressionHandler}
     */
    public GlueSchemaRegistryCompressionHandler getCompressionHandler(byte compressionByte) {
        if (AWSSchemaRegistryConstants.COMPRESSION_BYTE == compressionByte) {
            return getZlibCompression();
        }

        return null;
    }

    private synchronized GlueSchemaRegistryCompressionHandler getZlibCompression() {
        if (zlibCompression == null) {
            zlibCompression = new GlueSchemaRegistryDefaultCompression();
        }

        return zlibCompression;
    }

}
