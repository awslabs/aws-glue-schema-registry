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

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;

@Slf4j
public final class AVROUtils {
    private AVROUtils() {
    }

    private static class UtilsHelper {
        private static final AVROUtils INSTANCE = new AVROUtils();
    }

    /**
     * Thread safe singleton instance of the AVROUtil Class.
     *
     * @return Avro util instance. {@link AVROUtils}.
     */
    public static synchronized AVROUtils getInstance() {
        return UtilsHelper.INSTANCE;
    }

    /**
     * Get the schema definition.
     *
     * @param object object for which schema definition has to be derived
     * @return schema string
     */
    public String getSchemaDefinition(@NonNull Object object) {
        Schema schema = getSchema(object);

        if (null == schema) {
            String message = "Unsupported Type of Record received";
            log.error(message);
            throw new AWSSchemaRegistryException(message);
        }

        return schema.toString();
    }

    /**
     * Returns the schema Object.
     *
     * @param object object is given by the Kafka.
     * @return schema Object {@link Schema}.
     */
    public Schema getSchema(@NonNull Object object) {
        if (object instanceof GenericContainer) {
            // GenericContainer should contain data of all types except those primitive types
            return ((GenericContainer) object).getSchema();
        }
        log.error("Unsupported Avro Data Formats");
        return null;
    }

}
