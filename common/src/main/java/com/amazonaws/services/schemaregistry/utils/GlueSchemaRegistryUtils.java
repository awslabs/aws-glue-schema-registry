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

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategyDefaultImpl;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;

import java.util.Map;


@Slf4j
public final class GlueSchemaRegistryUtils {

    private GlueSchemaRegistryUtils() {
    }

    /**
     * Singleton helper.
     */
    private static class UtilsHelper {
        private static final GlueSchemaRegistryUtils INSTANCE = new GlueSchemaRegistryUtils();
    }

    /**
     * Thread safe singleton instance of the GlueSchemaRegistryUtils Class.
     *
     * @return GlueSchemaRegistryUtils instance. {@link GlueSchemaRegistryUtils}.
     */

    public static GlueSchemaRegistryUtils getInstance() {
        return UtilsHelper.INSTANCE;
    }

    /**
     * Check value is present in the map or not.
     *
     * @param map map of configuration elements
     * @param key key for checking the presence of entry in configuration map
     * @return true or false, if the map is empty or not.
     */
    public boolean checkIfPresentInMap(@NonNull Map<String, ?> map, @NonNull String key) {
        Validate.notEmpty(map);
        return map.containsKey(key);
    }

    public AWSSchemaNamingStrategy configureSchemaNamingStrategy(Map<String, ?> configs) {
        return isSchemaGenerationClassPresent(configs) ?
               useCustomerProvidedStrategy(String.valueOf(configs.get(AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS))) : useDefaultStrategy();
    }

    /**
     * Returns the schema Name.
     *
     * @param configs map of configuration elements
     * @return schema Name.
     */
    public String getSchemaName(Map<String, ?> configs) {
        return checkIfPresentInMap(configs, AWSSchemaRegistryConstants.SCHEMA_NAME) ?
               String.valueOf(configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME)) : null;
    }

    /**
     * Returns the data format.
     *
     * @param configs map of configuration elements
     * @return data format.
     */
    public String getDataFormat(Map<String, ?> configs) {
        if (checkIfPresentInMap(configs, AWSSchemaRegistryConstants.DATA_FORMAT)) {
            return String.valueOf(configs.get(AWSSchemaRegistryConstants.DATA_FORMAT)).toUpperCase();
        } else {
            String message =
                    String.format("Unable to find configuration for dataFormat");
            throw new AWSSchemaRegistryException(message);
        }
    }

    /**
     * Instantiates classes provided in the kafka properties.
     *
     * @param className class to initialize. {@link String}.
     * @return Schema Strategy Class {@link AWSSchemaNamingStrategy}
     */
    private AWSSchemaNamingStrategy initializeStrategy(@NonNull String className) {
        AWSSchemaNamingStrategy schemaNameStrategy = null;
        try {
            Object newInstance = Class.forName(className).newInstance();
            if (newInstance instanceof AWSSchemaNamingStrategy) {
                schemaNameStrategy = (AWSSchemaNamingStrategy) newInstance;
            }
        } catch (Exception e) {
            String message = String.format(
                    "Unable to locate the naming strategy class, check in the classpath for classname = %s", className);
            log.error(message, AWSSchemaRegistryConstants.DEFAULT_SCHEMA_STRATEGY);
            throw new AWSSchemaRegistryException(message, e);
        }

        return schemaNameStrategy;
    }

    private AWSSchemaNamingStrategy useDefaultStrategy() {
        return new AWSSchemaNamingStrategyDefaultImpl();
    }

    private AWSSchemaNamingStrategy useCustomerProvidedStrategy(@NonNull String className) {
        return initializeStrategy(className);
    }

    private boolean isSchemaGenerationClassPresent(Map<String, ?> configs) {
        return checkIfPresentInMap(configs, AWSSchemaRegistryConstants.SCHEMA_NAMING_GENERATION_CLASS);
    }

}
