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

/**
 * Defines the schema name to be used, while registering schema.
 * An implementation of this interface can be provided and passed via property.
 */
public interface AWSSchemaNamingStrategy {
    default String getSchemaName(String transportName,
                                 Object data) {
        return getSchemaName(transportName);
    }

    default String getSchemaName(String transportName,
                                 Object data,
                                 boolean isKey) {
        return isKey ? getSchemaName(transportName) : getSchemaName(transportName, data);
    }

    /**
     * Returns the schemaName.
     *
     * @param transportName topic Name or stream name etc.
     * @return schema name.
     */
    String getSchemaName(String transportName);
}
