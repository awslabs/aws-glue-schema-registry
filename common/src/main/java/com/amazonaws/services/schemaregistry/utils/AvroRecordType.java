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

import org.apache.commons.lang3.StringUtils;

/**
 * Defines a set of supported Data Formats
 */
public enum AvroRecordType {
    /**
     * Unknown.
     */
    UNKNOWN("UNKNOWN", 0),

    /**
     * Specific record type.
     */
    SPECIFIC_RECORD("SPECIFIC_RECORD", 1),

    /**
     * Generic record type.
     */
    GENERIC_RECORD("GENERIC_RECORD", 2);

    /**
     * Name of the Data Format.
     */
    private final String name;

    /**
     * Integer value of the Data Format.
     */
    private final int value;

    /**
     * Returns the name for this Data Format.
     *
     * @return Returns the name for this Data Format.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the value for this Data Format.
     *
     * @return Returns the value for this Data Format.
     */
    public int getValue() {
        return value;
    }

    /**
     * Creates Data Format with given name and value.
     *
     * @param name  Data Format name
     * @param value Data Format value
     */
    AvroRecordType(String name, int value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Returns data format associated with the given name.
     *
     * @param name Name of the data format.
     * @return Returns data format associated with the given name.
     */
    public static AvroRecordType fromName(String name) {
        if (!StringUtils.isEmpty(name)) {
            name = name.toUpperCase();
        }
        return valueOf(name);
    }
}
