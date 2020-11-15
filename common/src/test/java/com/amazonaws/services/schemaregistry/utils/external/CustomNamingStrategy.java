package com.amazonaws.services.schemaregistry.utils.external;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;

public class CustomNamingStrategy implements AWSSchemaNamingStrategy {

    /**
     * Returns the schemaName.
     *
     * @param transportName topic Name or stream name etc.
     * @return schema name.
     */
    @Override
    public String getSchemaName(String transportName) {
        return transportName;
    }
}