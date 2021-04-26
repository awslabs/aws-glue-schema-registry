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

package com.amazonaws.services.schemaregistry.serializers.avro;

import com.amazonaws.services.schemaregistry.common.AWSSchemaNamingStrategy;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import org.apache.avro.Schema;

public class CustomerProvidedSchemaNamingStrategy implements AWSSchemaNamingStrategy {
    @Override
    public String getSchemaName(String p0) {
        return p0;
    }

    @Override
    public String getSchemaName(String transportName,
                                Object data) {
        Schema schema = AVROUtils.getInstance()
                .getSchema(data);
        return String.format("%s-%s", transportName, schema.getName());
    }

    @Override
    public String getSchemaName(String transportName,
                                Object data,
                                boolean isKey) {
        Schema schema = AVROUtils.getInstance()
                .getSchema(data);
        return String.format("%s-%s-%s", transportName, schema.getName(), isKey ? "key" : "value");
    }
}
