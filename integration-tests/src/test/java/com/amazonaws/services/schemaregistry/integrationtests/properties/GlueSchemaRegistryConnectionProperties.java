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
package com.amazonaws.services.schemaregistry.integrationtests.properties;
import com.amazonaws.regions.Regions;

public interface GlueSchemaRegistryConnectionProperties {
    // Glue Service Endpoint
    String REGION = Regions.getCurrentRegion() == null ? "us-east-2" : Regions.getCurrentRegion().getName().toLowerCase();
    String ENDPOINT = String.format("https://glue.%s.amazonaws.com", REGION);
    String SRC_REGION = Regions.getCurrentRegion() == null ? "us-east-1" : Regions.getCurrentRegion().getName().toLowerCase();
    String SRC_ENDPOINT = String.format("https://glue.%s.amazonaws.com", SRC_REGION);
    String DEST_REGION = "us-east-2";
    String DEST_ENDPOINT = String.format("https://glue.%s.amazonaws.com", DEST_REGION);
}
