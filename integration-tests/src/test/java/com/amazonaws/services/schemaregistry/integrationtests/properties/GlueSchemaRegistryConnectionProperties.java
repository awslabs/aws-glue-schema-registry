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

import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.ServiceMetadata;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;

public interface GlueSchemaRegistryConnectionProperties {
    // Glue Service Endpoint. Derive the host from the region's partition so non-standard
    // partitions (e.g. cn-north-1, us-gov-west-1) resolve to the correct endpoint suffix.
    String REGION = resolveRegion();
    String ENDPOINT = "https://" + ServiceMetadata.of("glue").endpointFor(Region.of(REGION));

    static String resolveRegion() {
        try {
            return new DefaultAwsRegionProviderChain().getRegion().id();
        } catch (SdkClientException e) {
            return "us-east-2";
        }
    }
}
