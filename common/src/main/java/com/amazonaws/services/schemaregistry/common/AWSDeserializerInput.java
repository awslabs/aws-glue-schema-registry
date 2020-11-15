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

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

import java.nio.ByteBuffer;

/**
 * Encapsulates general inputs for deserializer
 */
@ToString
@EqualsAndHashCode
public class AWSDeserializerInput {
    @Getter
    private ByteBuffer buffer;

    @Getter
    private String transportName;

    @Builder
    public AWSDeserializerInput(@NonNull ByteBuffer buffer, String transportName) {
        this.buffer = buffer;
        if (transportName != null) {
            this.transportName = transportName;
        } else {
            this.transportName = "default-stream";
        }
    }

}
