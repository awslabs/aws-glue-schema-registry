/*
 * Copyright 2021 Red Hat
 * Portions Copyright 2020 Amazon.com, Inc. or its affiliates.
 * All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This will be removed once Apicurio releases the latest version with the json_name fix
 * https://github.com/Apicurio/apicurio-registry/blob/master/utils/protobuf-schema-utilities/src/test/java/io/apicurio/registry/utils/protobuf/schema/ProtobufTestCaseReader.java
 */

package com.amazonaws.services.schemaregistry.utils.apicurio;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ProtobufTestCaseReader {
    private static final String TEST_PROTO_PATH = "src/test/proto";

    public static String getRawSchema(String fileName) {
        try {
            return new String(Files.readAllBytes(Paths.get(TEST_PROTO_PATH, fileName)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        }
    }
}
