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
package com.amazonaws.services.schemaregistry.deserializers.protobuf;

/**
 * Stands in for a gadget class in the POJO-mode deserialization tests: a class that is on the
 * consumer's classpath, is not a {@code com.google.protobuf.Message}, and does something
 * observable when it is initialized.
 * <p>
 * A Protobuf schema can name any class through its {@code java_package} option and message
 * name, so a decoder that initializes the resolved class before checking it hands a schema
 * author the ability to run this static initializer. Nothing in the library should ever
 * initialize this class; a test asserting {@link StaticInitializerFlag#isTripped()} is false
 * is asserting exactly that.
 */
public final class StaticInitializerProbe {
    static {
        StaticInitializerFlag.trip();
    }

    private StaticInitializerProbe() {
    }
}
