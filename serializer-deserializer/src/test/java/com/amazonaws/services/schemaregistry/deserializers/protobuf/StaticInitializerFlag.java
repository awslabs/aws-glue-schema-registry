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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Records whether {@link StaticInitializerProbe}'s static initializer has run.
 * <p>
 * This lives in a separate class on purpose. Reading a static member of the probe itself
 * would initialize the probe, so a test that asserted on a flag declared inside the probe
 * would trip that flag by the act of checking it and could never distinguish a class that
 * was initialized by the deserializer from one initialized by the assertion.
 */
public final class StaticInitializerFlag {
    private static final AtomicBoolean TRIPPED = new AtomicBoolean(false);

    private StaticInitializerFlag() {
    }

    static void trip() {
        TRIPPED.set(true);
    }

    public static boolean isTripped() {
        return TRIPPED.get();
    }
}
