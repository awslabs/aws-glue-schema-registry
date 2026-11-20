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
package com.amazonaws.services.schemaregistry.deserializers.external;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Records whether {@link NotKafkaDeserializerProbe} ran its static initializer or its
 * constructor. Separate from the probe because reading a static member of the probe would
 * initialize it, tripping the flag being read. {@code newInstance()} does both, so the two
 * signals are tracked separately.
 */
public final class SecondaryDeserializerProbeFlag {
    private static final AtomicBoolean STATIC_INIT_RAN = new AtomicBoolean(false);
    private static final AtomicBoolean CONSTRUCTED = new AtomicBoolean(false);

    private SecondaryDeserializerProbeFlag() {
    }

    public static void tripStaticInit() {
        STATIC_INIT_RAN.set(true);
    }

    public static void tripConstructor() {
        CONSTRUCTED.set(true);
    }

    public static boolean staticInitRan() {
        return STATIC_INIT_RAN.get();
    }

    public static boolean constructed() {
        return CONSTRUCTED.get();
    }
}
