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
package com.amazonaws.services.schemaregistry.deserializers.avro;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Records whether {@code vofcanary.Plain}'s static initializer has run and whether it has been
 * constructed.
 * <p>
 * This lives outside {@code vofcanary.Plain} on purpose. Reading a static member of the probe
 * itself would initialize the probe, so a flag declared inside it would be tripped by the act of
 * checking it and could never distinguish a class the deserializer initialized from one the
 * assertion initialized.
 * <p>
 * The two signals are kept separate because the Avro sink reaches further than a class load:
 * {@code newInstance()} both initializes and constructs, so a regression could reappear as
 * either one.
 */
public final class SpecificRecordProbeFlag {
    private static final AtomicBoolean STATIC_INIT_RAN = new AtomicBoolean(false);
    private static final AtomicBoolean CONSTRUCTED = new AtomicBoolean(false);

    private SpecificRecordProbeFlag() {
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
