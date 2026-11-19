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
package vofcanary;

import com.amazonaws.services.schemaregistry.deserializers.avro.SpecificRecordProbeFlag;

/**
 * Stands in for a gadget class in the SPECIFIC_RECORD tests: a class on the consumer's classpath
 * that is <em>not</em> an {@code org.apache.avro.specific.SpecificRecord} and that does something
 * observable when it is initialized and when it is constructed.
 * <p>
 * The package and class name are deliberate. {@code SpecificData.getClassName(Schema)} maps a
 * schema's namespace and name straight onto a Java class name, so a writer schema of
 * {@code {"type":"record","name":"Plain","namespace":"vofcanary","fields":[]}} resolves here. That
 * schema is registry content, so a schema author picks the class.
 * <p>
 * Nothing in the library should ever initialize or construct this class. A test asserting
 * {@link SpecificRecordProbeFlag#staticInitRan()} and
 * {@link SpecificRecordProbeFlag#constructed()} are both false is asserting exactly that.
 * <p>
 * The no-argument constructor is public because that is what makes a class reachable through
 * {@code Class.newInstance()}, which is the shape this test is about.
 */
public class Plain {
    static {
        SpecificRecordProbeFlag.tripStaticInit();
    }

    public Plain() {
        SpecificRecordProbeFlag.tripConstructor();
    }
}
