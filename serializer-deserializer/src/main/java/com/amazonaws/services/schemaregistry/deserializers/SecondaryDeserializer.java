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
package com.amazonaws.services.schemaregistry.deserializers;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

@Slf4j
public class SecondaryDeserializer {
    private Class<?> clz;
    private Object obj;

    private SecondaryDeserializer() {}

    public static SecondaryDeserializer newInstance() {
        return new SecondaryDeserializer();
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        try {
            Method secConfigure = this.clz.getMethod("configure", Map.class, boolean.class);
            secConfigure.invoke(this.obj, configs, isKey);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            String message = "Can't find method called configure or invoke it.";
            throw new AWSSchemaRegistryException(message, e);
        }

    }

    /**
     * Reports whether the configured class is a Kafka {@link Deserializer}, checking it before it
     * is initialized or constructed.
     *
     * @param configs property map carrying {@code secondaryDeserializer}
     * @return {@code true} if the configured class is a Kafka {@link Deserializer}
     * @throws AWSSchemaRegistryException if the class cannot be found or cannot be instantiated
     */
    public boolean validate(Map<String, ?> configs) {
        Object className = configs.get(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER);
        String secDeserializer = String.valueOf(className);

        final Class<?> candidate;
        try {
            // initialize=false defers the static initializer until the check below passes.
            // Same classloader the one-argument Class.forName used.
            candidate = Class.forName(secDeserializer, false, SecondaryDeserializer.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            String message = "Can't find the class or instantiate it.";
            throw new AWSSchemaRegistryException(message, e);
        }

        // isAssignableFrom, not getInterfaces(): the latter misses an interface inherited
        // from a superclass.
        if (!Deserializer.class.isAssignableFrom(candidate)) {
            return false;
        }

        try {
            this.clz = candidate;
            this.obj = candidate.newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            String message = "Can't find the class or instantiate it.";
            throw new AWSSchemaRegistryException(message, e);
        }

        return true;
    }

    public Object deserialize(String topic, byte[] data) {
        if (this.obj == null) {
            throw new AWSSchemaRegistryException("Didn't find secondary deserializer.");
        }

        try {
            Method secDeserialize = this.clz.getMethod("deserialize", String.class, byte[].class);
            return secDeserialize.invoke(this.obj, topic, data);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            String message = "Can't find method called deserialize or invoke it.";
            throw new AWSSchemaRegistryException(message, e);
        }

    }

    public void close() {
        if (this.obj == null) {
            throw new AWSSchemaRegistryException("Didn't find secondary deserializer.");
        }

        try {
            Method close = this.clz.getMethod("close");
            close.invoke(this.obj);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            String message = "Can't find method called close or invoke it.";
            throw new AWSSchemaRegistryException(message, e);
        }
    }

}
