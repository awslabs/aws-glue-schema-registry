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

package com.amazonaws.services.schemaregistry.exception;

import lombok.Getter;

import java.util.UUID;

/**
 * Wraps all exceptions and throws to the caller.
 */
public class AWSSchemaRegistryException extends RuntimeException {

    /**
     * Serial Version ID.
     */
    private static final long serialVersionUID = -2898445676112111319L;

    @Getter
    private UUID schemaVersionId;

    /**
     * Constructs a new runtime exception with {@code null} as its detail message.
     */
    public AWSSchemaRegistryException() {
        super();
    }

     /**
     * Constructs a new runtime exception with {@code null} as its detail message.
     *
     * @param cause    original exception
     * @param schemaVersionId schema version id for context
     */
    public AWSSchemaRegistryException(Throwable cause, UUID schemaVersionId) {
        super(cause);
        this.schemaVersionId = schemaVersionId;
    }

    /**
     * Constructs a new runtime exception with the specified detail message.
     *
     * @param message the detail message.
     */
    public AWSSchemaRegistryException(String message) {
        super(message);
    }

    /**
     * Constructs a new runtime exception with the specified detail message and
     * cause.
     *
     * @param message the detail message.
     * @param cause   the cause.
     */
    public AWSSchemaRegistryException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new runtime exception with the specified cause and a detail
     * message of cause.
     *
     * @param cause the cause
     */
    public AWSSchemaRegistryException(Throwable cause) {
        super(cause);
    }
}
