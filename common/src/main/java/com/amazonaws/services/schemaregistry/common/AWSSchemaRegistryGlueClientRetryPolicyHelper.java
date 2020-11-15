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

import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;

import java.time.Duration;

import com.google.common.collect.ImmutableSet;

public class AWSSchemaRegistryGlueClientRetryPolicyHelper {

    private static final Duration BASE_DELAY = Duration.ofMillis(200);
    private static final Duration MAX_BACKOFF = Duration.ofMillis(20000);
    private static final int MAX_RETRIES = 3;

    public static RetryPolicy getRetryPolicy() {
        return RetryPolicy.defaultRetryPolicy()
                .toBuilder()
                .backoffStrategy(EqualJitterBackoffStrategy.builder()
                                         .baseDelay(BASE_DELAY)
                                         .maxBackoffTime(MAX_BACKOFF)
                                         .build())
                .numRetries(MAX_RETRIES)
                .retryCondition(getRetryCondition())
                .build();
    }

    private static RetryCondition getRetryCondition() {
        return OrRetryCondition.create(RetryCondition.defaultRetryCondition(),
                                       RetryOnExceptionsCondition.create(ImmutableSet.of(ConcurrentModificationException.class)));
    }
}
