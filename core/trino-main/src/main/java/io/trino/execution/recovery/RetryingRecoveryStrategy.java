/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.execution.recovery;

import io.trino.spi.ErrorCode;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static io.trino.execution.recovery.RetryableErrorClassifier.isRetryable;
import static java.util.Objects.requireNonNull;

public class RetryingRecoveryStrategy
        implements FailureRecoveryStrategy
{
    private final RecoveryAction recoveryAction;
    private final BooleanSupplier queryDone;
    private final BackoffPolicy backoffPolicy;
    private final AtomicInteger retries = new AtomicInteger();

    public RetryingRecoveryStrategy(RecoveryAction recoveryAction, BooleanSupplier queryDone, BackoffPolicy backoffPolicy)
    {
        this.recoveryAction = requireNonNull(recoveryAction, "recoveryAction is null");
        this.queryDone = requireNonNull(queryDone, "queryDone is null");
        this.backoffPolicy = requireNonNull(backoffPolicy, "backoffPolicy is null");
    }

    @Override
    public boolean handleFailure(Throwable failure, ErrorCode errorCode, Consumer<Throwable> failQuery)
    {
        if (queryDone.getAsBoolean() || !isRetryable(errorCode)) {
            failQuery.accept(failure);
            return true;
        }
        int retry = retries.getAndIncrement();
        if (retry >= backoffPolicy.maxRetries()) {
            return false; // caller handles
        }
        recoveryAction.run(failure, backoffPolicy.delayFor(retry));
        return true;
    }
}
