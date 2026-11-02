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
package io.trino.testing.containers.environment;

import com.google.common.base.Throwables;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;

import java.sql.SQLException;

import static java.time.temporal.ChronoUnit.SECONDS;

/// Executes JDBC operations with retries for known transient product-test failures.
public final class QueryRetry
{
    private static final Logger log = Logger.get(QueryRetry.class);

    private static final RetryPolicy<Object> RETRY_POLICY = RetryPolicy.builder()
            .handleIf(QueryRetry::isRetryableFailure)
            .withBackoff(1, 10, SECONDS)
            .withMaxRetries(30)
            .onRetry(event -> log.warn(event.getLastException(), "Query failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private QueryRetry() {}

    public static <T> T executeWithRetry(SqlOperation<T> operation)
            throws SQLException
    {
        try {
            return Failsafe.with(RETRY_POLICY).get(operation::execute);
        }
        catch (FailsafeException failure) {
            if (failure.getCause() instanceof SQLException sqlException) {
                throw sqlException;
            }
            throw failure;
        }
    }

    private static boolean isRetryableFailure(Throwable failure)
    {
        String value = Throwables.getStackTraceAsString(failure);
        return value.contains("could only be replicated to 0 nodes instead of minReplication") ||
                value.contains("could only be written to 0 of the 1 minReplication");
    }

    @FunctionalInterface
    public interface SqlOperation<T>
    {
        T execute()
                throws SQLException;
    }
}
