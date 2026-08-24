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

import java.net.SocketException;
import java.sql.Connection;
import java.sql.SQLException;

import static java.time.temporal.ChronoUnit.SECONDS;

/// Creates JDBC connections with retries for transient socket failures.
public final class ConnectionRetry
{
    private static final Logger log = Logger.get(ConnectionRetry.class);

    private static final RetryPolicy<Object> RETRY_POLICY = RetryPolicy.builder()
            .handleIf(failure -> Throwables.getRootCause(failure) instanceof SocketException)
            .withBackoff(1, 10, SECONDS)
            .withMaxRetries(30)
            .onRetry(event -> log.warn(event.getLastException(), "Connection failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private ConnectionRetry() {}

    public static Connection createWithRetry(ConnectionFactory connectionFactory)
            throws SQLException
    {
        try {
            return Failsafe.with(RETRY_POLICY).get(connectionFactory::create);
        }
        catch (FailsafeException failure) {
            if (failure.getCause() instanceof SQLException sqlException) {
                throw sqlException;
            }
            throw failure;
        }
    }

    @FunctionalInterface
    public interface ConnectionFactory
    {
        Connection create()
                throws SQLException;
    }
}
