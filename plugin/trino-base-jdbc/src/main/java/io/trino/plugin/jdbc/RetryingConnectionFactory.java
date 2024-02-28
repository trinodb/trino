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
package io.trino.plugin.jdbc;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.trino.plugin.jdbc.jmx.StatisticsAwareConnectionFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Set;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public class RetryingConnectionFactory
        implements ConnectionFactory
{
    private final RetryPolicy<Object> retryPolicy;

    private final ConnectionFactory delegate;

    @Inject
    public RetryingConnectionFactory(StatisticsAwareConnectionFactory delegate, Set<RetryStrategy> retryStrategies)
    {
        requireNonNull(retryStrategies);
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.retryPolicy = RetryPolicy.builder()
                .withMaxDuration(java.time.Duration.of(30, SECONDS))
                .withMaxAttempts(5)
                .withBackoff(50, 5_000, MILLIS, 4)
                .handleIf(throwable -> isExceptionRecoverable(retryStrategies, throwable))
                .abortOn(TrinoException.class)
                .build();
    }

    private static boolean isExceptionRecoverable(Set<RetryStrategy> retryStrategies, Throwable throwable)
    {
        return retryStrategies.stream()
                .anyMatch(retryStrategy -> retryStrategy.isExceptionRecoverable(throwable));
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        try {
            return Failsafe.with(retryPolicy)
                    .get(() -> delegate.openConnection(session));
        }
        catch (FailsafeException ex) {
            if (ex.getCause() instanceof SQLException) {
                throw (SQLException) ex.getCause();
            }
            throw ex;
        }
    }

    @Override
    public void close()
            throws SQLException
    {
        delegate.close();
    }

    public interface RetryStrategy
    {
        boolean isExceptionRecoverable(Throwable exception);
    }

    public static class DefaultRetryStrategy
            implements RetryStrategy
    {
        @Override
        public boolean isExceptionRecoverable(Throwable exception)
        {
            return Throwables.getCausalChain(exception).stream()
                    .anyMatch(SQLTransientException.class::isInstance);
        }
    }
}
