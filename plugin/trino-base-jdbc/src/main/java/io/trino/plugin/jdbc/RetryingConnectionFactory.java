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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientException;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public class RetryingConnectionFactory
        implements ConnectionFactory
{
    private final RetryPolicy<Object> retryPolicy;
    private final ConnectionFactory delegate;

    @Inject
    public RetryingConnectionFactory(@ForBaseJdbc ConnectionFactory delegate, RetryingConnectionCondition condition)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        requireNonNull(condition, "condition is null");

        this.retryPolicy = new RetryPolicy<>()
                .withMaxDuration(java.time.Duration.of(30, SECONDS))
                .withMaxAttempts(5)
                .withBackoff(50, 5_000, MILLIS, 4)
                .handleIf(condition::isRetryable)
                .abortOn(TrinoException.class);
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

    public static boolean isRetryableException(Throwable exception)
    {
        return Throwables.getCausalChain(exception).stream()
                .anyMatch(SQLTransientException.class::isInstance);
    }
}
