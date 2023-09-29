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

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public class RetryingConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;
    private final RetryPolicy<Object> retryPolicy;

    public RetryingConnectionFactory(ConnectionFactory delegate, RetryingConnectionFactory.RetryStrategy strategy)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        retryPolicy = RetryPolicy.builder()
                .withMaxDuration(java.time.Duration.of(30, SECONDS))
                .withMaxAttempts(5)
                .withBackoff(50, 5_000, MILLIS, 4)
                .handleIf(strategy::isSqlRecoverableException)
                .abortOn(TrinoException.class)
                .build();
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
        boolean isSqlRecoverableException(Throwable exception);
    }
}
