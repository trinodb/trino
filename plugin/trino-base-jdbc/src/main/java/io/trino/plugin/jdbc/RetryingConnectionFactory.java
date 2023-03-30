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
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;

public class RetryingConnectionFactory
        implements ConnectionFactory
{
    private static final RetryPolicy<Object> RETRY_POLICY = RetryPolicy.builder()
            .withMaxDuration(java.time.Duration.of(30, SECONDS))
            .withMaxAttempts(5)
            .withBackoff(50, 5_000, MILLIS, 4)
            .handleIf(RetryingConnectionFactory::isSqlRecoverableException)
            .abortOn(TrinoException.class)
            .build();

    private final ConnectionFactory delegate;

    public RetryingConnectionFactory(ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        try {
            return Failsafe.with(RETRY_POLICY)
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

    private static boolean isSqlRecoverableException(Throwable exception)
    {
        return Throwables.getCausalChain(exception).stream()
                .anyMatch(SQLRecoverableException.class::isInstance);
    }
}
