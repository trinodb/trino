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

import com.google.inject.Inject;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.trino.spi.connector.ConnectorSession;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class RetryingConnectionFactory
        implements ConnectionFactory
{
    private final ConnectionFactory delegate;
    private final RetryPolicy<Object> retryPolicy;

    @Inject
    public RetryingConnectionFactory(@ForRetrying ConnectionFactory delegate, RetryPolicy<Object> retryPolicy)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.retryPolicy = requireNonNull(retryPolicy, "retryPolicy is null");
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
}
