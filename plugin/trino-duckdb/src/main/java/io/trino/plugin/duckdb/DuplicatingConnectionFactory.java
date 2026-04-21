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
package io.trino.plugin.duckdb;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.PreDestroy;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class DuplicatingConnectionFactory
        implements ConnectionFactory
{
    private final CredentialProvider credentialProvider;
    private final ConnectionFactory delegate;
    private final AtomicReference<DuckDBConnection> currentConnection = new AtomicReference<>();

    public DuplicatingConnectionFactory(CredentialProvider credentialProvider, ConnectionFactory delegate)
    {
        this.credentialProvider = requireNonNull(credentialProvider, "credentialProvider is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        verify(
                credentialProvider.getConnectionUser(Optional.ofNullable(session.getIdentity())).isEmpty(),
                "DuckDB does not support user authentication");

        if (currentConnection.get() == null) {
            DuckDBConnection newConnection = delegate.openConnection(session).unwrap(DuckDBConnection.class);
            if (!currentConnection.compareAndSet(null, newConnection)) {
                try {
                    newConnection.close();
                }
                catch (SQLException _) {
                    // Ignore close exception on duplicated connection
                }
            }
        }
        return currentConnection.get().duplicate();
    }

    @Override
    @PreDestroy
    public void close()
            throws SQLException
    {
        DuckDBConnection previous = currentConnection.getAndSet(null);
        if (previous != null) {
            previous.close();
        }
    }
}
