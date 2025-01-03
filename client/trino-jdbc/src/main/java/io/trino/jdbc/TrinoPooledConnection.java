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
package io.trino.jdbc;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TrinoPooledConnection
        implements PooledConnection
{
    private final Connection connection;
    private final List<ConnectionEventListener> connectionEventListeners = new ArrayList<>();

    public TrinoPooledConnection(Connection connection)
    {
        this.connection = requireNonNull(connection, "connection is null");
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return connection;
    }

    @Override
    public void close()
            throws SQLException
    {
        connection.close();
        fireConnectionClosedEvent();
    }

    private void fireConnectionClosedEvent()
    {
        ConnectionEvent event = new ConnectionEvent(this);
        for (ConnectionEventListener listener : connectionEventListeners) {
            listener.connectionClosed(event);
        }
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener)
    {
        connectionEventListeners.add(listener);
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener)
    {
        connectionEventListeners.remove(listener);
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener)
    {
        // Not implementing
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener)
    {
        // Not implementing
    }
}
