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
package io.trino.plugin.jdbc.jmx;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.spi.connector.ConnectorSession;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class StatisticsAwareConnectionFactory
        implements ConnectionFactory
{
    private final JdbcApiStats openConnection = new JdbcApiStats();
    private final JdbcApiStats closeConnection = new JdbcApiStats();
    private final ConnectionFactory delegate;

    @Inject
    public StatisticsAwareConnectionFactory(@ForBaseJdbc ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return openConnection.wrap(() -> delegate.openConnection(session));
    }

    @Override
    public void close()
            throws SQLException
    {
        closeConnection.wrap(delegate::close);
    }

    @Managed
    @Nested
    public JdbcApiStats getOpenConnection()
    {
        return openConnection;
    }

    @Managed
    @Nested
    public JdbcApiStats getCloseConnection()
    {
        return closeConnection;
    }
}
