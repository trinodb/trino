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
package io.prestosql.plugin.jdbc.jmx;

import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcIdentity;
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

    public StatisticsAwareConnectionFactory(ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        return openConnection.wrap(() -> delegate.openConnection(identity));
    }

    @Override
    public void close()
            throws SQLException
    {
        closeConnection.wrap(() -> delegate.close());
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
