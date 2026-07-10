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
package io.trino.plugin.cluster;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcConnector;
import io.trino.plugin.jdbc.JdbcTransactionManager;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.procedure.Procedure;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

public class ClusterConnector
        extends JdbcConnector
{
    private final ConnectionFactory connectionFactory;
    private final ClusterClient clusterClient;

    @Inject
    public ClusterConnector(
            LifeCycleManager lifeCycleManager,
            ConnectorSplitManager jdbcSplitManager,
            ConnectorPageSourceProvider jdbcPageSourceProvider,
            ConnectorPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<ConnectorTableFunction> connectorTableFunctions,
            Set<SessionPropertiesProvider> sessionProperties,
            Set<TablePropertiesProvider> tableProperties,
            JdbcTransactionManager transactionManager,
            @ForBaseJdbc ConnectionFactory connectionFactory,
            ClusterClient clusterClient)
    {
        super(lifeCycleManager,
                jdbcSplitManager,
                jdbcPageSourceProvider,
                jdbcPageSinkProvider,
                accessControl,
                procedures,
                connectorTableFunctions,
                sessionProperties,
                tableProperties,
                transactionManager);
        this.connectionFactory = connectionFactory;
        this.clusterClient = clusterClient;
    }

    @Override
    public Collection<String> getCatalogs()
    {
        try {
            return clusterClient.listCatalogs(connectionFactory.openConnection());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
