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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConnectionCreationTest;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcPlugin;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;

// TODO: Implement dedicated test to count number of queries executed.
// This test approximates the number of I/O operations by counting number of times connection is opened since we almost always open a new connection to execute a query.
public class TestSqlServerJdbcConnectionAccesses
        extends BaseJdbcConnectionCreationTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingSqlServer sqlServer = closeAfterClass(new TestingSqlServer());
        this.connectionFactory = getConnectionCountingConnectionFactory(sqlServer);
        DistributedQueryRunner queryRunner = SqlServerQueryRunner.builder(sqlServer)
                // to make sure we always open connections in the same way
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false")
                .amendSession(sessionBuilder -> sessionBuilder.setCatalog("counting_sqlserver"))
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new JdbcPlugin(
                            "counting_sqlserver",
                            () -> combine(new SqlServerClientModule(), new TestingSqlServerModule(connectionFactory))));
                    runner.createCatalog("counting_sqlserver", "counting_sqlserver", ImmutableMap.of(
                            "connection-url", sqlServer.getJdbcUrl(),
                            "connection-user", sqlServer.getUsername(),
                            "connection-password", sqlServer.getPassword(),
                            // disables connection reuse to approximate number of I/O operations since we almost always open a new connection to execute a query
                            "query.reuse-connection", "false"));
                })
                .build();
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, ImmutableList.of(NATION, REGION));
        return queryRunner;
    }

    private static ConnectionCountingConnectionFactory getConnectionCountingConnectionFactory(TestingSqlServer sqlServer)
    {
        Properties connectionProperties = new Properties();
        CredentialProvider credentialProvider = new StaticCredentialProvider(
                Optional.of(sqlServer.getUsername()),
                Optional.of(sqlServer.getPassword()));
        DriverConnectionFactory delegate = DriverConnectionFactory.builder(new SQLServerDriver(), sqlServer.getJdbcUrl(), credentialProvider)
                .setConnectionProperties(connectionProperties)
                .build();
        return new ConnectionCountingConnectionFactory(delegate);
    }

    @Test
    public void testJdbcConnectionCreations()
    {
        assertJdbcConnections("SELECT * FROM nation LIMIT 1", 5, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation ORDER BY nationkey LIMIT 1", 5, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation WHERE nationkey = 1", 5, Optional.empty());
        assertJdbcConnections("SELECT avg(nationkey) FROM nation", 4, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation, region", 6, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation n, region r WHERE n.regionkey = r.regionkey", 9, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation JOIN region USING(regionkey)", 10, Optional.empty());
        assertJdbcConnections("SELECT * FROM information_schema.schemata", 1, Optional.empty());
        assertJdbcConnections("SELECT * FROM information_schema.tables", 1, Optional.empty());
        assertJdbcConnections("SELECT * FROM information_schema.columns", 1041, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation", 3, Optional.empty());
        assertJdbcConnections("SELECT * FROM TABLE (system.query(query => 'SELECT * FROM dbo.nation'))", 2, Optional.empty());
        assertJdbcConnections("CREATE TABLE copy_of_nation AS SELECT * FROM nation", 15, Optional.empty());
        assertJdbcConnections("INSERT INTO copy_of_nation SELECT * FROM nation", 14, Optional.empty());
        assertJdbcConnections("DELETE FROM copy_of_nation WHERE nationkey = 3", 6, Optional.empty());
        assertJdbcConnections("UPDATE copy_of_nation SET name = 'POLAND' WHERE nationkey = 1", 5, Optional.empty());
        assertJdbcConnections("MERGE INTO copy_of_nation n USING region r ON r.regionkey= n.regionkey WHEN MATCHED THEN DELETE", 6, Optional.of(MODIFYING_ROWS_MESSAGE));
        assertJdbcConnections("DROP TABLE copy_of_nation", 2, Optional.empty());
        assertJdbcConnections("SHOW SCHEMAS", 1, Optional.empty());
        assertJdbcConnections("SHOW TABLES", 2, Optional.empty());
        assertJdbcConnections("SHOW STATS FOR nation", 4, Optional.empty());
        assertJdbcConnections("SELECT * FROM system.jdbc.columns WHERE table_cat = 'counting_sqlserver'", 1041, Optional.empty());
    }

    private static final class TestingSqlServerModule
            extends AbstractConfigurationAwareModule
    {
        private final ConnectionCountingConnectionFactory connectionCountingConnectionFactory;

        private TestingSqlServerModule(ConnectionCountingConnectionFactory connectionCountingConnectionFactory)
        {
            this.connectionCountingConnectionFactory = requireNonNull(connectionCountingConnectionFactory, "connectionCountingConnectionFactory is null");
        }

        @Override
        protected void setup(Binder binder) {}

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory()
        {
            return connectionCountingConnectionFactory;
        }
    }
}
