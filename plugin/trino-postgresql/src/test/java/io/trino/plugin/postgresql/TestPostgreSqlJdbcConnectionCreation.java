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
package io.trino.plugin.postgresql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConnectionCreationTest;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcPlugin;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.postgresql.Driver;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.postgresql.PostgreSqlQueryRunner.createSession;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;

@Test(singleThreaded = true) // inherited from BaseJdbcConnectionCreationTest
public class TestPostgreSqlJdbcConnectionCreation
        extends BaseJdbcConnectionCreationTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        Properties connectionProperties = new Properties();
        CredentialProvider credentialProvider = new StaticCredentialProvider(
                Optional.of(postgreSqlServer.getUser()),
                Optional.of(postgreSqlServer.getPassword()));
        DriverConnectionFactory delegate = new DriverConnectionFactory(new Driver(), postgreSqlServer.getJdbcUrl(), connectionProperties, credentialProvider, OpenTelemetry.noop());
        this.connectionFactory = new ConnectionCountingConnectionFactory(delegate);
        return createPostgreSqlQueryRunner(postgreSqlServer, ImmutableList.of(NATION, REGION), connectionFactory);
    }

    @Test(dataProvider = "testCases")
    public void testJdbcConnectionCreations(@Language("SQL") String query, int expectedJdbcConnectionsCount, Optional<String> errorMessage)
    {
        assertJdbcConnections(query, expectedJdbcConnectionsCount, errorMessage);
    }

    @DataProvider
    public Object[][] testCases()
    {
        return new Object[][] {
                {"SELECT * FROM nation LIMIT 1", 3, Optional.empty()},
                {"SELECT * FROM nation ORDER BY nationkey LIMIT 1", 3, Optional.empty()},
                {"SELECT * FROM nation WHERE nationkey = 1", 3, Optional.empty()},
                {"SELECT avg(nationkey) FROM nation", 2, Optional.empty()},
                {"SELECT * FROM nation, region", 3, Optional.empty()},
                {"SELECT * FROM nation n, region r WHERE n.regionkey = r.regionkey", 3, Optional.empty()},
                {"SELECT * FROM nation JOIN region USING(regionkey)", 5, Optional.empty()},
                {"SELECT * FROM information_schema.schemata", 1, Optional.empty()},
                {"SELECT * FROM information_schema.tables", 1, Optional.empty()},
                {"SELECT * FROM information_schema.columns", 1, Optional.empty()},
                {"SELECT * FROM nation", 2, Optional.empty()},
                {"SELECT * FROM TABLE (system.query(query => 'SELECT * FROM tpch.nation'))", 2, Optional.empty()},
                {"CREATE TABLE copy_of_nation AS SELECT * FROM nation", 6, Optional.empty()},
                {"INSERT INTO copy_of_nation SELECT * FROM nation", 6, Optional.empty()},
                {"DELETE FROM copy_of_nation WHERE nationkey = 3", 1, Optional.empty()},
                {"UPDATE copy_of_nation SET name = 'POLAND' WHERE nationkey = 1", 1, Optional.of(MODIFYING_ROWS_MESSAGE)},
                {"MERGE INTO copy_of_nation n USING region r ON r.regionkey= n.regionkey WHEN MATCHED THEN DELETE", 1, Optional.of(MODIFYING_ROWS_MESSAGE)},
                {"DROP TABLE copy_of_nation", 1, Optional.empty()},
                {"SHOW SCHEMAS", 1, Optional.empty()},
                {"SHOW TABLES", 1, Optional.empty()},
                {"SHOW STATS FOR nation", 2, Optional.empty()},
        };
    }

    private static DistributedQueryRunner createPostgreSqlQueryRunner(
            TestingPostgreSqlServer server,
            Iterable<TpchTable<?>> tables,
            ConnectionCountingConnectionFactory connectionCountingConnectionFactory)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                // to make sure we always open connections in the same way
                .setCoordinatorProperties(ImmutableMap.of("node-scheduler.include-coordinator", "false"))
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new JdbcPlugin(
                    "postgresql",
                    combine(new PostgreSqlClientModule(), new TestingPostgreSqlModule(connectionCountingConnectionFactory))));
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.createCatalog("postgresql", "postgresql", ImmutableMap.of(
                    "connection-url", server.getJdbcUrl(),
                    "connection-user", server.getUser(),
                    "connection-password", server.getPassword()));
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), tables);
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static final class TestingPostgreSqlModule
            extends AbstractConfigurationAwareModule
    {
        private final ConnectionCountingConnectionFactory connectionCountingConnectionFactory;

        private TestingPostgreSqlModule(ConnectionCountingConnectionFactory connectionCountingConnectionFactory)
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
