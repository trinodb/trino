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
import io.trino.Session;
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
import org.postgresql.Driver;

import java.util.Optional;
import java.util.Properties;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_MERGE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;

// TODO: Implement dedicated test to count number of queries executed.
// This test approximates the number of I/O operations by counting number of times connection is opened since we almost always open a new connection to execute a query.
public class TestPostgreSqlJdbcConnectionAccesses
        extends BaseJdbcConnectionCreationTest
{
    private TestingPostgreSqlServer postgreSqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingPostgreSqlServer postgreSqlServer = closeAfterClass(new TestingPostgreSqlServer());
        this.postgreSqlServer = requireNonNull(postgreSqlServer, "postgreSqlServer is null");
        this.connectionFactory = getConnectionCountingConnectionFactory(postgreSqlServer);
        DistributedQueryRunner queryRunner = PostgreSqlQueryRunner.builder(postgreSqlServer)
                // to make sure we always open connections in the same way
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false")
                .amendSession(sessionBuilder -> sessionBuilder.setCatalog("counting_postgresql"))
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new JdbcPlugin(
                            "counting_postgresql",
                            () -> combine(new PostgreSqlClientModule(), new TestingPostgreSqlModule(connectionFactory))));
                    runner.createCatalog("counting_postgresql", "counting_postgresql", ImmutableMap.of(
                            "connection-url", postgreSqlServer.getJdbcUrl(),
                            "connection-user", postgreSqlServer.getUser(),
                            "connection-password", postgreSqlServer.getPassword(),
                            // disables connection reuse to approximate number of I/O operations since we almost always open a new connection to execute a query
                            "query.reuse-connection", "false"));
                })
                .build();
        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, ImmutableList.of(CUSTOMER, NATION, REGION));
        return queryRunner;
    }

    private static ConnectionCountingConnectionFactory getConnectionCountingConnectionFactory(TestingPostgreSqlServer postgreSqlServer)
    {
        Properties connectionProperties = new Properties();
        CredentialProvider credentialProvider = new StaticCredentialProvider(
                Optional.of(postgreSqlServer.getUser()),
                Optional.of(postgreSqlServer.getPassword()));
        DriverConnectionFactory delegate = DriverConnectionFactory.builder(new Driver(), postgreSqlServer.getJdbcUrl(), credentialProvider)
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
        assertJdbcConnections("SELECT * FROM information_schema.columns", 7, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation", 3, Optional.empty());
        assertJdbcConnections("SELECT * FROM TABLE (system.query(query => 'SELECT * FROM tpch.nation'))", 2, Optional.empty());
        assertJdbcConnections("CREATE TABLE copy_of_nation AS SELECT * FROM nation", 15, Optional.empty());
        assertJdbcConnections("INSERT INTO copy_of_nation SELECT * FROM nation", 13, Optional.empty());
        assertJdbcConnections("UPDATE copy_of_nation SET name = 'POLAND' WHERE nationkey = 1", 6, Optional.empty());
        assertJdbcConnections("MERGE INTO copy_of_nation n USING region r ON r.regionkey= n.regionkey WHEN MATCHED THEN DELETE", 7, Optional.of("Non-transactional MERGE is disabled"));
        assertJdbcConnections("DELETE FROM copy_of_nation WHERE nationkey = 3", 6, Optional.empty());
        assertJdbcConnections("DROP TABLE copy_of_nation", 2, Optional.empty());
        assertJdbcConnections("SHOW SCHEMAS", 1, Optional.empty());
        assertJdbcConnections("SHOW TABLES", 2, Optional.empty());
        assertJdbcConnections("SHOW STATS FOR nation", 4, Optional.empty());
        assertJdbcConnections("SELECT * FROM system.jdbc.columns WHERE table_cat = 'counting_postgresql'", 7, Optional.empty());

        testJdbcMergeConnectionCreations();
    }

    private void testJdbcMergeConnectionCreations()
    {
        Session mergeSession = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), NON_TRANSACTIONAL_MERGE, "true")
                .build();

        assertJdbcConnections(mergeSession, "CREATE TABLE copy_of_customer AS SELECT * FROM customer", 15, Optional.empty());

        postgreSqlServer.execute("ALTER TABLE copy_of_customer ADD CONSTRAINT t_copy_of_customer PRIMARY KEY (custkey)");
        assertJdbcConnections(mergeSession, "DELETE FROM copy_of_customer WHERE abs(custkey) = 1", 24, Optional.empty());
        assertJdbcConnections(mergeSession, "UPDATE copy_of_customer SET name = 'POLAND' WHERE abs(custkey) = 1", 32, Optional.empty());
        assertJdbcConnections(mergeSession, "MERGE INTO copy_of_customer c USING customer r ON r.custkey = c.custkey WHEN MATCHED THEN DELETE", 28, Optional.empty());
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
