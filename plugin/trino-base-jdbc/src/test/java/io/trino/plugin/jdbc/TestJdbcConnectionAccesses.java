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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.credential.EmptyCredentialProvider;
import io.trino.testing.QueryRunner;
import org.h2.Driver;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static io.trino.plugin.jdbc.TestingH2JdbcModule.createH2ConnectionUrl;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static java.util.Objects.requireNonNull;

// TODO: Implement dedicated test to count number of queries executed.
// This test approximates the number of I/O operations by counting number of times connection is opened since we almost always open a new connection to execute a query.
public class TestJdbcConnectionAccesses
        extends BaseJdbcConnectionCreationTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String connectionUrl = createH2ConnectionUrl();
        DriverConnectionFactory delegate = DriverConnectionFactory.builder(new Driver(), connectionUrl, new EmptyCredentialProvider()).build();
        this.connectionFactory = new ConnectionCountingConnectionFactory(delegate);
        return createH2QueryRunner(
                ImmutableList.of(NATION, REGION),
                ImmutableMap.of("connection-url", connectionUrl,
                        // disables connection reuse to approximate number of I/O operations since we almost always open a new connection to execute a query
                        "query.reuse-connection", "false"),
                // to make sure we always open connections in the same way
                ImmutableMap.of("node-scheduler.include-coordinator", "false"),
                new TestingConnectionH2Module(connectionFactory));
    }

    @Test
    public void testJdbcConnectionCreations()
    {
        assertJdbcConnections("SELECT * FROM nation LIMIT 1", 3, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation ORDER BY nationkey LIMIT 1", 3, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation WHERE nationkey = 1", 3, Optional.empty());
        assertJdbcConnections("SELECT avg(nationkey) FROM nation", 3, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation, region", 6, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation n, region r WHERE n.regionkey = r.regionkey", 6, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation JOIN region USING(regionkey)", 6, Optional.empty());
        assertJdbcConnections("SELECT * FROM information_schema.schemata", 1, Optional.empty());
        assertJdbcConnections("SELECT * FROM information_schema.tables", 1, Optional.empty());
        assertJdbcConnections("SELECT * FROM information_schema.columns", 5, Optional.empty());
        assertJdbcConnections("SELECT * FROM nation", 3, Optional.empty());
        assertJdbcConnections("CREATE TABLE copy_of_nation AS SELECT * FROM nation", 11, Optional.empty());
        assertJdbcConnections("INSERT INTO copy_of_nation SELECT * FROM nation", 12, Optional.empty());
        assertJdbcConnections("DELETE FROM copy_of_nation WHERE nationkey = 3", 3, Optional.empty());
        assertJdbcConnections("UPDATE copy_of_nation SET name = 'POLAND' WHERE nationkey = 1", 3, Optional.empty());
        assertJdbcConnections("MERGE INTO copy_of_nation n USING region r ON r.regionkey= n.regionkey WHEN MATCHED THEN DELETE", 4, Optional.of(MODIFYING_ROWS_MESSAGE));
        assertJdbcConnections("DROP TABLE copy_of_nation", 2, Optional.empty());
        assertJdbcConnections("SHOW SCHEMAS", 1, Optional.empty());
        assertJdbcConnections("SHOW TABLES", 2, Optional.empty());
        assertJdbcConnections("SHOW STATS FOR nation", 2, Optional.empty());
        assertJdbcConnections("SELECT * FROM system.jdbc.columns WHERE table_cat = 'jdbc'", 5, Optional.empty());
    }

    private static class TestingConnectionH2Module
            implements Module
    {
        private final ConnectionCountingConnectionFactory connectionCountingConnectionFactory;

        TestingConnectionH2Module(ConnectionCountingConnectionFactory connectionCountingConnectionFactory)
        {
            this.connectionCountingConnectionFactory = requireNonNull(connectionCountingConnectionFactory, "connectionCountingConnectionFactory is null");
        }

        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        @ForBaseJdbc
        public static JdbcClient provideJdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping)
        {
            return new TestingH2JdbcClient(config, connectionFactory, queryBuilder, identifierMapping);
        }

        @Provides
        @Singleton
        @ForBaseJdbc
        public ConnectionFactory getConnectionFactory()
        {
            return connectionCountingConnectionFactory;
        }
    }
}
