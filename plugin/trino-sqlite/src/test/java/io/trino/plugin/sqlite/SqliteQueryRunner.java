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
package io.trino.plugin.sqlite;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.sqlite.SqliteTpchTables.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SqliteQueryRunner
{
    private SqliteQueryRunner() {}

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.sqlite.JDBC", Level.OFF);
    }

    private static final Logger log = Logger.get(SqliteQueryRunner.class);

    public static final String SQLITE = "sqlite";

    public static final String SCHEMA = "main";

    public static Builder builder(TestingSqliteServer sqliteServer)
    {
        return new Builder(sqliteServer)
                .addConnectorProperty("connection-url", sqliteServer.getJdbcUrl())
                .addConnectorProperty("connection-user", sqliteServer.getUsername())
                .addConnectorProperty("connection-password", sqliteServer.getPassword());

                // FIXME: The following lines are necessary if the connector cannot rename the tables
                //.addConnectorProperty("statistics.enabled", "false")
                //.addConnectorProperty("insert.non-transactional-insert.enabled", "true")
                //.addConnectorProperty("merge.non-transactional-merge.enabled", "true");
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final TestingSqliteServer sqliteServer;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(TestingSqliteServer sqliteServer)
        {
            super(testSessionBuilder()
                    .setCatalog(SQLITE)
                    .setSchema(SCHEMA)
                    .build());
            this.sqliteServer = sqliteServer;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(requireNonNull(connectorProperties, "connectorProperties is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new SqlitePlugin());
                queryRunner.createCatalog(SQLITE, SQLITE, connectorProperties);
                log.info("%s catalog properties: %s", SQLITE, connectorProperties);

                //copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);
                copyTpchTables(sqliteServer, queryRunner, initialTables);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("io.trino.plugin.sqlite", Level.DEBUG);
        logger.setLevel("io.trino", Level.DEBUG);

        QueryRunner queryRunner = builder(new TestingSqliteServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(SqliteQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
