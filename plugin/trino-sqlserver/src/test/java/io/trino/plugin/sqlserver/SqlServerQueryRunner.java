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
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class SqlServerQueryRunner
{
    private SqlServerQueryRunner() {}

    static {
        Logging logging = Logging.initialize();
        logging.setLevel("com.microsoft.sqlserver.jdbc", Level.OFF);
    }

    private static final Logger log = Logger.get(SqlServerQueryRunner.class);

    public static final String CATALOG = "sqlserver";
    private static final String TEST_SCHEMA = "dbo";

    public static Builder builder(TestingSqlServer testingSqlServer)
    {
        return new Builder()
                .addConnectorProperties(Map.of(
                        "connection-url", testingSqlServer.getJdbcUrl(),
                        "connection-user", testingSqlServer.getUsername(),
                        "connection-password", testingSqlServer.getPassword()));
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder()
        {
            super(testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema(TEST_SCHEMA)
                    .build());
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(requireNonNull(connectorProperties, "connectorProperties is null"));
            return this;
        }

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

                queryRunner.installPlugin(new SqlServerPlugin());
                queryRunner.createCatalog(CATALOG, "sqlserver", connectorProperties);
                log.info("%s catalog properties: %s", CATALOG, connectorProperties);

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);

                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingSqlServer testingSqlServer = new TestingSqlServer();

        // SqlServer is using docker container so in case that shutdown hook is not called, developer can easily clean docker container on their own
        Runtime.getRuntime().addShutdownHook(new Thread(testingSqlServer::close));

        QueryRunner queryRunner = builder(testingSqlServer)
                .addExtraProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(SqlServerQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
