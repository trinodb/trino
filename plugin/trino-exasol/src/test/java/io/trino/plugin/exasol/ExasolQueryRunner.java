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
package io.trino.plugin.exasol;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.exasol.ExasolTpchTables.copyAndIngestTpchData;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_PASSWORD;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_SCHEMA;
import static io.trino.plugin.exasol.TestingExasolServer.TEST_USER;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ExasolQueryRunner
{
    private static final Logger log = Logger.get(ExasolQueryRunner.class);

    private ExasolQueryRunner() {}

    public static Builder builder(TestingExasolServer server)
    {
        return new Builder(server)
                .addConnectorProperty("connection-url", server.getJdbcUrl())
                .addConnectorProperty("connection-user", TEST_USER)
                .addConnectorProperty("connection-password", TEST_PASSWORD);
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private final TestingExasolServer server;

        private Builder(TestingExasolServer server)
        {
            super(testSessionBuilder()
                    .setCatalog("exasol")
                    .setSchema(TEST_SCHEMA)
                    .build());
            this.server = server;
        }

        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
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

                queryRunner.installPlugin(new ExasolPlugin());
                queryRunner.createCatalog("exasol", "exasol", connectorProperties);

                log.info("Loading data from exasol.%s...", TEST_SCHEMA);
                for (TpchTable<?> table : initialTables) {
                    log.info("Running import for %s", table.getTableName());
                    String tpchTableName = table.getTableName();
                    MaterializedResult rows = queryRunner.execute(format("SELECT * FROM tpch.%s.%s", TINY_SCHEMA_NAME, tpchTableName));
                    copyAndIngestTpchData(rows, server, table.getTableName());
                    log.info("Imported %s rows for %s", rows.getRowCount(), table.getTableName());
                }
                log.info("Loading from exasol.%s complete", TEST_SCHEMA);
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
        Logging.initialize();

        DistributedQueryRunner queryRunner = ExasolQueryRunner.builder(new TestingExasolServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(ExasolQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
