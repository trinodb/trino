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
package io.trino.plugin.scylladb;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.base.util.Closables;
import io.trino.plugin.cassandra.CassandraServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class ScyllaDbQueryRunner
{
    private ScyllaDbQueryRunner() {}

    public static Builder builder(CassandraServer server)
    {
        return new Builder(server)
                .addConnectorProperty("cassandra.contact-points", server.getHost())
                .addConnectorProperty("cassandra.native-protocol-port", Integer.toString(server.getPort()))
                .addConnectorProperty("cassandra.allow-drop-table", "true")
                .addConnectorProperty("cassandra.load-policy.use-dc-aware", "true")
                .addConnectorProperty("cassandra.load-policy.dc-aware.local-dc", "datacenter1");
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final CassandraServer server;
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        private Builder(CassandraServer server)
        {
            super(testSessionBuilder()
                    .setCatalog("cassandra")
                    .setSchema("tpch")
                    .build());
            this.server = requireNonNull(server, "server is null");
        }

        @CanIgnoreReturnValue
        public Builder addConnectorProperty(String key, String value)
        {
            this.connectorProperties.put(key, value);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(List<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(initialTables);
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

                queryRunner.installPlugin(new ScyllaDbPlugin());
                queryRunner.createCatalog("cassandra", "scylladb", connectorProperties);

                String schemaName = queryRunner.getDefaultSession().getSchema().orElseThrow();
                createKeyspace(server.getSession(), schemaName);
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);
                for (TpchTable<?> table : initialTables) {
                    server.refreshSizeEstimates(schemaName, table.getTableName());
                }
                return queryRunner;
            }
            catch (Throwable e) {
                Closables.closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        QueryRunner queryRunner = builder(new TestingScyllaDbServer())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setInitialTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(ScyllaDbQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
