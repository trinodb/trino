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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class ScyllaQueryRunner
{
    private ScyllaQueryRunner() {}

    public static DistributedQueryRunner createScyllaQueryRunner(
            TestingScyllaServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession("tpch"))
                .setExtraProperties(extraProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            // note: additional copy via ImmutableList so that if fails on nulls
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("cassandra.contact-points", server.getHost());
            connectorProperties.putIfAbsent("cassandra.native-protocol-port", Integer.toString(server.getPort()));
            connectorProperties.putIfAbsent("cassandra.allow-drop-table", "true");
            connectorProperties.putIfAbsent("cassandra.load-policy.use-dc-aware", "true");
            connectorProperties.putIfAbsent("cassandra.load-policy.dc-aware.local-dc", "datacenter1");

            queryRunner.installPlugin(new CassandraPlugin());
            queryRunner.createCatalog("cassandra", "cassandra", connectorProperties);

            createKeyspace(server.getSession(), "tpch");
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession("tpch"), tables);
            for (TpchTable<?> table : tables) {
                server.refreshSizeEstimates("tpch", table.getTableName());
            }
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession(String schema)
    {
        return testSessionBuilder()
                .setCatalog("cassandra")
                .setSchema(schema)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        DistributedQueryRunner queryRunner = createScyllaQueryRunner(
                new TestingScyllaServer(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                TpchTable.getTables());

        Logger log = Logger.get(ScyllaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
