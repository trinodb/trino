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
package io.trino.plugin.neo4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingTrinoClient;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class Neo4jQueryRunner
{
    private static final Logger log = Logger.get(Neo4jQueryRunner.class);

    private Neo4jQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createDefaultQueryRunner(TestingNeo4jServer neo4jServer) throws Exception
    {
        DistributedQueryRunner queryRunner = createNeo4jQueryRunner(
                neo4jServer,
                ImmutableMap.of(),
                ImmutableMap.of(),
                Optional.empty());
        return queryRunner;
    }

    public static DistributedQueryRunner createDefaultQueryRunner() throws Exception
    {
        TestingNeo4jServer neo4jServer = new TestingNeo4jServer();
        return createDefaultQueryRunner(neo4jServer);
    }

    public static DistributedQueryRunner createNeo4jQueryRunner(TestingNeo4jServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            Optional<Iterable<TpchTable<?>>> tables)
            throws Exception
    {
        server.loadSampleData();
        Session session = createSession();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl(Optional.empty()));
            connectorProperties.putIfAbsent("connection-user", server.getUsername());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
            queryRunner.installPlugin(new Neo4jPlugin());
            queryRunner.createCatalog("neo4j", "neo4j", connectorProperties);

            TestingTrinoClient trinoClient = queryRunner.getClient();
            if (tables.isPresent()) {
                for (TpchTable<?> table : tables.get()) {
                    loadTpchTable(server, trinoClient, table);
                }
            }
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
            log.info("======== SERVER STARTED ========");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void loadTpchTable(TestingNeo4jServer server, TestingTrinoClient trinoClient, TpchTable table)
    {
        log.info("Running import for %s", table.getTableName());
        Neo4jLoader loader = new Neo4jLoader(server, table.getTableName(), trinoClient.getServer(), trinoClient.getDefaultSession());
        loader.execute(format("SELECT * from %s", new QualifiedObjectName(TPCH_SCHEMA, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH))));
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("neo4j")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        TestingNeo4jServer neo4jServer = new TestingNeo4jServer();
        Neo4jQueryRunner.createNeo4jQueryRunner(neo4jServer, ImmutableMap.of(), ImmutableMap.of(), Optional.of(ImmutableList.of(ORDERS, NATION, REGION, CUSTOMER)));
    }
}
