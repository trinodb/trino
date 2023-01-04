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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class Neo4jQueryRunner
{
    private static final Logger log = Logger.get(Neo4jQueryRunner.class);

    private Neo4jQueryRunner() {}

    private static final String TPCH_SCHEMA = "tpch";

    public static DistributedQueryRunner createDefaultQueryRunner(TestingNeo4jServer neo4jServer) throws Exception
    {
        neo4jServer.loadSampleData();
        DistributedQueryRunner queryRunner = createNeo4jQueryRunner(
                neo4jServer,
                ImmutableMap.of(),
                ImmutableMap.of());
        return queryRunner;
    }

    public static DistributedQueryRunner createDefaultQueryRunner() throws Exception
    {
        TestingNeo4jServer neo4jServer = new TestingNeo4jServer();
        return createDefaultQueryRunner(neo4jServer);
    }

    public static DistributedQueryRunner createNeo4jQueryRunner(TestingNeo4jServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setExtraProperties(extraProperties)
                .build();
        try {
//            queryRunner.installPlugin(new TpchPlugin());
//            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl(Optional.empty()));
            connectorProperties.putIfAbsent("connection-user", server.getUsername());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());
            queryRunner.installPlugin(new Neo4jPlugin());
            queryRunner.createCatalog("neo4j", "neo4j", connectorProperties);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
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
        DistributedQueryRunner queryRunner = Neo4jQueryRunner.createDefaultQueryRunner();
        Logger log = Logger.get(Neo4jQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
