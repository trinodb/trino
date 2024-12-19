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
package io.trino.plugin.neo4j.support;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.testing.Closeables;
import io.trino.Session;
import io.trino.plugin.neo4j.Neo4jPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingSession;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Neo4jQueryRunner
{
    private static final Logger log = Logger.get(Neo4jQueryRunner.class);

    private Neo4jQueryRunner() {}

    public static DistributedQueryRunner createDefaultQueryRunner()
            throws Exception
    {
        Neo4JContainerTestingServer neo4jServer = new Neo4JContainerTestingServer();
        return createDefaultQueryRunner(neo4jServer);
    }

    public static DistributedQueryRunner createDefaultQueryRunner(Neo4jTestingServer neo4jServer)
            throws Exception
    {
        return createNeo4jQueryRunner(
                neo4jServer,
                ImmutableMap.of(),
                ImmutableMap.of());
    }

    public static DistributedQueryRunner createNeo4jQueryRunner(Neo4jTestingServer server,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties)
            throws Exception
    {
        //server.loadSampleData();

        try (InputStream stream = Neo4jQueryRunner.class.getResourceAsStream("/node-properties.cql")) {
            String cypher = new String(stream.readAllBytes(), StandardCharsets.UTF_8);

            server.withSession(s -> {
                s.executeWrite(tx -> tx.run(cypher).consume());
            });
        }

        Session session = createSession();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();
        try {
            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("neo4j.uri", server.getUri());
            connectorProperties.putIfAbsent("neo4j.auth.type", "basic");
            connectorProperties.putIfAbsent("neo4j.auth.basic.user", server.getUsername());
            connectorProperties.putIfAbsent("neo4j.auth.basic.password", server.getPassword());
            queryRunner.installPlugin(new Neo4jPlugin());
            queryRunner.createCatalog("neo4j", "neo4j", connectorProperties);

            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
            log.info("======== SERVER STARTED ========");
            return queryRunner;
        }
        catch (Throwable e) {
            Closeables.closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Session createSession()
    {
        return TestingSession.testSessionBuilder()
                .setCatalog("neo4j")
                .setSchema("neo4j")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Neo4JContainerTestingServer neo4jServer = new Neo4JContainerTestingServer();
        Neo4jQueryRunner.createNeo4jQueryRunner(neo4jServer, ImmutableMap.of(), ImmutableMap.of());
    }
}
