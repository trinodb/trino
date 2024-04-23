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

import io.trino.testing.ResourcePresence;
import io.trino.testing.containers.TestContainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Neo4jContainer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;

public class Neo4JContainerTestingServer
        implements Neo4jTestingServer
{
    private static final String DEFAULT_IMAGE = "neo4j:5.12.0";
    private static final String DEFAULT_USER_NAME = "neo4j";
    private static final String DEFAULT_PASSWORD = "p@ssword";
    private static final String SAMPLE_DATA_PATH = "/neo4j/data/movie-graph-data.cql";

    private final Neo4jContainer<?> container;
    private final Closeable cleanup;

    public Neo4JContainerTestingServer()
    {
        this(DEFAULT_IMAGE);
    }

    public Neo4JContainerTestingServer(String dockerImageName)
    {
        this.container = new Neo4jContainer<>(dockerImageName)
                .withAdminPassword(DEFAULT_PASSWORD)
                .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]") // for neo4j 4.4
                .withEnv("NEO4J_PLUGINS", "[\"apoc\"]");     // for neo4j 5.0

        this.cleanup = TestContainers.startOrReuse(this.container);
    }

    @Override
    public String getUri()
    {
        return this.container.getBoltUrl();
    }

    @Override
    public String getUsername()
    {
        return DEFAULT_USER_NAME;
    }

    @Override
    public String getPassword()
    {
        return container.getAdminPassword();
    }

    public void loadSampleData()
            throws Exception
    {
        Container.ExecResult result = this.container.execInContainer("sh", "-c", String.format("cypher-shell -u %s -p %s -f %s", DEFAULT_USER_NAME, DEFAULT_PASSWORD, SAMPLE_DATA_PATH));
    }

    @Override
    public void close()
    {
        try {
            cleanup.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @ResourcePresence
    public boolean isRunning()
    {
        return container.getContainerId() != null;
    }

    public static void main(String[] args)
            throws Exception
    {
        try (Neo4JContainerTestingServer server = new Neo4JContainerTestingServer()) {
            System.out.println("neo4j.uri: " + server.getUri());

            System.out.println("loading sample data");
            server.loadSampleData();

            System.out.println("sample data loaded");
        }
    }
}
