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

import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Neo4jContainer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.testing.containers.TestContainers.startOrReuse;

public class TestingNeo4jServer
        implements AutoCloseable
{
    private static final String DEFAULT_IMAGE = "neo4j:4.4";
    private static final String DEFAULT_USER_NAME = "neo4j";
    private static final String DEFAULT_PASSWORD = "p@ssword";
    private static final String SAMPLE_DATA_PATH = "/neo4j/data/movie-graph-data.cql";

    private final Neo4jContainer<?> container;
    private final Closeable cleanup;

    public TestingNeo4jServer()
    {
        this(DEFAULT_IMAGE);
    }

    public TestingNeo4jServer(String dockerImageName)
    {
        this.container = new Neo4jContainer<>(dockerImageName)
                .withAdminPassword(DEFAULT_PASSWORD)
//                .withEnv("NEO4JLABS_PLUGINS", "[\"apoc\"]")
                // this data is used to pre-load neo4j schema
                .withClasspathResourceMapping("/movies-graph-data.cql", SAMPLE_DATA_PATH, BindMode.READ_ONLY);

        this.cleanup = startOrReuse(this.container);
    }

    public String getBoltUrl()
    {
        return this.container.getBoltUrl();
    }

    public String getUsername()
    {
        return DEFAULT_USER_NAME;
    }

    public String getPassword()
    {
        return container.getAdminPassword();
    }

    public String getJdbcUrl(Optional<Map<String, String>> extraUrlParams)
    {
        String extraUrlParamsString = "";
        if (extraUrlParams.isPresent()) {
            extraUrlParamsString = extraUrlParamsString + extraUrlParams.get().keySet().stream().map(key -> key + "=" + extraUrlParams.get().get(key)).collect(Collectors.joining("&"));
        }
        return String.format("jdbc:neo4j:%s/?" + extraUrlParamsString, this.container.getBoltUrl());
    }

    public void loadSampleData() throws Exception
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

    public static void main(String[] args) throws Exception
    {
        try (TestingNeo4jServer server = new TestingNeo4jServer()) {
            System.out.println("Bolt url - " + server.getBoltUrl());
            server.loadSampleData();
        }
    }
}
