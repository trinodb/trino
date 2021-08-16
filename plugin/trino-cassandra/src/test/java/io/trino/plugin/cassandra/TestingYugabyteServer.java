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

import com.datastax.driver.core.Cluster;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;

import static com.datastax.driver.core.ProtocolVersion.V3;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestingYugabyteServer
        implements Closeable
{
    public static final Integer PORT = 9042;
    public static final String SCHEMA = "tpch";

    private final GenericContainer<?> dockerContainer;
    private final CassandraSession session;

    public TestingYugabyteServer()
    {
        this("yugabytedb/yugabyte:latest");
    }

    public TestingYugabyteServer(String dockerImageName)
    {
        dockerContainer = new GenericContainer<>(DockerImageName.parse(dockerImageName))
                .withCommand("bin/yugabyted", "start", "--daemon=false")
                .withStartupTimeout(Duration.ofMinutes(3))
                .withExposedPorts(PORT);
        dockerContainer.start();

        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(V3)
                .withClusterName("TestCluster")
                .addContactPointsWithPorts(ImmutableList.of(
                        new InetSocketAddress(dockerContainer.getContainerIpAddress(), dockerContainer.getMappedPort(PORT))))
                .withMaxSchemaAgreementWaitSeconds(30);

        ReopeningCluster cluster = new ReopeningCluster(clusterBuilder::build, new io.airlift.units.Duration(Long.MAX_VALUE, DAYS));
        session = new CassandraSession(
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                cluster,
                new io.airlift.units.Duration(Long.MAX_VALUE, DAYS),
                new io.airlift.units.Duration(1, MINUTES));
    }

    public CassandraSession getSession()
    {
        return requireNonNull(session, "session is null");
    }

    public String getHost()
    {
        return dockerContainer.getHost();
    }

    public int getPort()
    {
        return dockerContainer.getMappedPort(PORT);
    }

    @Override
    public void close()
            throws IOException
    {
        dockerContainer.close();
    }
}
