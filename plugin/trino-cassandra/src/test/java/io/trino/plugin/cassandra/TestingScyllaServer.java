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
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.testcontainers.containers.GenericContainer;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.datastax.driver.core.ProtocolVersion.V3;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestingScyllaServer
        implements Closeable
{
    private static final Logger log = Logger.get(TestingScyllaServer.class);

    private static final int PORT = 9042;

    private static final Duration REFRESH_SIZE_ESTIMATES_TIMEOUT = new Duration(1, MINUTES);

    private final GenericContainer<?> container;
    private final CassandraSession session;

    public TestingScyllaServer()
    {
        this("2.2.0");
    }

    public TestingScyllaServer(String version)
    {
        container = new GenericContainer<>("scylladb/scylla:" + version)
                .withCommand("--smp", "1") // Limit SMP to run in a machine having many cores https://github.com/scylladb/scylla/issues/5638
                .withExposedPorts(PORT);
        container.start();

        Cluster.Builder clusterBuilder = Cluster.builder()
                .withProtocolVersion(V3)
                .withClusterName("TestCluster")
                .addContactPointsWithPorts(ImmutableList.of(
                        new InetSocketAddress(container.getContainerIpAddress(), container.getMappedPort(PORT))))
                .withMaxSchemaAgreementWaitSeconds(60);

        session = new CassandraSession(
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                new ReopeningCluster(clusterBuilder::build),
                new Duration(1, MINUTES));
    }

    public CassandraSession getSession()
    {
        return requireNonNull(session, "session is null");
    }

    public String getHost()
    {
        return container.getContainerIpAddress();
    }

    public int getPort()
    {
        return container.getMappedPort(PORT);
    }

    public void refreshSizeEstimates(String keyspace, String table)
            throws Exception
    {
        long deadline = System.nanoTime() + REFRESH_SIZE_ESTIMATES_TIMEOUT.roundTo(NANOSECONDS);
        while (System.nanoTime() - deadline < 0) {
            flushTable(keyspace, table);
            refreshSizeEstimates();
            List<SizeEstimate> sizeEstimates = getSession().getSizeEstimates(keyspace, table);
            if (!sizeEstimates.isEmpty()) {
                log.info("Size estimates for the table %s.%s have been refreshed successfully: %s", keyspace, table, sizeEstimates);
                return;
            }
            log.info("Size estimates haven't been refreshed as expected. Retrying ...");
            SECONDS.sleep(1);
        }
        throw new TimeoutException(format("Attempting to refresh size estimates for table %s.%s has timed out after %s", keyspace, table, REFRESH_SIZE_ESTIMATES_TIMEOUT));
    }

    private void flushTable(String keyspace, String table)
            throws Exception
    {
        container.execInContainer("nodetool", "flush", keyspace, table);
    }

    private void refreshSizeEstimates()
            throws Exception
    {
        container.execInContainer("nodetool", "refreshsizeestimates");
    }

    @Override
    public void close()
    {
        container.close();
    }
}
