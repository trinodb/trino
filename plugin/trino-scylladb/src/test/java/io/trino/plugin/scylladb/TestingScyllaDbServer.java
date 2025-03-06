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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.cassandra.CassandraServer;
import io.trino.plugin.cassandra.CassandraSession;
import io.trino.plugin.cassandra.ExtraColumnMetadata;
import io.trino.plugin.cassandra.SizeEstimate;
import org.testcontainers.scylladb.ScyllaDBContainer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_VERSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static io.trino.plugin.cassandra.CassandraTestingUtils.CASSANDRA_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestingScyllaDbServer
        implements CassandraServer
{
    private static final Logger log = Logger.get(TestingScyllaDbServer.class);

    private static final int PORT = 9042;

    private static final String VERSION = "6.2";

    private static final Duration REFRESH_SIZE_ESTIMATES_TIMEOUT = new Duration(1, MINUTES);

    private final ScyllaDBContainer container;
    private final CassandraSession session;

    public TestingScyllaDbServer()
    {
        this(VERSION);
    }

    public TestingScyllaDbServer(String version)
    {
        container = new ScyllaDBContainer("scylladb/scylla:" + version);
        container.start();

        ProgrammaticDriverConfigLoaderBuilder config = DriverConfigLoader.programmaticBuilder();
        config.withDuration(REQUEST_TIMEOUT, java.time.Duration.ofSeconds(12));
        config.withString(PROTOCOL_VERSION, ProtocolVersion.V3.name());
        config.withDuration(CONTROL_CONNECTION_AGREEMENT_TIMEOUT, java.time.Duration.ofSeconds(30));
        // allow the retrieval of metadata for the system keyspaces
        config.withStringList(METADATA_SCHEMA_REFRESHED_KEYSPACES, List.of());

        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
                .withApplicationName("TestCluster")
                .addContactPoint(new InetSocketAddress(this.container.getHost(), this.container.getMappedPort(PORT)))
                .withLocalDatacenter("datacenter1")
                .withConfigLoader(config.build());

        session = new CassandraSession(
                CASSANDRA_TYPE_MANAGER,
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                cqlSessionBuilder::build,
                new Duration(1, MINUTES));
    }

    @Override
    public CassandraSession getSession()
    {
        return session;
    }

    @Override
    public String getHost()
    {
        return container.getHost();
    }

    @Override
    public int getPort()
    {
        return container.getMappedPort(PORT);
    }

    @Override
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
        session.close();
        container.close();
    }
}
