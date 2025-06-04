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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_VERSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.cassandra.CassandraTestingUtils.CASSANDRA_TYPE_MANAGER;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.writeString;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class CassandraServer
        implements Closeable
{
    private static final Logger log = Logger.get(CassandraServer.class);
    private static final Duration REFRESH_SIZE_ESTIMATES_TIMEOUT = new Duration(1, MINUTES);

    private final CassandraContainer dockerContainer;
    private final CassandraSession session;

    public CassandraServer()
            throws Exception
    {
        this("cassandra:3.0", "cu-cassandra.yaml");
    }

    public CassandraServer(String imageName, String configFileName)
            throws Exception
    {
        this(DockerImageName.parse(imageName), ImmutableMap.of(), "/etc/cassandra/cassandra.yaml", configFileName);
    }

    @SuppressWarnings("deprecation")
    public CassandraServer(DockerImageName imageName, Map<String, String> environmentVariables, String configPath, String configFileName)
            throws Exception
    {
        log.debug("Starting cassandra...");

        this.dockerContainer = new CassandraContainer(imageName)
                .withCopyFileToContainer(forHostPath(prepareCassandraYaml(configFileName)), configPath)
                .withEnv(environmentVariables)
                .withStartupTimeout(java.time.Duration.ofMinutes(10))
                // TODO: https://github.com/testcontainers/testcontainers-java/issues/9337
                .waitingFor(new CassandraQueryWaitStrategy());
        this.dockerContainer.start();

        ProgrammaticDriverConfigLoaderBuilder driverConfigLoaderBuilder = DriverConfigLoader.programmaticBuilder();
        driverConfigLoaderBuilder.withDuration(REQUEST_TIMEOUT, java.time.Duration.ofSeconds(30));
        driverConfigLoaderBuilder.withString(PROTOCOL_VERSION, ProtocolVersion.V3.name());
        driverConfigLoaderBuilder.withDuration(CONTROL_CONNECTION_AGREEMENT_TIMEOUT, java.time.Duration.ofSeconds(30));
        // allow the retrieval of metadata for the system keyspaces
        driverConfigLoaderBuilder.withStringList(METADATA_SCHEMA_REFRESHED_KEYSPACES, List.of());

        CqlSessionBuilder cqlSessionBuilder = CqlSession.builder()
                .addContactPoint(dockerContainer.getContactPoint())
                .withLocalDatacenter(dockerContainer.getLocalDatacenter())
                .withConfigLoader(driverConfigLoaderBuilder.build());

        session = new CassandraSession(
                CASSANDRA_TYPE_MANAGER,
                JsonCodec.listJsonCodec(ExtraColumnMetadata.class),
                cqlSessionBuilder::build,
                new Duration(1, MINUTES));
    }

    private static String prepareCassandraYaml(String fileName)
            throws IOException
    {
        String original = Resources.toString(getResource(fileName), UTF_8);

        Path tmpDirPath = createTempDirectory(null);
        Path dataDir = tmpDirPath.resolve("data");
        createDirectory(dataDir);

        String modified = original.replaceAll("\\$\\{data_directory\\}", dataDir.toAbsolutePath().toString());

        File yamlFile = tmpDirPath.resolve(fileName).toFile();
        yamlFile.deleteOnExit();
        writeString(yamlFile.toPath(), modified, UTF_8);

        return yamlFile.getAbsolutePath();
    }

    public CassandraSession getSession()
    {
        return session;
    }

    public String getHost()
    {
        return dockerContainer.getHost();
    }

    public int getPort()
    {
        return dockerContainer.getContactPoint().getPort();
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
                log.debug("Size estimates for the table %s.%s have been refreshed successfully: %s", keyspace, table, sizeEstimates);
                return;
            }
            log.debug("Size estimates haven't been refreshed as expected. Retrying ...");
            SECONDS.sleep(1);
        }
        throw new TimeoutException(format("Attempting to refresh size estimates for table %s.%s has timed out after %s", keyspace, table, REFRESH_SIZE_ESTIMATES_TIMEOUT));
    }

    private void flushTable(String keyspace, String table)
            throws Exception
    {
        dockerContainer.execInContainer("nodetool", "flush", keyspace, table);
    }

    private void refreshSizeEstimates()
            throws Exception
    {
        dockerContainer.execInContainer("nodetool", "refreshsizeestimates");
    }

    @Override
    public void close()
    {
        session.close();
        dockerContainer.close();
    }
}
