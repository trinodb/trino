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
package io.trino.plugin.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Closer;
import com.google.common.io.MoreFiles;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.utility.MountableFile.forClasspathResource;
import static org.testcontainers.utility.MountableFile.forHostPath;

public class TestingDruidServer
        implements Closeable
{
    private final String hostWorkingDirectory;
    private final GenericContainer<?> broker;
    private final GenericContainer<?> coordinator;
    private final GenericContainer<?> historical;
    private final GenericContainer<?> middleManager;
    private final GenericContainer<?> zookeeper;
    private final OkHttpClient httpClient;
    private final Network network;

    private static final int DRUID_COORDINATOR_PORT = 8081;
    private static final int DRUID_BROKER_PORT = 8082;
    private static final int DRUID_HISTORICAL_PORT = 8083;
    private static final int DRUID_MIDDLE_MANAGER_PORT = 8091;

    private static final String DRUID_DOCKER_IMAGE = "apache/druid:0.18.0";

    public TestingDruidServer()
    {
        this(DRUID_DOCKER_IMAGE);
    }

    public TestingDruidServer(String dockerImageName)
    {
        try {
            // Cannot use Files.createTempDirectory() because on Mac by default it uses
            // /var/folders/ which is not visible to Docker for Mac
            hostWorkingDirectory = Files.createDirectory(
                    Paths.get("/tmp/docker-tests-files-" + randomUUID().toString()))
                    .toAbsolutePath().toString();
            File f = new File(hostWorkingDirectory);
            // Enable read/write/exec access for the services running in containers
            f.setWritable(true, false);
            f.setReadable(true, false);
            f.setExecutable(true, false);
            this.httpClient = new OkHttpClient();
            network = Network.newNetwork();
            this.zookeeper = new GenericContainer<>("zookeeper")
                    .withNetwork(network)
                    .withNetworkAliases("zookeeper")
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .waitingFor(Wait.forListeningPort());
            zookeeper.start();

            this.coordinator = new GenericContainer<>(dockerImageName)
                    .withExposedPorts(DRUID_COORDINATOR_PORT)
                    .withNetwork(network)
                    .withCommand("coordinator")
                    .withWorkingDirectory("/opt/druid")
                    .withFileSystemBind(hostWorkingDirectory, "/opt/druid/var", BindMode.READ_WRITE)
                    .dependsOn(zookeeper)
                    .withCopyFileToContainer(
                            forClasspathResource("common.runtime.properties"),
                            "/opt/druid/conf/druid/cluster/_common/common.runtime.properties")
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .withCopyFileToContainer(
                            forClasspathResource("druid-coordinator.config"),
                            "/opt/druid/conf/druid/cluster/master/coordinator-overlord/runtime.properties")
                    .withCopyFileToContainer(
                            forClasspathResource("druid-coordinator-jvm.config"),
                            "/opt/druid/conf/druid/cluster/master/coordinator-overlord/jvm.config")
                    .waitingFor(Wait.forHttp("/status/selfDiscovered"));
            coordinator.start();

            this.broker = new GenericContainer<>(DRUID_DOCKER_IMAGE)
                    .withExposedPorts(DRUID_BROKER_PORT)
                    .withNetwork(network)
                    .withCommand("broker")
                    .withWorkingDirectory("/opt/druid")
                    .dependsOn(zookeeper, coordinator)
                    .withFileSystemBind(hostWorkingDirectory, "/opt/druid/var", BindMode.READ_WRITE)
                    .withCopyFileToContainer(
                            forClasspathResource("common.runtime.properties"),
                            "/opt/druid/conf/druid/cluster/_common/common.runtime.properties")
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .withCopyFileToContainer(
                            forClasspathResource("broker.config"),
                            "/opt/druid/conf/druid/cluster/query/broker/runtime.properties")
                    .withCopyFileToContainer(
                            forClasspathResource("broker-jvm.config"),
                            "/opt/druid/conf/druid/cluster/query/broker/jvm.config")
                    .waitingFor(Wait.forHttp("/status/selfDiscovered"));
            broker.start();

            this.historical = new GenericContainer<>(DRUID_DOCKER_IMAGE)
                    .withExposedPorts(DRUID_HISTORICAL_PORT)
                    .withNetwork(network)
                    .withCommand("historical")
                    .withWorkingDirectory("/opt/druid")
                    .dependsOn(zookeeper, coordinator)
                    .withFileSystemBind(hostWorkingDirectory, "/opt/druid/var", BindMode.READ_WRITE)
                    .withCopyFileToContainer(
                            forClasspathResource("common.runtime.properties"),
                            "/opt/druid/conf/druid/cluster/_common/common.runtime.properties")
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .withCopyFileToContainer(
                            forClasspathResource("historical.config"),
                            "/opt/druid/conf/druid/cluster/data/historical/runtime.properties")
                    .withCopyFileToContainer(
                            forClasspathResource("historical-jvm.config"),
                            "/opt/druid/conf/druid/cluster/data/historical/jvm.config")
                    .waitingFor(Wait.forHttp("/status/selfDiscovered"));
            historical.start();

            this.middleManager = new GenericContainer<>(DRUID_DOCKER_IMAGE)
                    .withExposedPorts(DRUID_MIDDLE_MANAGER_PORT)
                    .withNetwork(network)
                    .withCommand("middleManager")
                    .withWorkingDirectory("/opt/druid")
                    .dependsOn(zookeeper, coordinator)
                    .withFileSystemBind(hostWorkingDirectory, "/opt/druid/var", BindMode.READ_WRITE)
                    .withCopyFileToContainer(
                            forClasspathResource("common.runtime.properties"),
                            "/opt/druid/conf/druid/cluster/_common/common.runtime.properties")
                    .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
                    .withCopyFileToContainer(
                            forClasspathResource("middleManager.config"),
                            "/opt/druid/conf/druid/cluster/data/middleManager/runtime.properties")
                    .withCopyFileToContainer(
                            forClasspathResource("middleManager-jvm.config"),
                            "/opt/druid/conf/druid/cluster/data/middleManager/jvm.config")
                    .waitingFor(Wait.forHttp("/status/selfDiscovered"));
            middleManager.start();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getHostWorkingDirectory()
    {
        return hostWorkingDirectory;
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> MoreFiles.deleteRecursively(Paths.get(hostWorkingDirectory), ALLOW_INSECURE));
            closer.register(broker::stop);
            closer.register(historical::stop);
            closer.register(middleManager::stop);
            closer.register(coordinator::stop);
            closer.register(zookeeper::stop);
            closer.register(network::close);
        }
        catch (FileSystemException e) {
            // Unfortunately, on CI environment, the user running file deletion runs into
            // access issues. However, the MoreFiles.deleteRecursively procedure may run successfully
            // on developer environments. So it makes sense to still have it so that cruft
            // isn't left behind.
            // TODO: Once access issues are resolved, remove this catch block.
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getJdbcUrl()
    {
        return getJdbcUrl(broker.getMappedPort(DRUID_BROKER_PORT));
    }

    public void execute(String sql)
    {
        try (Connection connection = DriverManager.getConnection(getJdbcUrl());
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int getCoordinatorOverlordPort()
    {
        return coordinator.getMappedPort(DRUID_COORDINATOR_PORT);
    }

    private static String getJdbcUrl(int port)
    {
        return format("jdbc:avatica:remote:url=http://localhost:%s/druid/v2/sql/avatica/", port);
    }

    void ingestData(String datasource, String indexTask, String dataFilePath)
            throws IOException, InterruptedException
    {
        middleManager.withCopyFileToContainer(forHostPath(dataFilePath),
                getMiddleManagerContainerPathForDataFile(dataFilePath));

        Request.Builder requestBuilder = new Request.Builder();
        requestBuilder.addHeader("content-type", "application/json;charset=utf-8")
                .url("http://localhost:" + getCoordinatorOverlordPort() + "/druid/indexer/v1/task")
                .post(RequestBody.create(null, indexTask));
        Request ingestionRequest = requestBuilder.build();
        try (Response ignored = httpClient.newCall(ingestionRequest).execute()) {
            assertThat(checkDatasourceAvailable(datasource)).as("Datasource %s not loaded", datasource).isTrue();
        }
    }

    private boolean checkDatasourceAvailable(String datasource)
            throws IOException, InterruptedException
    {
        Map<String, Double> datasourceAvailabilityDetails;
        boolean datasourceNotLoaded = true;
        int attempts = 10;
        while (datasourceNotLoaded && attempts > 0) {
            Request.Builder requestBuilder = new Request.Builder();
            requestBuilder.url("http://localhost:" + getCoordinatorOverlordPort() + "/druid/coordinator/v1/loadstatus")
                    .get();
            Request datasourceAvailabilityRequest = requestBuilder.build();
            try (Response response = httpClient.newCall(datasourceAvailabilityRequest).execute()) {
                ObjectMapper mapper = new ObjectMapper();
                datasourceAvailabilityDetails = mapper.readValue(response.body().string(), Map.class);
                datasourceNotLoaded = datasourceAvailabilityDetails.get(datasource) == null || Double.compare(datasourceAvailabilityDetails.get(datasource), 100.0) < 0;
                if (datasourceNotLoaded) {
                    attempts--;
                    // Wait for some time since it can take a while for coordinator to load the ingested segments
                    Thread.sleep(15000);
                }
            }
        }
        return !datasourceNotLoaded;
    }

    private static String getMiddleManagerContainerPathForDataFile(String dataFilePath)
    {
        return "/opt/druid/var/" + dataFilePath;
    }
}
