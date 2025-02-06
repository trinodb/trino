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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.plugin.pinot.auth.password.PinotPasswordAuthenticationProvider;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.Resources.getResource;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.utils.http.HttpClient.DEFAULT_SOCKET_TIMEOUT_MS;
import static org.testcontainers.utility.DockerImageName.parse;

public class TestingPinotCluster
        implements Closeable
{
    public static final String PINOT_LATEST_IMAGE_NAME = "apachepinot/pinot:1.2.0";
    private static final String ZOOKEEPER_INTERNAL_HOST = "zookeeper";
    private static final JsonCodec<List<String>> LIST_JSON_CODEC = listJsonCodec(String.class);
    private static final JsonCodec<PinotSuccessResponse> PINOT_SUCCESS_RESPONSE_JSON_CODEC = jsonCodec(PinotSuccessResponse.class);
    private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();

    public static final int ZOOKEEPER_PORT = 2181;
    public static final int CONTROLLER_PORT = 9000;
    public static final int BROKER_PORT = 8099;
    public static final int SERVER_ADMIN_PORT = 8097;
    public static final int SERVER_PORT = 8098;
    public static final int GRPC_PORT = 8090;

    private final GenericContainer<?> controller;
    private final GenericContainer<?> broker;
    private final GenericContainer<?> server;
    private final GenericContainer<?> zookeeper;
    private final HttpClient httpClient;
    private final Closer closer = Closer.create();
    private final boolean secured;

    public TestingPinotCluster(String version, Network network, boolean secured)
    {
        httpClient = closer.register(new JettyHttpClient());
        zookeeper = new GenericContainer<>(parse("zookeeper:3.9"))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withNetworkAliases(ZOOKEEPER_INTERNAL_HOST)
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(ZOOKEEPER_PORT))
                .withExposedPorts(ZOOKEEPER_PORT);
        closer.register(zookeeper::stop);

        String controllerConfig = secured ? "/var/pinot/controller/config/pinot-controller-secured.conf" : "/var/pinot/controller/config/pinot-controller.conf";
        controller = new GenericContainer<>(parse(version))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withClasspathResourceMapping("/pinot-controller", "/var/pinot/controller/config", BindMode.READ_ONLY)
                .withEnv("JAVA_OPTS", "-Xmx512m -Dlog4j2.configurationFile=/opt/pinot/conf/pinot-controller-log4j2.xml -Dplugins.dir=/opt/pinot/plugins")
                .withCommand("StartController", "-configFileName", controllerConfig)
                .withNetworkAliases("pinot-controller", "localhost")
                .withExposedPorts(CONTROLLER_PORT);
        closer.register(controller::stop);

        String brokerConfig = secured ? "/var/pinot/broker/config/pinot-broker-secured.conf" : "/var/pinot/broker/config/pinot-broker.conf";
        broker = new GenericContainer<>(parse(version))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withClasspathResourceMapping("/pinot-broker", "/var/pinot/broker/config", BindMode.READ_ONLY)
                .withEnv("JAVA_OPTS", "-Xmx512m -Dlog4j2.configurationFile=/opt/pinot/conf/pinot-broker-log4j2.xml -Dplugins.dir=/opt/pinot/plugins")
                .withCommand("StartBroker", "-clusterName", "pinot", "-zkAddress", getZookeeperInternalHostPort(), "-configFileName", brokerConfig)
                .withNetworkAliases("pinot-broker", "localhost")
                .withExposedPorts(BROKER_PORT);
        closer.register(broker::stop);

        String serverConfig = secured ? "/var/pinot/server/config/pinot-server-secured.conf" : "/var/pinot/server/config/pinot-server.conf";
        server = new GenericContainer<>(parse(version))
                .withStartupAttempts(3)
                .withNetwork(network)
                .withClasspathResourceMapping("/pinot-server", "/var/pinot/server/config", BindMode.READ_ONLY)
                .withEnv("JAVA_OPTS", "-Xmx512m -Dlog4j2.configurationFile=/opt/pinot/conf/pinot-server-log4j2.xml -Dplugins.dir=/opt/pinot/plugins")
                .withCommand("StartServer", "-clusterName", "pinot", "-zkAddress", getZookeeperInternalHostPort(), "-configFileName", serverConfig)
                .withNetworkAliases("pinot-server", "localhost")
                .withExposedPorts(SERVER_PORT, SERVER_ADMIN_PORT, GRPC_PORT);
        closer.register(server::stop);

        this.secured = secured;
    }

    public void start()
    {
        zookeeper.start();
        controller.start();
        broker.start();
        server.start();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    private static String getZookeeperInternalHostPort()
    {
        return format("%s:%s", ZOOKEEPER_INTERNAL_HOST, ZOOKEEPER_PORT);
    }

    public String getControllerConnectString()
    {
        return controller.getHost() + ":" + controller.getMappedPort(CONTROLLER_PORT);
    }

    public HostAndPort getBrokerHostAndPort()
    {
        return HostAndPort.fromParts(broker.getHost(), broker.getMappedPort(BROKER_PORT));
    }

    public HostAndPort getServerHostAndPort()
    {
        return HostAndPort.fromParts(server.getHost(), server.getMappedPort(SERVER_PORT));
    }

    public HostAndPort getServerGrpcHostAndPort()
    {
        return HostAndPort.fromParts(server.getHost(), server.getMappedPort(GRPC_PORT));
    }

    public void createSchema(String resourceName, String tableName)
            throws Exception
    {
        try (InputStream stream = getResource(resourceName).openStream()) {
            byte[] bytes = stream.readAllBytes();
            Request request = Request.Builder.preparePost()
                    .setUri(getControllerUri("schemas"))
                    .setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON)
                    .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                    .addHeader(HttpHeaders.AUTHORIZATION, secured ? controllerAuthToken() : "")
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(bytes))
                    .build();

            PinotSuccessResponse response = doWithRetries(() -> httpClient.execute(request, createJsonResponseHandler(PINOT_SUCCESS_RESPONSE_JSON_CODEC)), 10);
            checkState(response.getStatus().equals(format("%s successfully added", tableName)), "Unexpected response: '%s'", response.getStatus());
            verifySchema(tableName);
        }
    }

    private URI getControllerUri(String path)
    {
        return URI.create(format("http://%s/%s", getControllerConnectString(), path));
    }

    private void verifySchema(String tableName)
            throws Exception
    {
        Request request = Request.Builder.prepareGet().setUri(getControllerUri("schemas"))
                .setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON)
                .addHeader(HttpHeaders.AUTHORIZATION, secured ? controllerAuthToken() : "")
                .build();
        doWithRetries(() -> {
            List<String> schemas = httpClient.execute(request, createJsonResponseHandler(LIST_JSON_CODEC));
            checkState(schemas.contains(tableName), format("Schema for '%s' not found", tableName));
            return null;
        }, 10);
    }

    public void addRealTimeTable(String resourceName, String tableName)
            throws Exception
    {
        try (InputStream stream = getResource(resourceName).openStream()) {
            byte[] bytes = stream.readAllBytes();
            Request request = Request.Builder.preparePost()
                    .setUri(getControllerUri("tables"))
                    .setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON)
                    .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                    .addHeader(HttpHeaders.AUTHORIZATION, secured ? controllerAuthToken() : "")
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(bytes))
                    .build();

            PinotSuccessResponse response = doWithRetries(() -> httpClient.execute(request, createJsonResponseHandler(PINOT_SUCCESS_RESPONSE_JSON_CODEC)), 10);
            checkState(response.getStatus().startsWith(format("Table %s_REALTIME successfully added", tableName)), "Unexpected response: '%s'", response.getStatus());
        }
    }

    public void addOfflineTable(String resourceName, String tableName)
            throws Exception
    {
        try (InputStream stream = getResource(resourceName).openStream()) {
            byte[] bytes = stream.readAllBytes();
            Request request = Request.Builder.preparePost()
                    .setUri(getControllerUri("tables"))
                    .setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON)
                    .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                    .addHeader(HttpHeaders.AUTHORIZATION, secured ? controllerAuthToken() : "")
                    .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(bytes))
                    .build();

            PinotSuccessResponse response = doWithRetries(() -> httpClient.execute(request, createJsonResponseHandler(PINOT_SUCCESS_RESPONSE_JSON_CODEC)), 10);
            checkState(response.getStatus().startsWith(format("Table %s_OFFLINE successfully added", tableName)), "Unexpected response: '%s'", response.getStatus());
        }
    }

    public void publishOfflineSegment(String tableName, Path segmentPath)
    {
        try {
            String rawTableName = TableNameBuilder.extractRawTableName(tableName);
            String fileName = segmentPath.toFile().getName();
            checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
            String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
            List<NameValuePair> parameters = ImmutableList.of(new BasicNameValuePair(FileUploadDownloadClient.QueryParameters.TABLE_NAME, rawTableName));
            List<Header> headers = ImmutableList.of(new BasicHeader(HttpHeaders.AUTHORIZATION, secured ? controllerAuthToken() : ""));
            RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(() -> {
                try (InputStream inputStream = Files.newInputStream(segmentPath)) {
                    SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT.uploadSegment(
                            getControllerUri("v2/segments"),
                            segmentName,
                            inputStream,
                            headers,
                            parameters,
                            DEFAULT_SOCKET_TIMEOUT_MS);
                    // TODO: {"status":"Successfully uploaded segment: myTable2_2020-09-09_2020-09-09 of table: myTable2"}
                    checkState(response.getStatusCode() == 200, "Unexpected response: '%s'", response.getResponse());
                    return true;
                }
                catch (HttpErrorStatusException e) {
                    int statusCode = e.getStatusCode();
                    if (statusCode >= 500) {
                        return false;
                    }
                    throw e;
                }
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                Files.deleteIfExists(segmentPath);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static <T> T doWithRetries(Supplier<T> supplier, int retries)
            throws Exception
    {
        Exception exception = null;
        for (int retry = 0; retry < retries; retry++) {
            try {
                return supplier.get();
            }
            catch (Exception t) {
                exception = t;
            }
            Thread.sleep(1000);
        }
        throw exception;
    }

    private static String controllerAuthToken()
    {
        // Secrets defined in pinot-controller-secured.conf
        return PinotPasswordAuthenticationProvider.encode("admin", "verysecret");
    }

    public static class PinotSuccessResponse
    {
        private final String status;

        @JsonCreator
        public PinotSuccessResponse(@JsonProperty("status") String status)
        {
            this.status = requireNonNull(status, "status is null");
        }

        @JsonProperty
        public String getStatus()
        {
            return status;
        }
    }
}
