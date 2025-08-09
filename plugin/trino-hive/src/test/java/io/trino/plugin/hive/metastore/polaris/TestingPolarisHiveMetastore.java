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
package io.trino.plugin.hive.metastore.polaris;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.ObjectMapperProvider;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.plugin.base.util.AutoCloseableCloser;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.util.Objects.requireNonNull;

/**
 * Testing utility for creating a PolarisHiveMetastore backed by a real Polaris container.
 * This provides comprehensive integration testing for the Polaris Hive metastore backend.
 */
public final class TestingPolarisHiveMetastore
        implements AutoCloseable
{
    private static final int POLARIS_PORT = 8181;

    private final GenericContainer<?> polarisContainer;
    private final String token;
    private final String hostWarehouseLocation;
    private final AutoCloseableCloser closer;

    private static final HttpClient HTTP_CLIENT = new JettyHttpClient();
    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    public TestingPolarisHiveMetastore(Path warehouseLocation, AutoCloseableCloser closer)
            throws Exception
    {
        requireNonNull(warehouseLocation, "warehouseLocation is null");
        requireNonNull(closer, "closer is null");
        this.closer = closer;
        this.hostWarehouseLocation = warehouseLocation.toAbsolutePath().toString();

        Files.createDirectories(warehouseLocation);
        warehouseLocation.toFile().setReadable(true, false);
        warehouseLocation.toFile().setWritable(true, false);
        warehouseLocation.toFile().setExecutable(true, false);

        polarisContainer = new GenericContainer<>("apache/polaris:1.0.0-incubating");
        polarisContainer.addExposedPort(POLARIS_PORT);
        polarisContainer.withFileSystemBind(hostWarehouseLocation, hostWarehouseLocation, BindMode.READ_WRITE);
        polarisContainer.waitingFor(new LogMessageWaitStrategy().withRegEx(".*Apache Polaris Server.* started.*"));

        polarisContainer.withEnv("POLARIS_BOOTSTRAP_CREDENTIALS", "default-realm,root,s3cr3t");
        polarisContainer.withEnv("polaris.realm-context.realms", "default-realm");
        polarisContainer.withEnv("polaris.readiness.ignore-severe-issues", "true");
        polarisContainer.withEnv("polaris.features.\"SUPPORTED_CATALOG_STORAGE_TYPES\"", "[\"FILE\"]");
        polarisContainer.withEnv("polaris.features.\"ALLOW_INSECURE_STORAGE_TYPES\"", "true");
        polarisContainer.withEnv("polaris.features.\"DROP_WITH_PURGE_ENABLED\"", "true");

        polarisContainer.start();
        closer.register(polarisContainer);

        try {
            token = getToken();
            createCatalog();
            grantPrivilege();
        }
        catch (Exception e) {
            throw new UncheckedIOException("Failed to initialize Polaris catalog", new IOException(e));
        }
    }

    /**
     * Creates a TestingPolarisHiveMetastore with a temporary warehouse directory.
     */
    public static TestingPolarisHiveMetastore create(Consumer<AutoCloseable> registerResource)
    {
        try {
            Path tempDir = java.nio.file.Files.createTempDirectory("polaris-warehouse");
            AutoCloseableCloser closer = AutoCloseableCloser.create();
            TestingPolarisHiveMetastore metastore = new TestingPolarisHiveMetastore(tempDir, closer);

            registerResource.accept(metastore);
            registerResource.accept(closer);

            return metastore;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create TestingPolarisHiveMetastore", e);
        }
    }

    /**
     * Creates a TestingPolarisHiveMetastore with the specified warehouse directory.
     */
    public static TestingPolarisHiveMetastore create(Path warehouseDir, Consumer<AutoCloseable> registerResource)
    {
        checkState(exists(warehouseDir), "%s does not exist", warehouseDir);
        checkState(isDirectory(warehouseDir), "%s is not a directory", warehouseDir);

        try {
            AutoCloseableCloser closer = AutoCloseableCloser.create();
            TestingPolarisHiveMetastore metastore = new TestingPolarisHiveMetastore(warehouseDir, closer);
            registerResource.accept(metastore);
            registerResource.accept(closer);

            return metastore;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to create TestingPolarisHiveMetastore", e);
        }
    }

    /**
     * Creates a PolarisHiveMetastore configured to connect to this test container.
     */
    public PolarisHiveMetastore createHiveMetastore()
    {
        PolarisMetastoreConfig config = new PolarisMetastoreConfig()
                .setUri(URI.create(getRestUri() + "/api/catalog"))
                .setPrefix("polaris");

        // Create RESTSessionCatalog similar to PolarisMetastoreModule
        SecurityProperties securityProperties = () -> ImmutableMap.<String, String>builder()
                .put("credential", "root:s3cr3t")
                .put("scope", "PRINCIPAL_ROLE:ALL")
                .put("oauth2-server-uri", getRestUri() + "/api/catalog/v1/oauth/tokens")
                .buildOrThrow();

        // DEBUG: Show what security setup the test is using
        System.out.println("=== TEST SECURITY SETUP DEBUG ===");
        System.out.println("Test SecurityProperties keys: " + securityProperties.get().keySet());
        System.out.println("Using OAuth2 server URI: " + (getRestUri() + "/api/catalog/v1/oauth/tokens"));
        System.out.println("=== END TEST SECURITY SETUP DEBUG ===");
        AwsProperties awsProperties = new DefaultAwsProperties();

        // Configure properties for RESTSessionCatalog matching what the smoke test uses
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("uri", getRestUri() + "/api/catalog")
                .put("prefix", "polaris")
                .put("warehouse", "polaris");

        // Add security and AWS properties
        properties.putAll(securityProperties.get());
        properties.putAll(awsProperties.get());

        ObjectMapper objectMapper = new ObjectMapperProvider().get();

        PolarisRestClient restClient = new PolarisRestClient(
                HTTP_CLIENT,
                config,
                securityProperties,
                objectMapper);

        // Create RESTSessionCatalog directly for testing - simpler than factory pattern
        // This avoids complex dependency chains that cause issues in test environments
        RESTSessionCatalog restSessionCatalog = new RESTSessionCatalog(
                httpConfig -> HTTPClient.builder(httpConfig)
                        .uri(httpConfig.get("uri"))
                        .withHeaders(Map.of())
                        .build(),
                (context, ioConfig) -> {
                    Path warehousePath = Path.of(hostWarehouseLocation);
                    return new ForwardingFileIo(new LocalFileSystem(warehousePath), ioConfig);
                });

        restSessionCatalog.initialize("polaris", properties.buildOrThrow());

        return new PolarisHiveMetastore(
                restClient,
                restSessionCatalog,
                securityProperties,
                new PolarisMetastoreStats());
    }

    /**
     * Returns the REST URI for connecting to the Polaris container.
     */
    public String getRestUri()
    {
        return "http://%s:%s".formatted(polarisContainer.getHost(), polarisContainer.getMappedPort(POLARIS_PORT));
    }

    /**
     * Returns the warehouse path that should be used by both Polaris and Trino.
     * This is the same as the host path since we use same-path mounting.
     */
    public String getWarehousePath()
    {
        return hostWarehouseLocation;
    }

    /**
     * Direct REST API call to drop table, similar to Iceberg's TestingPolarisCatalog.dropTable()
     * This provides a reliable way to clean up tables during testing
     */
    public void dropTable(String schema, String table)
    {
        try {
            Request request = Request.Builder.prepareDelete()
                    .setUri(URI.create(getRestUri() + "/api/catalog/v1/polaris/namespaces/" + schema + "/tables/" + table))
                    .setHeader("Authorization", "Bearer " + token)
                    .setHeader("Content-Type", "application/json")
                    .build();
            HTTP_CLIENT.execute(request, createStatusResponseHandler());
        }
        catch (Exception e) {
            // Ignore failures during cleanup - table might not exist
        }
    }

    private String getToken()
            throws Exception
    {
        String body = "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL";
        Request request = preparePost()
                .setUri(URI.create(getRestUri() + "/api/catalog/v1/oauth/tokens"))
                .setHeader("Polaris-Realm", "default-realm")
                .setHeader("Content-Type", "application/x-www-form-urlencoded")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        StringResponseHandler.StringResponse response = HTTP_CLIENT.execute(request, createStringResponseHandler());
        try {
            return OBJECT_MAPPER.readTree(response.getBody()).get("access_token").asText();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createCatalog()
            throws Exception
    {
        @Language("JSON")
        String body = "{" +
                "\"name\": \"polaris\"," +
                "\"id\": 1," +
                "\"type\": \"INTERNAL\"," +
                "\"readOnly\": false, " +
                "\"storageConfigInfo\": {\"storageType\": \"FILE\", \"allowedLocations\":[\"" + hostWarehouseLocation + "\"]}, " +
                "\"properties\": {\"default-base-location\": \"file://" + hostWarehouseLocation + "\"}" +
                "}";
        Request request = preparePost()
                .setUri(URI.create(getRestUri() + "/api/management/v1/catalogs"))
                .setHeader("Authorization", "Bearer " + token)
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        StatusResponseHandler.StatusResponse response = HTTP_CLIENT.execute(request, createStatusResponseHandler());
        checkState(response.getStatusCode() == 201, "Failed to create polaris catalog, status code: %s", response.getStatusCode());
    }

    private void grantPrivilege()
            throws Exception
    {
        String[] privileges = {
            "TABLE_READ_DATA",
            "TABLE_WRITE_DATA",
            "TABLE_CREATE",
            "TABLE_DROP",
            "CATALOG_MANAGE_CONTENT"
        };

        for (String privilege : privileges) {
            @Language("JSON")
            String body = "{\"grant\": {\"type\": \"catalog\", \"privilege\": \"" + privilege + "\"}}";
            Request request = preparePost()
                    .setUri(URI.create(getRestUri() + "/api/management/v1/catalogs/polaris/catalog-roles/catalog_admin/grants"))
                    .setHeader("Authorization", "Bearer " + token)
                    .setHeader("Content-Type", "application/json")
                    .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                    .build();
            StatusResponseHandler.StatusResponse response = HTTP_CLIENT.execute(request, createStatusResponseHandler());
            checkState(response.getStatusCode() == 201, "Failed to grant privilege %s, status code: %s", privilege, response.getStatusCode());
        }
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
