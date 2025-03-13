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
package io.trino.plugin.iceberg.catalog.rest;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.JsonCodec;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.Network.NetworkImpl;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.Closeable;
import java.net.URI;
import java.util.Map;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class TestingLakekeeperCatalog
        implements Closeable
{
    public static final String DEFAULT_BUCKET = "examples";
    public static final String DEFAULT_WAREHOUSE = "lakekeeper";
    public static final String DEFAULT_PROJECT_ID = "00000000-0000-0000-0000-000000000000";

    public static final String LAKEKEEPER_DB_URL_WRITE_KEY = "LAKEKEEPER__PG_DATABASE_URL_WRITE";
    private static final String LAKEKEEPER_DB_URL_READ_KEY = "LAKEKEEPER__PG_DATABASE_URL_READ";
    public static final String LAKEKEEPER_PG_ENCRYPTION_KEY = "LAKEKEEPER__PG_ENCRYPTION_KEY";
    public static final String PG_ENCRYPTION_KEY = "This-is-NOT-Secure!";
    public static final String POSTGRES_IMAGE = "postgres:16";

    public static final String LAKEKEEPER_CATALOG_IMAGE = "quay.io/lakekeeper/catalog:v0.7.0";

    private static final int MINIO_PORT = 9000;
    private static final String MINIO_ALIAS = "minio-1";
    private static final int LAKEKEEPER_PORT = 8181;
    private static final HttpClient HTTP_CLIENT = new JettyHttpClient();

    private final String prefix;
    private final GenericContainer<?> lakekeeperCatalog;
    private final GenericContainer<?> migrator;
    private final PostgreSQLContainer<?> postgresqlContainer;
    private final GenericContainer<?> minio;
    private final Network network;

    public TestingLakekeeperCatalog()
    {
        this("1s");
    }

    public TestingLakekeeperCatalog(String taskQueuePollInterval)
    {
        network = NetworkImpl.SHARED;

        postgresqlContainer = new PostgreSQLContainer<>(POSTGRES_IMAGE);
        postgresqlContainer.withNetwork(network).start();

        String pgConnectionString = String.format("postgresql://%s:%s@%s:%d/%s",
                postgresqlContainer.getUsername(),
                postgresqlContainer.getPassword(),
                postgresqlContainer.getContainerName().substring(1),
                PostgreSQLContainer.POSTGRESQL_PORT,
                postgresqlContainer.getDatabaseName());

        migrator = new GenericContainer<>(LAKEKEEPER_CATALOG_IMAGE);
        migrator.withEnv(LAKEKEEPER_PG_ENCRYPTION_KEY, PG_ENCRYPTION_KEY)
                .withEnv(LAKEKEEPER_DB_URL_READ_KEY, pgConnectionString)
                .withEnv(LAKEKEEPER_DB_URL_WRITE_KEY, pgConnectionString).withNetwork(postgresqlContainer.getNetwork())
                .withCommand("migrate").start();

        minio = new GenericContainer<>("bitnami/minio:2025.2.18");
        minio.withEnv(Map.of("MINIO_ROOT_USER", MINIO_ACCESS_KEY,
                        "MINIO_ROOT_PASSWORD", MINIO_SECRET_KEY,
                        "MINIO_API_PORT_NUMBER", "%d".formatted(MINIO_PORT),
                        "MINIO_CONSOLE_PORT_NUMBER", "9001",
                        "MINIO_SCHEME", "http",
                        "MINIO_DEFAULT_BUCKETS", DEFAULT_BUCKET))
                .withNetwork(postgresqlContainer.getNetwork())
                .withNetworkAliases(MINIO_ALIAS)
                .waitingFor(Wait.forSuccessfulCommand("mc ls local | grep examples"))
                .withExposedPorts(MINIO_PORT, 9001).start();

        lakekeeperCatalog = new GenericContainer<>(LAKEKEEPER_CATALOG_IMAGE);
        lakekeeperCatalog.withEnv(Map.of(LAKEKEEPER_PG_ENCRYPTION_KEY, PG_ENCRYPTION_KEY,
                        LAKEKEEPER_DB_URL_READ_KEY, pgConnectionString,
                        LAKEKEEPER_DB_URL_WRITE_KEY, pgConnectionString,
                        "LAKEKEEPER__QUEUE_CONFIG__POLL_INTERVAL", String.format("\"%s\"", taskQueuePollInterval)))
                .withNetwork(postgresqlContainer.getNetwork())
                .withCommand("serve").withExposedPorts(LAKEKEEPER_PORT)
                .withExposedPorts(8181);
        try {
            lakekeeperCatalog.start();
            bootstrapServer();
            createCatalog();
            prefix = fetchPrefix();
        }
        catch (Exception e) {
            System.err.println(lakekeeperCatalog.getLogs());
            close();
            throw e;
        }
    }

    public void dropWithoutPurge(String schema, String table)
    {
        Request request = Request.Builder.prepareDelete()
                .setUri(URI.create(restUri() + "/catalog/v1/" + prefix + "/namespaces/" + schema + "/tables/" + table + "?purgeRequested=false"))
                .setHeader("Content-Type", "application/json")
                .build();
        StatusResponseHandler.StatusResponse sr = HTTP_CLIENT.execute(request, createStatusResponseHandler());
        if (sr.getStatusCode() != 204) {
            throw new RuntimeException("Failed to drop table. Status: " + sr.getStatusCode());
        }
    }

    public String getLogs()
    {
        return lakekeeperCatalog.getLogs();
    }

    public String restUri()
    {
        return "http://%s:%s".formatted(lakekeeperCatalog.getHost(), lakekeeperCatalog.getMappedPort(LAKEKEEPER_PORT));
    }

    @Override
    public void close()
    {
        migrator.close();
        lakekeeperCatalog.close();
        postgresqlContainer.close();
        minio.close();
        network.close();
    }

    public String externalMinioAddress()
    {
        return "http://localhost:%d".formatted(minioPort());
    }

    private int minioPort()
    {
        return this.minio.getMappedPort(MINIO_PORT);
    }

    private String fetchPrefix()
    {
        Request request = Request.Builder.prepareGet().setUri(URI.create(restUri() + "/catalog/v1/config?warehouse=" + DEFAULT_WAREHOUSE)).build();
        Map<String, Object> resp = HTTP_CLIENT.execute(request, createJsonResponseHandler(JsonCodec.mapJsonCodec(String.class, Object.class)));
        @SuppressWarnings("unchecked")
        Map<String, String> overrides = ((Map<String, String>) resp.get("overrides"));
        return requireNonNull(overrides.get("prefix"));
    }

    private void bootstrapServer()
    {
        @Language("JSON")
        String body = "{\"accept-terms-of-use\": true}";
        Request request = Request.Builder.preparePost()
                .setUri(URI.create(restUri() + "/management/v1/bootstrap"))
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    private void createCatalog()
    {
        String body = JsonCodec.jsonCodec(Map.class).toJson(createWarehousePayload());

        Request request = Request.Builder.preparePost()
                .setUri(URI.create(restUri() + "/management/v1/warehouse"))
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();

        StatusResponseHandler.StatusResponse response = HTTP_CLIENT.execute(
                request,
                createStatusResponseHandler());

        if (response.getStatusCode() != 201) {
            throw new RuntimeException("Failed to create catalog. Status: " + response.getStatusCode());
        }
    }

    private static Map<String, Object> createWarehousePayload()
    {
        return Map.of(
                "warehouse-name", DEFAULT_WAREHOUSE,
                "project-id", DEFAULT_PROJECT_ID,
                "storage-profile", storageProfile(),
                "storage-credential", storageCredential());
    }

    private static Map<String, Object> storageProfile()
    {
        return Map.of(
                "type", "s3",
                "bucket", DEFAULT_BUCKET,
                "endpoint", "http://%s:%d".formatted(MINIO_ALIAS, MINIO_PORT),
                "region", "local-01",
                "path-style-access", true,
                "flavor", "s3-compat",
                "sts-enabled", true);
    }

    private static Map<String, Object> storageCredential()
    {
        return Map.of(
                "type", "s3",
                "credential-type", "access-key",
                "aws-access-key-id", MINIO_ACCESS_KEY,
                "aws-secret-access-key", MINIO_SECRET_KEY);
    }
}
