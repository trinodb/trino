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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StringResponseHandler.StringResponse;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.json.ObjectMapperProvider;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;

import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.trino.testing.TestingProperties.getDockerImagesVersion;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class TestingPolarisCatalog
        implements Closeable
{
    public static final String WAREHOUSE = "polaris";
    private static final int POLARIS_PORT = 8181;
    public static final String CREDENTIAL = "root:s3cr3t";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final HttpClient HTTP_CLIENT = new JettyHttpClient();

    private final GenericContainer<?> polarisCatalog;
    private final String token;
    private final String warehouseLocation;

    public TestingPolarisCatalog(String warehouseLocation)
    {
        this.warehouseLocation = requireNonNull(warehouseLocation, "warehouseLocation is null");

        // TODO: Use the official docker image once Polaris community provides it
        polarisCatalog = new GenericContainer<>("ghcr.io/trinodb/testing/polaris-catalog:" + getDockerImagesVersion());
        polarisCatalog.addExposedPort(POLARIS_PORT);
        polarisCatalog.withFileSystemBind(warehouseLocation, warehouseLocation, BindMode.READ_WRITE);
        polarisCatalog.waitingFor(new LogMessageWaitStrategy().withRegEx(".*Apache Polaris Server.* started.*"));
        polarisCatalog.withEnv("POLARIS_BOOTSTRAP_CREDENTIALS", "default-realm,root,s3cr3t");
        polarisCatalog.withEnv("polaris.realm-context.realms", "default-realm");
        polarisCatalog.withCommand("java", "-jar", "polaris-quarkus-server-1.0.0-incubating-SNAPSHOT/quarkus-run.jar");
        polarisCatalog.start();

        token = getToken();
        createCatalog();
        grantPrivilege();
    }

    private String getToken()
    {
        String body = "grant_type=client_credentials&client_id=root&client_secret=s3cr3t&scope=PRINCIPAL_ROLE:ALL";
        Request request = Request.Builder.preparePost()
                .setUri(URI.create(restUri() + "/api/catalog/v1/oauth/tokens"))
                .setHeader("Polaris-Realm", "default-realm")
                .setHeader("Content-Type", "application/x-www-form-urlencoded")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        StringResponse response = HTTP_CLIENT.execute(request, createStringResponseHandler());
        try {
            return OBJECT_MAPPER.readTree(response.getBody()).get("access_token").asText();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void createCatalog()
    {
        @Language("JSON")
        String body = "{" +
                "\"name\": \"polaris\"," +
                "\"id\": 1," +
                "\"type\": \"INTERNAL\"," +
                "\"readOnly\": false, " +
                "\"storageConfigInfo\": {\"storageType\": \"FILE\"}, \"properties\": {\"default-base-location\": \"file://" + warehouseLocation + "\"}" +
                "}";
        Request request = Request.Builder.preparePost()
                .setUri(URI.create(restUri() + "/api/management/v1/catalogs"))
                .setHeader("Authorization", "Bearer " + token)
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    private void grantPrivilege()
    {
        @Language("JSON")
        String body = "{\"grant\": {\"type\": \"catalog\", \"privilege\": \"TABLE_WRITE_DATA\"}}";
        Request request = Request.Builder.preparePut()
                .setUri(URI.create(restUri() + "/api/management/v1/catalogs/polaris/catalog-roles/catalog_admin/grants"))
                .setHeader("Authorization", "Bearer " + token)
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    public void dropTable(String schema, String table)
    {
        Request request = Request.Builder.prepareDelete()
                .setUri(URI.create(restUri() + "/api/catalog/v1/polaris/namespaces/" + schema + "/tables/" + table))
                .setHeader("Authorization", "Bearer " + token)
                .setHeader("Content-Type", "application/json")
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    public String restUri()
    {
        return "http://%s:%s".formatted(polarisCatalog.getHost(), polarisCatalog.getMappedPort(POLARIS_PORT));
    }

    @Override
    public void close()
    {
        polarisCatalog.close();
    }
}
