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
import io.airlift.http.client.jetty.JettyHttpClient;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.Closeable;
import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.trino.testing.TestingProperties.getDockerImagesVersion;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class TestingPolarisCatalog
        implements Closeable
{
    public static final String WAREHOUSE = "polaris";
    private static final int POLARIS_PORT = 8181;
    private static final Pattern PATTERN = Pattern.compile("realm: default-realm root principal credentials: (?<id>[a-f0-9]+):(?<secret>[a-f0-9]+)");

    private static final HttpClient HTTP_CLIENT = new JettyHttpClient();

    private final GenericContainer<?> polarisCatalog;
    private final String clientId;
    private final String clientSecret;
    private final String warehouseLocation;

    public TestingPolarisCatalog(String warehouseLocation)
    {
        this.warehouseLocation = requireNonNull(warehouseLocation, "warehouseLocation is null");

        // TODO: Use the official docker image once Polaris community provides it
        polarisCatalog = new GenericContainer<>("ghcr.io/trinodb/testing/polaris-catalog:" + getDockerImagesVersion());
        polarisCatalog.addExposedPort(POLARIS_PORT);
        polarisCatalog.withFileSystemBind(warehouseLocation, warehouseLocation, BindMode.READ_WRITE);
        polarisCatalog.waitingFor(new LogMessageWaitStrategy().withRegEx(".*o.eclipse.jetty.server.Server: Started.*"));
        polarisCatalog.start();

        clientId = findClientId();
        clientSecret = findClientSecret();
        createCatalog();
        grantPrivilege();
    }

    private String findClientId()
    {
        return Stream.of(polarisCatalog.getLogs().split("\n"))
                .map(PATTERN::matcher)
                .filter(Matcher::find)
                .map(matcher -> matcher.group("id"))
                .collect(onlyElement());
    }

    private String findClientSecret()
    {
        return Stream.of(polarisCatalog.getLogs().split("\n"))
                .map(PATTERN::matcher)
                .filter(Matcher::find)
                .map(matcher -> matcher.group("secret"))
                .collect(onlyElement());
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
                .setHeader("Authorization", "Bearer " + oauth2Token())
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
                .setHeader("Authorization", "Bearer " + oauth2Token())
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(createStaticBodyGenerator(body, UTF_8))
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    public void dropTable(String schema, String table)
    {
        Request request = Request.Builder.prepareDelete()
                .setUri(URI.create(restUri() + "/api/catalog/v1/polaris/namespaces/" + schema + "/tables/" + table))
                .setHeader("Authorization", "Bearer " + oauth2Token())
                .setHeader("Content-Type", "application/json")
                .build();
        HTTP_CLIENT.execute(request, createStatusResponseHandler());
    }

    public String restUri()
    {
        return "http://%s:%s".formatted(polarisCatalog.getHost(), polarisCatalog.getMappedPort(POLARIS_PORT));
    }

    public String oauth2Token()
    {
        return "principal:root;password:%s;realm:default-realm;role:ALL".formatted(clientSecret);
    }

    public String oauth2Credentials()
    {
        return "%s:%s".formatted(clientId, clientSecret);
    }

    @Override
    public void close()
    {
        polarisCatalog.close();
    }
}
