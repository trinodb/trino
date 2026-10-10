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

import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Floci;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Checks that the views a table is read through are sent to the REST catalog when the credentials
 * vended for the table are loaded, not only when the table handle is created.
 */
@TestInstance(PER_CLASS)
final class TestIcebergS3RestCatalogVendedCredentialsReferencedBy
        extends AbstractTestQueryFramework
{
    private static final String REFERENCED_BY = "referenced-by";
    private static final Pattern TABLE_PATH = Pattern.compile(".*/namespaces/[^/]+/tables/([^/]+)");
    private static final Pattern VIEW_PATH = Pattern.compile(".*/namespaces/[^/]+/views/([^/]+)");

    private final String bucketName = "test-iceberg-vended-credentials-referenced-by-" + randomNameSuffix();
    private final List<LoadRequest> tableLoadRequests = new CopyOnWriteArrayList<>();
    private final List<LoadRequest> viewLoadRequests = new CopyOnWriteArrayList<>();
    private Floci floci;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        floci = closeAfterClass(new Floci());
        floci.start();
        floci.createBucket(bucketName);
        floci.createRole("test");

        JdbcCatalog backend = closeAfterClass(buildBackendCatalog("s3://%s/default/".formatted(bucketName)));

        // Records the referenced-by of every table load and vends S3 credentials with each loaded table
        VendedCredentialsRestCatalogAdapter adapter = new VendedCredentialsRestCatalogAdapter(backend)
        {
            @Override
            protected <T extends RESTResponse> T execute(
                    HTTPRequest request,
                    Class<T> responseType,
                    Consumer<ErrorResponse> errorHandler,
                    Consumer<Map<String, String>> responseHeaders)
            {
                if ("GET".equalsIgnoreCase(request.method().name())) {
                    Matcher tableMatcher = TABLE_PATH.matcher(request.path());
                    Matcher viewMatcher = VIEW_PATH.matcher(request.path());
                    if (tableMatcher.matches()) {
                        tableLoadRequests.add(new LoadRequest(tableMatcher.group(1), referencedByOf(request)));
                    }
                    else if (viewMatcher.matches()) {
                        viewLoadRequests.add(new LoadRequest(viewMatcher.group(1), referencedByOf(request)));
                    }
                }
                return super.execute(request, responseType, errorHandler, responseHeaders);
            }

            @Override
            protected Map<String, String> getVendedCredentialsConfig(String restServerUri)
            {
                Credentials sessionCredentials = sessionCredentials();
                return ImmutableMap.<String, String>builder()
                        .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                        .put(S3FileIOProperties.ACCESS_KEY_ID, sessionCredentials.accessKeyId())
                        .put(S3FileIOProperties.SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey())
                        .put(S3FileIOProperties.SESSION_TOKEN, sessionCredentials.sessionToken())
                        .buildOrThrow();
            }
        };

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        TestingHttpServer testServer = new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, new RESTCatalogServlet(adapter), ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.rest-catalog.vended-credentials-enabled", "true")
                .addIcebergProperty("iceberg.rest-catalog.referenced-by-enabled", "true")
                .addIcebergProperty("fs.s3.enabled", "true")
                .addIcebergProperty("s3.endpoint", floci.endpoint().toString())
                .addIcebergProperty("s3.region", FLOCI_REGION)
                .addIcebergProperty("s3.path-style-access", "true")
                .build();
    }

    @Test
    void testDirectReadSendsNoViews()
    {
        String table = "direct_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        int loadsBefore = tableLoadsOf(table).size();

        assertThat(query("SELECT * FROM " + table)).matches("VALUES 1");

        assertThat(tableLoadsOf(table).subList(loadsBefore, tableLoadsOf(table).size()))
                .allSatisfy(views -> assertThat(views).isEmpty());

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testReadThroughViewSendsViewOnEveryLoad()
    {
        String table = "t_" + randomNameSuffix();
        String view = "v_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + view + " AS SELECT * FROM " + table);
        int loadsBefore = tableLoadsOf(table).size();

        assertThat(query("SELECT * FROM " + view)).matches("VALUES 1");

        assertThat(tableLoadsOf(table).subList(loadsBefore, tableLoadsOf(table).size()))
                .isNotEmpty()
                .allSatisfy(views -> assertThat(views).containsExactly(schema() + "." + view));

        assertUpdate("DROP VIEW " + view);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testReadThroughNestedViewsSendsViewsOnEveryLoad()
    {
        String table = "t_" + randomNameSuffix();
        String innerView = "inner_view_" + randomNameSuffix();
        String outerView = "outer_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + innerView + " SECURITY INVOKER AS SELECT * FROM " + table);
        assertUpdate("CREATE VIEW " + outerView + " SECURITY DEFINER AS SELECT * FROM " + innerView);
        int loadsBefore = tableLoadsOf(table).size();

        assertThat(query("SELECT * FROM " + outerView)).matches("VALUES 1");

        assertThat(tableLoadsOf(table).subList(loadsBefore, tableLoadsOf(table).size()))
                .isNotEmpty()
                .allSatisfy(views -> assertThat(views).containsExactly(schema() + "." + outerView, schema() + "." + innerView));

        assertUpdate("DROP VIEW " + outerView);
        assertUpdate("DROP VIEW " + innerView);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testNestedViewLoadSendsEnclosingViews()
    {
        String table = "t_" + randomNameSuffix();
        String innerView = "inner_view_" + randomNameSuffix();
        String outerView = "outer_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + innerView + " SECURITY INVOKER AS SELECT * FROM " + table);
        assertUpdate("CREATE VIEW " + outerView + " SECURITY DEFINER AS SELECT * FROM " + innerView);
        int outerLoadsBefore = viewLoadsOf(outerView).size();
        int innerLoadsBefore = viewLoadsOf(innerView).size();

        assertThat(query("SELECT * FROM " + outerView)).matches("VALUES 1");

        assertThat(viewLoadsOf(outerView).subList(outerLoadsBefore, viewLoadsOf(outerView).size()))
                .isNotEmpty()
                .allSatisfy(views -> assertThat(views).isEmpty());
        assertThat(viewLoadsOf(innerView).subList(innerLoadsBefore, viewLoadsOf(innerView).size()))
                .isNotEmpty()
                .allSatisfy(views -> assertThat(views).containsExactly(schema() + "." + outerView));

        assertUpdate("DROP VIEW " + outerView);
        assertUpdate("DROP VIEW " + innerView);
        assertUpdate("DROP TABLE " + table);
    }

    @Test
    void testTableAndViewOverSameTableInOneQuery()
    {
        String table = "t_" + randomNameSuffix();
        String view = "v_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate("CREATE VIEW " + view + " AS SELECT * FROM " + table);
        int loadsBefore = tableLoadsOf(table).size();

        assertThat(query("SELECT t.x, v.x FROM " + table + " t JOIN " + view + " v ON t.x = v.x")).matches("VALUES (1, 1)");

        // The direct read is loaded once and then served from the table cache, while reads through the view always bypass the cache
        List<List<String>> loads = tableLoadsOf(table).subList(loadsBefore, tableLoadsOf(table).size());
        assertThat(loads.stream().filter(List::isEmpty)).hasSize(1);
        assertThat(loads.stream().filter(views -> !views.isEmpty()))
                .isNotEmpty()
                .allSatisfy(views -> assertThat(views).containsExactly(schema() + "." + view));

        assertUpdate("DROP VIEW " + view);
        assertUpdate("DROP TABLE " + table);
    }

    private static List<String> referencedByOf(HTTPRequest request)
    {
        String referencedBy = request.queryParameters().get(REFERENCED_BY);
        return referencedBy == null ? List.of() : List.of(referencedBy.replace("%1F", ".").replace("\u001F", ".").split(","));
    }

    private List<List<String>> tableLoadsOf(String table)
    {
        return tableLoadRequests.stream()
                .filter(request -> request.name().equals(table))
                .map(LoadRequest::referencedBy)
                .toList();
    }

    private List<List<String>> viewLoadsOf(String view)
    {
        return viewLoadRequests.stream()
                .filter(request -> request.name().equals(view))
                .map(LoadRequest::referencedBy)
                .toList();
    }

    private String schema()
    {
        return getSession().getSchema().orElseThrow();
    }

    private Credentials sessionCredentials()
    {
        try (StsClient stsClient = StsClient.builder().applyMutation(floci::updateClient).build()) {
            return stsClient.assumeRole(AssumeRoleRequest.builder()
                            .roleArn("arn:aws:iam::000000000000:role/test")
                            .roleSessionName("test")
                            .build())
                    .credentials();
        }
    }

    private JdbcCatalog buildBackendCatalog(String warehouseLocation)
            throws IOException
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath());
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "username", "user");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "password", "password");
        properties.put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
        properties.put(FILE_IO_IMPL, S3FileIO.class.getName());
        properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
        properties.put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION);
        properties.put(S3FileIOProperties.ACCESS_KEY_ID, FLOCI_ACCESS_KEY);
        properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, FLOCI_SECRET_KEY);
        properties.put(S3FileIOProperties.ENDPOINT, floci.endpoint().toString());

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("backend_jdbc", properties.buildOrThrow());
        return catalog;
    }

    private record LoadRequest(String name, List<String> referencedBy) {}
}
