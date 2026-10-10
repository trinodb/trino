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
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseIcebergRestCatalogReferencedByTest
        extends AbstractTestQueryFramework
{
    private static final String REFERENCED_BY = "referenced-by";

    private static final Pattern TABLE_PATH = Pattern.compile(".*/namespaces/[^/]+/tables/([^/]+)");
    private static final Pattern VIEW_PATH = Pattern.compile(".*/namespaces/[^/]+/views/([^/]+)");

    private final List<LoadRequest> tableLoadRequests = new CopyOnWriteArrayList<>();
    private final List<LoadRequest> viewLoadRequests = new CopyOnWriteArrayList<>();
    private final boolean referencedByEnabled;
    private String restUri;

    protected BaseIcebergRestCatalogReferencedByTest(boolean referencedByEnabled)
    {
        this.referencedByEnabled = referencedByEnabled;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));
        JdbcCatalog backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        RESTCatalogAdapter adapter = new RESTCatalogAdapter(backend)
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
        };

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig().setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        TestingHttpServer testServer = new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, new RESTCatalogServlet(adapter), ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        testServer.start();
        closeAfterClass(testServer::stop);
        restUri = testServer.getBaseUrl().toString();

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.catalog.type", "rest",
                        "iceberg.rest-catalog.uri", testServer.getBaseUrl().toString(),
                        "iceberg.rest-catalog.referenced-by-enabled", String.valueOf(referencedByEnabled),
                        "fs.hadoop.enabled", "true"))
                .build();
    }

    private static List<String> referencedByOf(HTTPRequest request)
    {
        String referencedBy = request.queryParameters().get(REFERENCED_BY);
        return referencedBy == null ? List.of() : List.of(referencedBy.replace("%1F", ".").replace("\u001F", ".").split(","));
    }

    protected void createIcebergCatalog(String catalogName, Map<String, String> properties)
    {
        getQueryRunner().createCatalog(catalogName, "iceberg", ImmutableMap.<String, String>builder()
                .put("iceberg.catalog.type", "rest")
                .put("iceberg.rest-catalog.uri", restUri)
                .put("fs.hadoop.enabled", "true")
                .putAll(properties)
                .buildOrThrow());
    }

    protected List<List<String>> tableReferencedByOf(String table)
    {
        return tableLoadRequests.stream()
                .filter(request -> request.name().equals(table))
                .map(LoadRequest::referencedBy)
                .toList();
    }

    protected List<List<String>> viewReferencedByOf(String view)
    {
        return viewLoadRequests.stream()
                .filter(request -> request.name().equals(view))
                .map(LoadRequest::referencedBy)
                .toList();
    }

    protected String schema()
    {
        return getSession().getSchema().orElseThrow();
    }

    private record LoadRequest(String name, List<String> referencedBy) {}
}
