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

import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

// Table-only permissions: the REST catalog responds with 403 Forbidden to all view endpoints
final class TestIcebergRestCatalogForbiddenViews
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        RESTCatalogServlet servlet = new RESTCatalogServlet(new ViewForbiddingRestCatalogAdapter(closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation))));

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        TestingHttpServer testServer = new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, servlet, ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .setBaseDataDir(Optional.of(warehouseLocation))
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .build();
    }

    @Test
    void testTableQueriesWithForbiddenViews()
    {
        String tableName = "test_forbidden_views_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '42' a", 1);
        try {
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains(tableName);
            assertQuery("SELECT * FROM " + tableName, "VALUES 42");
            assertUpdate("INSERT INTO " + tableName + " VALUES 43", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES 42, 43");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    private static class ViewForbiddingRestCatalogAdapter
            extends RESTCatalogAdapter
    {
        ViewForbiddingRestCatalogAdapter(Catalog delegate)
        {
            super(delegate);
        }

        @Override
        protected <T extends RESTResponse> T execute(
                HTTPRequest request,
                Class<T> responseType,
                Consumer<ErrorResponse> errorHandler,
                Consumer<Map<String, String>> responseHeaders)
        {
            if (request.path().contains("/views")) {
                errorHandler.accept(ErrorResponse.builder()
                        .responseCode(403)
                        .withType(ForbiddenException.class.getSimpleName())
                        .withMessage("Forbidden: no permission to access views")
                        .build());
                throw new RESTException("Forbidden: no permission to access views");
            }
            return super.execute(request, responseType, errorHandler, responseHeaders);
        }
    }
}
