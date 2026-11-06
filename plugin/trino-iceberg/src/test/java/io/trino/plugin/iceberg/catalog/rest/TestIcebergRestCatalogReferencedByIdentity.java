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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServer.ClientCertificate;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.Session;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNullElse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
final class TestIcebergRestCatalogReferencedByIdentity
        extends AbstractTestQueryFramework
{
    private static final String OWNER = "owner";
    private static final String INVOKER = "invoker";
    private static final String INVOKER_TOKEN = INVOKER + "-token";

    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        // Records the path and Authorization header of every request, to show on whose behalf the catalog is called
        RESTCatalogServlet servlet = new RESTCatalogServlet(new RESTCatalogAdapter(backendCatalog(warehouseLocation)))
        {
            @Override
            protected void service(HttpServletRequest request, HttpServletResponse response)
                    throws ServletException, IOException
            {
                requests.add(new RecordedRequest(
                        request.getRequestURI(),
                        requireNonNullElse(request.getHeader("Authorization"), "")));
                super.service(request, response);
            }
        };

        TestingHttpServer testServer = testServer(servlet);
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .setBaseDataDir(Optional.of(warehouseLocation))
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                .addIcebergProperty("iceberg.rest-catalog.session", "USER")
                .addIcebergProperty("iceberg.rest-catalog.oauth2.token", "service-account-token")
                .addIcebergProperty("iceberg.rest-catalog.referenced-by-enabled", "true")
                .build();
    }

    @Test
    void testEveryRequestThroughDefinerViewIsSentAsInvoker()
    {
        String table = "t_" + randomNameSuffix();
        String definerView = "definer_view_" + randomNameSuffix();

        Session owner = sessionOf(OWNER);
        Session invoker = sessionOf(INVOKER);

        assertUpdate(owner, "CREATE TABLE " + table + " AS SELECT 1 x", 1);
        assertUpdate(owner, "CREATE VIEW " + definerView + " SECURITY DEFINER AS SELECT * FROM " + table);

        // the view lookup, and every table load while analyzing, planning and running, carry the invoker's token
        for (String query : List.of(
                "SELECT * FROM " + definerView,
                "SELECT * FROM " + definerView + " WHERE x = 1",
                "SHOW STATS FOR (SELECT * FROM " + definerView + ")",
                "EXPLAIN ANALYZE SELECT * FROM " + definerView)) {
            assertThat(authorizationsOf(invoker, query))
                    .describedAs(query)
                    .isNotEmpty()
                    .containsOnly("Bearer " + INVOKER_TOKEN);
        }

        assertUpdate(owner, "DROP VIEW " + definerView);
        assertUpdate(owner, "DROP TABLE " + table);
    }

    private record RecordedRequest(String path, String authorization) {}

    private static TestingHttpServer testServer(RESTCatalogServlet servlet)
            throws IOException
    {
        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setMinThreads(4)
                .setMaxThreads(8)
                .setHttpEnabled(true);
        HttpConfig httpConfig = new HttpConfig()
                .setHttpPort(0)
                .setHttpAcceptorThreads(4)
                .setAcceptQueueSize(10);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(httpConfig), Optional.empty(), nodeInfo);

        return new TestingHttpServer(
                "rest-catalog",
                httpServerInfo,
                nodeInfo,
                config,
                Optional.of(httpConfig),
                Optional.empty(),
                servlet,
                ImmutableSet.of(),
                ImmutableSet.of(),
                ServerFeature.builder()
                        // Required due to URIs like: HEAD /v1/namespaces/level_1%1Flevel_2
                        .withLegacyUriCompliance(true)
                        .build(),
                ClientCertificate.NONE);
    }

    private List<String> authorizationsOf(Session session, String query)
    {
        requests.clear();
        computeActual(session, query);
        return ImmutableList.copyOf(requests).stream()
                .filter(request -> request.path().startsWith("/v1/namespaces/"))
                .map(RecordedRequest::authorization)
                .collect(toImmutableList());
    }

    private Session sessionOf(String user)
    {
        return testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("tpch")
                .setIdentity(Identity.forUser(user).withExtraCredentials(Map.of("token", user + "-token")).build())
                .build();
    }
}
