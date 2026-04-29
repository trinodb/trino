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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.node.NodeInfo;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;

@Execution(ExecutionMode.SAME_THREAD)
abstract class AbstractTestIcebergRestCatalogVendedCredentialsRefreshTest
        extends AbstractTestQueryFramework
{
    private static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(REGION);

    protected String warehouseLocation;
    private VendedCredentialsRestCatalogServlet servlet;
    protected final AtomicReference<Instant> sessionTokenExpirationTime = new AtomicReference<>(Instant.now().plus(1, ChronoUnit.HOURS));

    protected abstract String setupStorageAndGetWarehouseLocation()
            throws Exception;

    protected abstract Map<String, String> backendCatalogFileIoProperties();

    protected abstract VendedCredentialsRestCatalogServlet createServlet(Catalog backendCatalog);

    protected abstract Map<String, String> catalogFileSystemProperties();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = setupStorageAndGetWarehouseLocation();

        JdbcCatalog backend = closeAfterClass(buildBackendCatalog(warehouseLocation));
        servlet = createServlet(backend);

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpPort(0)
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, nodeInfo);
        TestingHttpServer testServer = new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, servlet, ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        testServer.start();
        closeAfterClass(testServer::stop);

        IcebergQueryRunner.Builder builder = IcebergQueryRunner.builder()
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                .addIcebergProperty("iceberg.rest-catalog.vended-credentials-enabled", "true")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB");
        catalogFileSystemProperties().forEach(builder::addIcebergProperty);
        return builder
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
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
        properties.putAll(backendCatalogFileIoProperties());

        ConnectorSession connectorSession = TestingConnectorSession.builder().build();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        HdfsContext context = new HdfsContext(connectorSession);

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.setConf(hdfsEnvironment.getConfiguration(context, new Path(warehouseLocation)));
        catalog.initialize("backend_jdbc", properties.buildOrThrow());
        return catalog;
    }

    @BeforeEach
    public void initSessionTokenExpirationTime()
    {
        // Simulate a real-world expiration time of the token ~ 1 hour
        sessionTokenExpirationTime.set(Instant.now().plus(1, ChronoUnit.HOURS));
    }

    @Test
    public void testCreateTableAsSelect()
    {
        try (TestTable testTable = newTrinoTable("test_ctas", "AS SELECT * FROM region")) {
            // Intentionally set the expiration time to be in the near future to test credential refresh logic
            sessionTokenExpirationTime.set(Instant.now().plusSeconds(60));
            int refreshCount = servlet.getVendedCredentialsRefreshCount();
            assertQuery("SELECT * FROM " + testTable.getName(), "SELECT * FROM region");
            assertThat(servlet.getVendedCredentialsRefreshCount()).isGreaterThan(refreshCount);

            // Providing a session token that expires only in one hour should not trigger token refresh
            sessionTokenExpirationTime.set(Instant.now().plus(1, ChronoUnit.HOURS));
            refreshCount = servlet.getVendedCredentialsRefreshCount();
            assertQuery("SELECT * FROM " + testTable.getName(), "SELECT * FROM region");
            assertThat(servlet.getVendedCredentialsRefreshCount()).isEqualTo(refreshCount);
        }
    }
}
