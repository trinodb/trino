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
package io.trino.plugin.iceberg.catalog.jdbc;

import com.google.common.collect.ImmutableMap;
import io.trino.metastore.TableInfo.ExtendedRelationType;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.iceberg.catalog.BaseTrinoCatalogTest;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.TrinoPrincipal;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.postgresql.Driver;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.hdfs.HdfsTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig.SchemaVersion.V1;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.PASSWORD;
import static io.trino.plugin.iceberg.catalog.jdbc.TestingIcebergJdbcServer.USER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.jdbc.JdbcCatalog.PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestTrinoJdbcCatalog
        extends BaseTrinoCatalogTest
{
    private static final String CATALOG_NAME = "iceberg_jdbc";

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private TestingIcebergJdbcServer server;

    @BeforeAll
    public void setUp()
    {
        server = closer.register(new TestingIcebergJdbcServer());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closer.close();
    }

    @Override
    protected TrinoCatalog createTrinoCatalog(boolean useUniqueTableLocations)
            throws IOException
    {
        Path warehouseLocation = Files.createTempDirectory(null);
        warehouseLocation.toFile().deleteOnExit();
        JdbcCatalog jdbcCatalog = createJdbcCatalog(server.getJdbcUrl(), warehouseLocation);
        closer.register(jdbcCatalog);
        return createTrinoJdbcCatalog(useUniqueTableLocations, warehouseLocation, server.getJdbcUrl(), jdbcCatalog);
    }

    private static TrinoJdbcCatalog createTrinoJdbcCatalog(boolean useUniqueTableLocations, Path warehouseLocation, String jdbcUrl, JdbcCatalog jdbcCatalog)
    {
        IcebergJdbcClient jdbcClient = new IcebergJdbcClient(
                new IcebergJdbcConnectionFactory(new Driver(), jdbcUrl, Optional.of(USER), Optional.of(PASSWORD)),
                CATALOG_NAME,
                V1);
        return new TrinoJdbcCatalog(
                new CatalogName(CATALOG_NAME),
                TESTING_TYPE_MANAGER,
                new IcebergJdbcTableOperationsProvider(HDFS_FILE_SYSTEM_FACTORY, FILE_IO_FACTORY, jdbcClient),
                jdbcCatalog,
                jdbcClient,
                HDFS_FILE_SYSTEM_FACTORY,
                FILE_IO_FACTORY,
                useUniqueTableLocations,
                warehouseLocation.toAbsolutePath().toString(),
                V1,
                directExecutor());
    }

    private static JdbcCatalog createJdbcCatalog(String jdbcUrl, Path warehouseLocation)
    {
        return (JdbcCatalog) buildIcebergCatalog(
                CATALOG_NAME,
                ImmutableMap.<String, String>builder()
                        .put(CATALOG_IMPL, JdbcCatalog.class.getName())
                        .put(URI, jdbcUrl)
                        .put(PROPERTY_PREFIX + "user", USER)
                        .put(PROPERTY_PREFIX + "password", PASSWORD)
                        .put(PROPERTY_PREFIX + "schema-version", V1.toString())
                        .put(WAREHOUSE_LOCATION, warehouseLocation.toAbsolutePath().toString())
                        .buildOrThrow(),
                null);
    }

    @Override
    protected void createNamespaceWithProperties(TrinoCatalog catalog, String namespace, Map<String, String> properties)
    {
        catalog.createNamespace(
                SESSION,
                namespace,
                ImmutableMap.copyOf(properties),
                new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
    }

    @Override
    protected ExtendedRelationType getViewType()
    {
        return ExtendedRelationType.OTHER_VIEW;
    }

    @Test
    @Override
    public void testNonLowercaseNamespace()
            throws Exception
    {
        TrinoCatalog catalog = createTrinoCatalog(false);

        String namespace = "testNonLowercaseNamespace";
        String schema = namespace.toLowerCase(ENGLISH);

        catalog.createNamespace(SESSION, namespace, defaultNamespaceProperties(namespace), new TrinoPrincipal(PrincipalType.USER, SESSION.getUser()));
        try {
            assertThat(catalog.namespaceExists(SESSION, namespace)).as("catalog.namespaceExists(namespace)")
                    .isTrue();
            assertThat(catalog.namespaceExists(SESSION, schema)).as("catalog.namespaceExists(schema)")
                    .isFalse();
            assertThat(catalog.listNamespaces(SESSION)).as("catalog.listNamespaces")
                    // JDBC catalog lowercases namespaces returned from listNamespaces
                    .doesNotContain(namespace)
                    .contains(schema);
        }
        finally {
            catalog.dropNamespace(SESSION, namespace);
        }
    }

    @Test
    @Override
    public void testSchemaWithInvalidProperties()
    {
        // JDBC catalog preserves arbitrary namespace properties
        // https://github.com/trinodb/trino/issues/29769
    }
}
