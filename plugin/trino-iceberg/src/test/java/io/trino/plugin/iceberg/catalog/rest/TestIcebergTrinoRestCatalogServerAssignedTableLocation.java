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
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergTrinoRestCatalogServerAssignedTableLocation
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "test_server_assigned_table_location";

    @TempDir
    private static Path warehouseLocation;
    private JdbcCatalog backend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Backend that assigns each table a location with a random suffix, like BigLake metastore
        backend = closeAfterClass(backendCatalogWithServerAssignedTableLocation(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();
        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        backend.createNamespace(Namespace.of(SCHEMA), ImmutableMap.of());

        return IcebergQueryRunner.builder(SCHEMA)
                .setIcebergProperties(ImmutableMap.<String, String>builder()
                        .put("iceberg.catalog.type", "rest")
                        .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                        .put("iceberg.rest-catalog.server-assigned-table-location-enabled", "true")
                        .put("fs.hadoop.enabled", "true")
                        .buildOrThrow())
                .disableSchemaInitializer()
                .build();
    }

    @Test
    void testCreateTable()
    {
        // The namespace has a location, but Trino must not derive a table location from it
        // and let the catalog server assign one instead
        assertThat(backend.loadNamespaceMetadata(Namespace.of(SCHEMA))).containsKey("location");

        try (TestTable table = newTrinoTable("test_table_", "(col INTEGER)")) {
            assertThat(backend.loadTable(TableIdentifier.of(SCHEMA, table.getName())).location())
                    .startsWith(warehouseLocation.resolve(SCHEMA).resolve(table.getName()) + "/");
            assertUpdate("INSERT INTO " + table.getName() + " VALUES 1", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 1");
        }
    }

    @Test
    void testCreateTableAsSelect()
    {
        try (TestTable table = newTrinoTable("test_ctas_", " AS SELECT 42 col")) {
            assertThat(backend.loadTable(TableIdentifier.of(SCHEMA, table.getName())).location())
                    .startsWith(warehouseLocation.resolve(SCHEMA).resolve(table.getName()) + "/");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 42");
        }
    }

    /**
     * Creates a backend catalog backed by a file:// URI warehouse that assigns each table
     * a location with a random suffix ({namespace_path}/{table_name}/{random_suffix}),
     * emulating BigLake metastore.
     */
    private static JdbcCatalog backendCatalogWithServerAssignedTableLocation(Path warehouseLocation)
            throws IOException
    {
        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath())
                .put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation.toFile().getAbsolutePath())
                .put(JdbcCatalog.PROPERTY_PREFIX + "username", "user")
                .put(JdbcCatalog.PROPERTY_PREFIX + "password", "password")
                .put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1")
                .buildOrThrow();
        JdbcCatalog backend = new JdbcCatalog()
        {
            @Override
            protected String defaultWarehouseLocation(TableIdentifier tableIdentifier)
            {
                return super.defaultWarehouseLocation(tableIdentifier) + "/" + randomNameSuffix();
            }
        };
        backend.initialize("backend_jdbc", properties);
        return backend;
    }
}
