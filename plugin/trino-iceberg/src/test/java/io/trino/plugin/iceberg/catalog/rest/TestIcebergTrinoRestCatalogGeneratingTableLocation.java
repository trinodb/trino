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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergTrinoRestCatalogGeneratingTableLocation
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "test_no_namespace_location";

    private Path warehouseLocation;
    private JdbcCatalog backend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        // Backend that never returns a location property for its namespaces, so
        // TrinoRestCatalog.defaultTableLocation returns null and CREATE TABLE goes
        // through the non-staged create path (tableBuilder.create().newTransaction()).
        backend = closeAfterClass(backendCatalogWithoutNamespaceLocation(warehouseLocation));

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
                        .put("fs.hadoop.enabled", "true")
                        .buildOrThrow())
                .disableSchemaInitializer()
                .build();
    }

    @Test
    void testCreateTable()
    {
        // Iceberg writes the initial metadata files to the table location before
        // IcebergMetadata.beginCreateTable checks whether the location is empty, which used
        // to cause a spurious "Cannot create a table on a non-empty location" error.
        try (TestTable table = newTrinoTable("test_table_", "(col INTEGER)")) {
            assertThat(backend.loadTable(TableIdentifier.of(SCHEMA, table.getName())).location())
                    .isEqualTo(warehouseLocation.resolve(SCHEMA, table.getName()).toString());
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    /**
     * Creates a backend catalog backed by a file:// URI warehouse whose namespace metadata
     * never includes a {@code location} property (such REST catalogs are technically allowed).
     */
    private static JdbcCatalog backendCatalogWithoutNamespaceLocation(Path warehouseLocation)
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath())
                .put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation.toFile().getAbsolutePath())
                .put(JdbcCatalog.PROPERTY_PREFIX + "username", "user")
                .put(JdbcCatalog.PROPERTY_PREFIX + "password", "password")
                .put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1")
                .buildOrThrow();
        JdbcCatalog backend = new JdbcCatalog()
        {
            @Override
            public Map<String, String> loadNamespaceMetadata(Namespace namespace)
            {
                Map<String, String> metadata = new HashMap<>(super.loadNamespaceMetadata(namespace));
                metadata.remove("location");
                return ImmutableMap.copyOf(metadata);
            }
        };
        backend.initialize("backend_jdbc", properties);
        return backend;
    }
}
