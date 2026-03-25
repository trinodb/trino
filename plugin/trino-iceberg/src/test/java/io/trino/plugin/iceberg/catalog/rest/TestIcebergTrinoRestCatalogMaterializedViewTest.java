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
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.REGION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergTrinoRestCatalogMaterializedViewTest
        extends AbstractTestQueryFramework
{
    private static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(NATION, REGION);

    private Path warehouseLocation;

    private JdbcCatalog backend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterAll
    public void teardown()
    {
        backend = null; // closed by closeAfterClass
    }

    @Test
    void testCreateView()
    {
        String schemaName = "test_demo" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + schemaName);

        TableIdentifier viewIdentifier = TableIdentifier.of(schemaName, "test_trino_view" + randomNameSuffix());
        assertUpdate("CREATE VIEW %s.%s AS SELECT * FROM tpch.nation".formatted(schemaName, viewIdentifier.name()));

        assertThat(backend.viewExists(viewIdentifier)).isTrue();

        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");

        assertThat(backend.viewExists(viewIdentifier)).isFalse();
    }

    @Test
    void testCreateMaterializedView()
    {
        String schemaName = "test_demo" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + schemaName);

        TableIdentifier viewIdentifier = TableIdentifier.of(schemaName, "test_trino_mv" + randomNameSuffix());
        assertUpdate("CREATE MATERIALIZED VIEW %s.%s AS SELECT * FROM tpch.nation".formatted(schemaName, viewIdentifier.name()));

        assertThat(backend.viewExists(viewIdentifier)).isTrue();
        assertThat(backend.loadView(viewIdentifier).currentVersion().storageTable()).isNotNull();

        TableIdentifier storageTable = backend.loadView(viewIdentifier).currentVersion().storageTable();

        //assertUpdate("REFRESH MATERIALIZED VIEW %s.%s".formatted(schemaName, viewIdentifier.name()));
        // simulate MV refresh until this operation will be done on IRC side
        assertUpdate("CREATE OR REPLACE TABLE %s.%s AS SELECT * FROM tpch.nation".formatted(storageTable.namespace().toString(), storageTable.name()), 25);
        assertThat(query("SELECT * FROM %s.%s".formatted(schemaName, viewIdentifier.name())))
                .matches("SELECT * FROM tpch.nation");

        assertUpdate("DROP MATERIALIZED VIEW %s.%s".formatted(schemaName, viewIdentifier.name()));
        assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");

        assertThat(backend.viewExists(viewIdentifier)).isFalse();
    }
}
