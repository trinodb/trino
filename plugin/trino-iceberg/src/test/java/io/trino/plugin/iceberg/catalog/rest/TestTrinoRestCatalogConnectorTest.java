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
import io.trino.plugin.iceberg.TestIcebergParquetConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.assertj.core.util.Files;

import java.io.File;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoRestCatalogConnectorTest
        extends TestIcebergParquetConnectorTest
{
    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_RENAME_SCHEMA -> false;
            case SUPPORTS_CREATE_VIEW, SUPPORTS_COMMENT_ON_VIEW, SUPPORTS_COMMENT_ON_VIEW_COLUMN -> false;
            case SUPPORTS_CREATE_MATERIALIZED_VIEW, SUPPORTS_RENAME_MATERIALIZED_VIEW -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File warehouseLocation = Files.newTemporaryFolder();
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));

        Catalog backend = backendCatalog(warehouseLocation);

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .buildOrThrow())
                .setInitialTables(ImmutableList.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(LINE_ITEM)
                        .build())
                .build();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        // h2 test database backend limit
        return OptionalInt.of(255);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageContaining("Failed to execute");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // This value depends on metastore backend limit
        // The connector appends uuids to the end of all table names
        // 33 is the length of random suffix. e.g. {table name}-142763c594d54e4b9329a98f90528caf
        return OptionalInt.of(255 - 33);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching(".*Failed to create.*|.*Failed to execute.*|.*Failed to rename.*");
    }

    @Override
    protected int maxTableRenameLength()
    {
        // h2 test database backend limit
        return 255;
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .getCause()
                .hasMessageContaining("Commit failed: Requirement failed: last assigned field id changed");
    }

    @Override
    public void testShowCreateSchema()
    {
        // Overridden due to REST catalog not supporting namespace principal
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("\\QCREATE SCHEMA iceberg.tpch\n" +
                        "WITH (\n" +
                        "   location = '\\E.*\\Q/iceberg_data/tpch'\n" +
                        ")\\E");
    }

    @Override
    protected void verifyIcebergTableProperties(MaterializedResult actual)
    {
        assertThat(actual)
                .anySatisfy(row -> assertThat(row).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, "write.format.default", this.format.name())))
                .anySatisfy(row -> assertThat(row.getFields()).contains("created-at"));
    }

    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createView is not supported for Iceberg REST catalog");
    }

    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public void testFederatedMaterializedView()
    {
        assertThatThrownBy(super::testFederatedMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Override
    public void testMaterializedViewSnapshotSummariesHaveTrinoQueryId()
    {
        assertThatThrownBy(super::testMaterializedViewSnapshotSummariesHaveTrinoQueryId)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }
}
