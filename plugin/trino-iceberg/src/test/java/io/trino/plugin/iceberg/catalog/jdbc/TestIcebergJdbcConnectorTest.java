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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.BaseIcebergConnectorTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.OptionalInt;

import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergJdbcConnectorTest
        extends BaseIcebergConnectorTest
{
    public TestIcebergJdbcConnectorTest()
    {
        super(new IcebergConfig().getFileFormat());
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingIcebergJdbcServer server = closeAfterClass(new TestingIcebergJdbcServer());
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "jdbc")
                                .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                                .put("iceberg.jdbc-catalog.catalog-name", "tpch")
                                .buildOrThrow())
                .setInitialTables(ImmutableList.<TpchTable<?>>builder()
                        .addAll(REQUIRED_TPCH_TABLES)
                        .add(LINE_ITEM)
                        .build())
                .build();
    }

    @Override
    public void testShowCreateSchema()
    {
        // Override because Iceberg JDBC catalog requires location in the namespace
        assertThat(computeActual("SHOW CREATE SCHEMA tpch").getOnlyValue().toString())
                .matches("""
                      CREATE SCHEMA iceberg.tpch
                      WITH \\(
                         location = '.*'
                      \\)""");
    }

    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessage("renameNamespace is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRenameSchemaToLongName()
    {
        assertThatThrownBy(super::testRenameSchemaToLongName)
                .hasMessage("renameNamespace is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testCreateViewSchemaNotFound()
    {
        assertThatThrownBy(super::testCreateViewSchemaNotFound)
                .hasMessageContaining("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testShowCreateView()
    {
        assertThatThrownBy(super::testShowCreateView)
                .hasMessageContaining("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testCompatibleTypeChangeForView()
    {
        assertThatThrownBy(super::testCompatibleTypeChangeForView)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testCompatibleTypeChangeForView2()
    {
        assertThatThrownBy(super::testCompatibleTypeChangeForView2)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testDropNonEmptySchemaWithView()
    {
        assertThatThrownBy(super::testDropNonEmptySchemaWithView)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Test(dataProvider = "testViewMetadataDataProvider")
    @Override
    public void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        assertThatThrownBy(() -> super.testViewMetadata(securityClauseInCreate, securityClauseInShowCreate))
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testViewCaseSensitivity()
    {
        assertThatThrownBy(super::testViewCaseSensitivity)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testViewAndMaterializedViewTogether()
    {
        assertThatThrownBy(super::testViewAndMaterializedViewTogether)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasMessage("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        assertThatThrownBy(super::testReadMetadataWithRelationsConcurrentModifications)
                .hasMessageMatching(".* (createView|createMaterializedView) is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessage("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testMaterializedViewBaseTableGone(boolean initialized)
    {
        assertThatThrownBy(() -> super.testMaterializedViewBaseTableGone(initialized))
                .hasMessage("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Test(dataProvider = "testColumnNameDataProvider")
    @Override
    public void testMaterializedViewColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testMaterializedViewColumnName(columnName))
                .hasMessage("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testDropNonEmptySchemaWithMaterializedView()
    {
        assertThatThrownBy(super::testDropNonEmptySchemaWithMaterializedView)
                .hasMessage("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRenameMaterializedView()
    {
        assertThatThrownBy(super::testRenameMaterializedView)
                .hasMessage("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testFederatedMaterializedView()
    {
        assertThatThrownBy(super::testFederatedMaterializedView)
                .hasMessage("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testMaterializedViewSnapshotSummariesHaveTrinoQueryId()
    {
        assertThatThrownBy(super::testMaterializedViewSnapshotSummariesHaveTrinoQueryId)
                .hasMessage("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageStartingWith("Failed to add column: Failed to update table due to concurrent updates");
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary")) &&
                !(typeName.equalsIgnoreCase("uuid"));
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary");
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "10")
                .build();
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(255);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e)
                .hasMessageContaining("Failed to create a namespace")
                .hasStackTraceContaining("ERROR: value too long for type character varying(255)");
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Failed to create file.*|.*Failed to rename table.*");
    }
}
