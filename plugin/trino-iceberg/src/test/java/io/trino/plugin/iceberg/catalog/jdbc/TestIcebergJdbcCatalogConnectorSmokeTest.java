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
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergJdbcCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    public TestIcebergJdbcCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

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
        TestingIcebergJdbcServer server = closeAfterClass(new TestingIcebergJdbcServer());
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "jdbc")
                                .put("iceberg.jdbc-catalog.connection-url", server.getJdbcUrl())
                                .put("iceberg.jdbc-catalog.catalog-name", "tpch")
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("createView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg JDBC catalogs");
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        // used when registering a table, which is not supported by the JDBC catalog
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        // used when registering a table, which is not supported by the JDBC catalog
        throw new UnsupportedOperationException("metadata location for register_table is not supported");
    }

    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("registerTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("registerTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("registerTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("registerTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("registerTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("metadata location for register_table is not supported");
    }

    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testUnregisterBrokenTable()
    {
        assertThatThrownBy(super::testUnregisterBrokenTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testUnregisterTableNotExistingTable()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasMessageContaining("unregisterTable is not supported for Iceberg JDBC catalogs");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        // used when unregistering a table, which is not supported by the JDBC catalog
    }
}
