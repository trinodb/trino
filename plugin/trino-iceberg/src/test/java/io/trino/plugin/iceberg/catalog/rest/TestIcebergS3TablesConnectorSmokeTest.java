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

import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestIcebergS3TablesConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    public static final String S3_TABLES_BUCKET = requireEnv("S3_TABLES_BUCKET");
    public static final String AWS_ACCESS_KEY_ID = requireEnv("AWS_ACCESS_KEY_ID");
    public static final String AWS_SECRET_ACCESS_KEY = requireEnv("AWS_SECRET_ACCESS_KEY");
    public static final String AWS_REGION = requireEnv("AWS_REGION");

    public TestIcebergS3TablesConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder("tpch")
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", "https://glue.%s.amazonaws.com/iceberg".formatted(AWS_REGION))
                .addIcebergProperty("iceberg.rest-catalog.warehouse", "s3tablescatalog/" + S3_TABLES_BUCKET)
                .addIcebergProperty("iceberg.rest-catalog.view-endpoints-enabled", "false")
                .addIcebergProperty("iceberg.rest-catalog.sigv4-enabled", "true")
                .addIcebergProperty("iceberg.rest-catalog.signing-name", "glue")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .addIcebergProperty("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                .addIcebergProperty("fs.hadoop.enabled", "false")
                .addIcebergProperty("fs.native-s3.enabled", "true")
                .addIcebergProperty("s3.region", AWS_REGION)
                .addIcebergProperty("s3.aws-access-key", AWS_ACCESS_KEY_ID)
                .addIcebergProperty("s3.aws-secret-key", AWS_SECRET_ACCESS_KEY)
                .disableSchemaInitializer()
                .build();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        TrinoCatalog trinoCatalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
        BaseTable table = trinoCatalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String schemaPath()
    {
        return "dummy";
    }

    @Override
    protected boolean locationExists(String location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void deleteDirectory(String location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Test
    @Override // Override because the location pattern differs
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("CREATE TABLE iceberg.tpch.region \\(\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar,\n" +
                        "   comment varchar\n" +
                        "\\)\n" +
                        "WITH \\(\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = 2,\n" +
                        "   location = 's3://.*--table-s3'\n" +
                        "\\)");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Test
    @Override // Override because S3 Tables does not support specifying the location
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        String tableName = "test_create_table_with_trailing_space_" + randomNameSuffix();
        String tableLocationWithTrailingSpace = schemaPath() + tableName + " ";

        assertQueryFails(
                format("CREATE TABLE %s WITH (location = '%s') AS SELECT 1 AS a, 'INDIA' AS b, true AS c", tableName, tableLocationWithTrailingSpace),
                "Failed to create transaction");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasStackTraceContaining("Unable to process: RenameTable endpoint is not supported for Glue Catalog");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasStackTraceContaining("Unable to process: RenameTable endpoint is not supported for Glue Catalog");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasMessageContaining("Server does not support endpoint: POST /v1/{prefix}/namespaces/{namespace}/views");
    }

    @Test
    @Override // The locationExists helper method is unsupported
    public void testCreateTableWithNonExistingSchemaVerifyLocation() {}

    @Test
    @Override // The TrinoFileSystem.deleteFile is unsupported
    public void testDropTableWithMissingMetadataFile() {}

    @Test
    @Override // The TrinoFileSystem.deleteFile is unsupported
    public void testDropTableWithMissingManifestListFile() {}

    @Test
    @Override // The TrinoFileSystem.listFiles is unsupported
    public void testMetadataDeleteAfterCommitEnabled() {}

    @Test
    @Override // The TrinoFileSystem.deleteFile is unsupported
    public void testDropTableWithMissingSnapshotFile() {}

    @Test
    @Override // The TrinoFileSystem.listFiles is unsupported
    public void testDropTableWithMissingDataFile() {}

    @Test
    @Override // The TrinoFileSystem.deleteDirectory is unsupported
    public void testDropTableWithNonExistentTableLocation() {}

    @Test
    @Override // BaseIcebergConnectorSmokeTest.isFileSorted method is unsupported
    public void testSortedNationTable() {}

    @Test
    @Override // The TrinoFileSystem.deleteFile is unsupported
    public void testFileSortingWithLargerTable() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithTableLocation() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithComments() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithShowCreateTable() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithReInsert() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithDroppedTable() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithDifferentTableName() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithMetadataFile() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRegisterTableWithTrailingSpaceInLocation() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testUnregisterTable() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testUnregisterBrokenTable() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testUnregisterTableNotExistingSchema() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testUnregisterTableNotExistingTable() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testRepeatUnregisterTable() {}

    @Test
    @Override // The procedure is unsupported in S3 Tables
    public void testUnregisterTableAccessControl() {}
}
