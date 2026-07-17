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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemStats;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.minio.MinioClient;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Isolated // TODO remove
@TestInstance(PER_CLASS)
final class TestIcebergPolarisCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName;
    private TestingPolarisCatalog polarisCatalog;
    private final String warehouseLocation;

    public TestIcebergPolarisCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        bucketName = "test-iceberg-vending-polaris-smoke-test-" + randomNameSuffix();
        warehouseLocation = "s3://%s/default/".formatted(bucketName);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        polarisCatalog = closeAfterClass(new TestingPolarisCatalog(warehouseLocation, bucketName));

        return IcebergQueryRunner.builder()
                .addIcebergProperty("fs.s3.enabled", "true")
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.nested-namespace-enabled", "true")
                .addIcebergProperty("iceberg.rest-catalog.uri", polarisCatalog.restUri() + "/api/catalog")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", TestingPolarisCatalog.WAREHOUSE)
                .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                .addIcebergProperty("iceberg.rest-catalog.oauth2.credential", TestingPolarisCatalog.CREDENTIAL)
                .addIcebergProperty("iceberg.rest-catalog.oauth2.scope", "PRINCIPAL_ROLE:ALL")
                .addIcebergProperty("iceberg.rest-catalog.http-headers", TestingPolarisCatalog.POLARIS_REALM_HEADER + ": " + TestingPolarisCatalog.POLARIS_REALM_NAME)
                .addIcebergProperty("iceberg.rest-catalog.vended-credentials-enabled", "true")
                .addIcebergProperty("s3.region", MINIO_REGION)
                .addIcebergProperty("s3.endpoint", polarisCatalog.minio().getMinioAddress())
                .addIcebergProperty("s3.path-style-access", "true")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setRegion(MINIO_REGION)
                        .setEndpoint(polarisCatalog.minio().getMinioAddress())
                        .setPathStyleAccess(true)
                        .setAwsAccessKey(MINIO_ROOT_USER)
                        .setAwsSecretKey(MINIO_ROOT_PASSWORD),
                new S3FileSystemStats()).create(SESSION);
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        polarisCatalog.dropTable(getSession().getSchema().orElseThrow(), tableName);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return loadTable(tableName).operations().current().metadataFileLocation();
    }

    @Override
    protected String getTableLocation(String tableName)
    {
        return loadTable(tableName).operations().current().location();
    }

    private BaseTable loadTable(String tableName)
    {
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        TrinoCatalog trinoCatalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
        return trinoCatalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
    }

    @Override
    protected String schemaPath()
    {
        return format("%s%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        try (MinioClient minioClient = polarisCatalog.minio().createMinioClient()) {
            String prefix = "s3://" + bucketName + "/";
            String key = location.substring(prefix.length());
            return !minioClient.listObjects(bucketName, key).isEmpty();
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void testNestedNamespace()
    {
        String parentNamespace = "level1_" + randomNameSuffix();
        String nestedNamespace = parentNamespace + ".level2_" + randomNameSuffix();

        assertUpdate("CREATE SCHEMA " + parentNamespace);
        assertUpdate("CREATE SCHEMA \"" + nestedNamespace + "\"");
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains(parentNamespace, nestedNamespace);

        assertUpdate("DROP SCHEMA \"" + nestedNamespace + "\"");
        assertUpdate("DROP SCHEMA " + parentNamespace);
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
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
    public void testRegisterTableWithTableLocation()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testUnregisterTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRepeatUnregisterTable()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
            throws Exception
    {
        try {
            super.testDropTableWithMissingDataFile();
        }
        catch (AssertionError e) {
            assertThat(e).hasMessageContaining("Table location should not exist");
        }
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        // register_table procedure with vended credentials is currently not supported
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .hasStackTraceContaining("Expecting value to be false but was true");
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Table location should not exist");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        // TODO: Fix https://github.com/trinodb/trino/issues/23941
        abort("Skipped for now due to #23941");
    }
}
