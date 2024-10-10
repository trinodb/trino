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

import com.databricks.client.jdbc.Driver;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // Don't run tests in parallel for mitigating rate limit of Databricks REST API
final class TestIcebergUnityRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final Logger log = Logger.get(TestIcebergUnityRestCatalogConnectorSmokeTest.class);

    private static final RetryPolicy<?> RETRY_POLICY = RetryPolicy.builder()
            .handleIf(throwable -> throwable.getMessage().contains("HTTP Response code: 502"))
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(60)
            .onRetry(event -> log.warn(event.getLastException(), "Query failed on attempt %d, will retry.", event.getAttemptCount()))
            .build();

    private final File warehouseLocation;
    private final String databricksHost;
    private final String databricksLogin;
    private final String databricksToken;
    private final String databricksUnityJdbcUrl;
    private final String databricksCatalogName;
    private final String s3Region;
    private final String s3AccessKey;
    private final String s3SecretKey;

    public TestIcebergUnityRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        warehouseLocation = Files.newTemporaryFolder();

        databricksHost = requireEnv("DATABRICKS_HOST");
        databricksLogin = requireEnv("DATABRICKS_LOGIN");
        databricksToken = requireEnv("DATABRICKS_TOKEN");
        databricksUnityJdbcUrl = requireEnv("DATABRICKS_UNITY_JDBC_URL") + ";EnableArrow=0";
        databricksCatalogName = requireEnv("DATABRICKS_UNITY_CATALOG_NAME");

        s3Region = requireEnv("AWS_REGION");
        s3AccessKey = requireEnv("AWS_ACCESS_KEY_ID");
        s3SecretKey = requireEnv("AWS_SECRET_ACCESS_KEY");
    }

    private static String requireEnv(String variable)
    {
        return requireNonNull(System.getenv(variable), () -> "environment variable not set: " + variable);
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
        closeAfterClass(() -> deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE));

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.security", "read_only")
                                .put("iceberg.rest-catalog.uri", "https://%s:443/api/2.1/unity-catalog/iceberg" .formatted(databricksHost))
                                .put("iceberg.rest-catalog.namespace", databricksCatalogName)
                                .put("iceberg.rest-catalog.security", "OAUTH2")
                                .put("iceberg.rest-catalog.oauth2.token", databricksToken)
                                .put("iceberg.register-table-procedure.enabled", "true")
                                .put("fs.native-s3.enabled", "true")
                                .put("s3.region", s3Region)
                                .put("s3.aws-access-key", s3AccessKey)
                                .put("s3.aws-secret-key", s3SecretKey)
                                .buildOrThrow())
                .disableSchemaInitializer()
                .build();

        for (TpchTable<?> table : REQUIRED_TPCH_TABLES) {
            copyTpchTable(queryRunner, table.getTableName());
        }

        return queryRunner;
    }

    private void copyTpchTable(DistributedQueryRunner queryRunner, String tableName)
    {
        if (isLoaded(queryRunner, tableName)) {
            return;
        }

        Properties properties = new Properties();
        properties.setProperty("user", databricksLogin);
        properties.setProperty("password", databricksToken);

        try (Connection connection = new Driver().connect(databricksUnityJdbcUrl, properties);
                Statement statement = connection.createStatement()) {
            Failsafe.with(RETRY_POLICY).run(() -> statement.execute("DROP TABLE IF EXISTS main.tpch." + tableName));

            String columns = switch (tableName) {
                case "region" -> "r_regionkey AS regionkey, r_name AS name, r_comment AS comment";
                case "nation" -> "n_nationkey AS nationkey, n_name AS name, n_regionkey AS regionkey, n_comment AS comment";
                default -> throw new IllegalArgumentException("Unexpected table name: " + tableName);
            };

            String createTableSql = "CREATE TABLE main.tpch." + tableName + " " +
                    "USING DELTA " +
                    "TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg') AS " +
                    "SELECT " + columns + " FROM samples.tpch." + tableName;

            Failsafe.with(RETRY_POLICY).run(() -> statement.execute(createTableSql));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isLoaded(DistributedQueryRunner queryRunner, String tableName)
    {
        try {
            long actual = (long) queryRunner.execute("SELECT COUNT(*) FROM " + tableName).getOnlyValue();
            long expected = (long) queryRunner.execute("SELECT COUNT(*) FROM tpch.tiny." + tableName).getOnlyValue();
            return actual == expected;
        }
        catch (Exception e) {
            return false;
        }
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String schemaPath()
    {
        return format("%s/%s", warehouseLocation, getSession().getSchema());
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
        try {
            deleteRecursively(Path.of(location), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override // Overridden as the table location and column data type is different
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE " + TpchTable.REGION.getTableName()))
                .matches(""
                        + "CREATE TABLE iceberg.tpch.region \\(\n"
                        + "   regionkey bigint,\n"
                        + "   name varchar,\n"
                        + "   comment varchar\n"
                        + "\\)\n"
                        + "WITH \\(\n"
                        + "   format = 'PARQUET',\n"
                        + "   format_version = 1,\n"
                        + "   location = 's3://.*'\n"
                        + "\\)");
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testHiddenPathColumn()
    {
        assertThatThrownBy(super::testHiddenPathColumn)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        assertThatThrownBy(super::testDeleteAllDataFromTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        assertThatThrownBy(super::testDeleteRowsConcurrently)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateOrReplaceTable()
    {
        assertThatThrownBy(super::testCreateOrReplaceTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        assertThatThrownBy(super::testCreateOrReplaceTableChangeColumnNamesAndTypes)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testCreateTableWithTrailingSpaceInLocation)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testUnregisterBrokenTable()
    {
        assertThatThrownBy(super::testUnregisterBrokenTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testUnregisterTableNotExistingTable()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingTable)
                .hasStackTraceContaining("Table .* not found");
    }

    @Test
    @Override
    public void testUnregisterTableNotExistingSchema()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingSchema)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasStackTraceContaining("Table .* not found");
    }

    @Test
    @Override
    public void testUnregisterTableAccessControl()
    {
        assertThatThrownBy(super::testUnregisterTableAccessControl)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTableWithNonExistingSchemaVerifyLocation()
    {
        assertThatThrownBy(super::testCreateTableWithNonExistingSchemaVerifyLocation)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testSortedNationTable()
    {
        assertThatThrownBy(super::testSortedNationTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testFileSortingWithLargerTable()
    {
        assertThatThrownBy(super::testFileSortingWithLargerTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingDataFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testMetadataTables()
    {
        assertThatThrownBy(super::testMetadataTables)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testPartitionFilterRequired()
    {
        assertThatThrownBy(super::testPartitionFilterRequired)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testTableChangesFunction()
    {
        assertThatThrownBy(super::testTableChangesFunction)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRowLevelDeletesWithTableChangesFunction()
    {
        assertThatThrownBy(super::testRowLevelDeletesWithTableChangesFunction)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateOrReplaceWithTableChangesFunction()
    {
        assertThatThrownBy(super::testCreateOrReplaceWithTableChangesFunction)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testTruncateTable()
    {
        assertThatThrownBy(super::testTruncateTable)
                .hasMessageContaining("Access Denied");
    }
}
