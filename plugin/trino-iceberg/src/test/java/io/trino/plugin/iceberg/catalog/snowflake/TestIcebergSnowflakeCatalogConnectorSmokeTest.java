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
package io.trino.plugin.iceberg.catalog.snowflake;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.SQLException;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_JDBC_URI;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_PASSWORD;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_ROLE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_TEST_DATABASE;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.SNOWFLAKE_USER;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.TableType.ICEBERG;
import static io.trino.plugin.iceberg.catalog.snowflake.TestingSnowflakeServer.TableType.NATIVE;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.util.Locale.ENGLISH;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergSnowflakeCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    public static final String S3_ACCESS_KEY = requiredNonEmptySystemProperty("testing.snowflake.catalog.s3.access-key");
    public static final String S3_SECRET_KEY = requiredNonEmptySystemProperty("testing.snowflake.catalog.s3.secret-key");
    public static final String S3_REGION = requiredNonEmptySystemProperty("testing.snowflake.catalog.s3.region");

    public static final String SNOWFLAKE_S3_EXTERNAL_VOLUME = requiredNonEmptySystemProperty("testing.snowflake.catalog.s3.external.volume");
    public static final String SNOWFLAKE_TEST_SCHEMA = requiredNonEmptySystemProperty("testing.snowflake.catalog.schema");

    private TestingSnowflakeServer server;

    public TestIcebergSnowflakeCatalogConnectorSmokeTest()
    {
        super(PARQUET);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        server = new TestingSnowflakeServer();
        server.execute(SNOWFLAKE_TEST_SCHEMA, "CREATE SCHEMA IF NOT EXISTS %s".formatted(SNOWFLAKE_TEST_SCHEMA));
        if (!server.checkIfTableExists(ICEBERG, SNOWFLAKE_TEST_SCHEMA, TpchTable.NATION.getTableName())) {
            executeOnSnowflake("""
                    CREATE OR REPLACE ICEBERG TABLE %s (
                    	NATIONKEY NUMBER(38,0),
                    	NAME STRING,
                    	REGIONKEY NUMBER(38,0),
                    	COMMENT STRING
                    )
                     EXTERNAL_VOLUME = '%s'
                     CATALOG = 'SNOWFLAKE'
                     BASE_LOCATION = '%s/'""".formatted(TpchTable.NATION.getTableName(), SNOWFLAKE_S3_EXTERNAL_VOLUME, TpchTable.NATION.getTableName()));

            executeOnSnowflake("INSERT INTO %s(NATIONKEY, NAME, REGIONKEY, COMMENT) SELECT N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.%s"
                    .formatted(TpchTable.NATION.getTableName(), TpchTable.NATION.getTableName()));
        }
        if (!server.checkIfTableExists(ICEBERG, SNOWFLAKE_TEST_SCHEMA, TpchTable.REGION.getTableName())) {
            executeOnSnowflake("""
                    CREATE OR REPLACE ICEBERG TABLE %s (
                    	REGIONKEY NUMBER(38,0),
                    	NAME STRING,
                    	COMMENT STRING
                    )
                     EXTERNAL_VOLUME = '%s'
                     CATALOG = 'SNOWFLAKE'
                     BASE_LOCATION = '%s/'""".formatted(TpchTable.REGION.getTableName(), SNOWFLAKE_S3_EXTERNAL_VOLUME, TpchTable.REGION.getTableName()));

            executeOnSnowflake("INSERT INTO %s(REGIONKEY, NAME, COMMENT) SELECT R_REGIONKEY, R_NAME, R_COMMENT FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.%s"
                    .formatted(TpchTable.REGION.getTableName(), TpchTable.REGION.getTableName()));
        }

        ImmutableMap<String, String> properties = ImmutableMap.<String, String>builder()
                .put("fs.native-s3.enabled", "true")
                .put("s3.aws-access-key", S3_ACCESS_KEY)
                .put("s3.aws-secret-key", S3_SECRET_KEY)
                .put("s3.region", S3_REGION)
                .put("iceberg.file-format", "parquet") // only Parquet is supported
                .put("iceberg.catalog.type", "snowflake")
                .put("iceberg.snowflake-catalog.role", SNOWFLAKE_ROLE)
                .put("iceberg.snowflake-catalog.database", SNOWFLAKE_TEST_DATABASE)
                .put("iceberg.snowflake-catalog.account-uri", SNOWFLAKE_JDBC_URI)
                .put("iceberg.snowflake-catalog.user", SNOWFLAKE_USER)
                .put("iceberg.snowflake-catalog.password", SNOWFLAKE_PASSWORD)
                .buildOrThrow();

        return IcebergQueryRunner.builder(SNOWFLAKE_TEST_SCHEMA.toLowerCase(ENGLISH))
                .setIcebergProperties(properties)
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withSchemaName(SNOWFLAKE_TEST_SCHEMA.toLowerCase(ENGLISH))
                                .build())
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_TABLE,
                    SUPPORTS_DELETE,
                    SUPPORTS_INSERT,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_CREATE_SCHEMA,
                    SUPPORTS_MERGE,
                    SUPPORTS_UPDATE,
                    SUPPORTS_RENAME_TABLE,
                    SUPPORTS_ROW_LEVEL_UPDATE,
                    SUPPORTS_ROW_LEVEL_DELETE,
                    SUPPORTS_CREATE_OR_REPLACE_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_DATA,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_COMMENT_ON_VIEW -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("This connector does not support creating views");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("This connector does not support creating materialized views");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("This connector does not support renaming schemas");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasStackTraceContaining("This connector does not support renaming tables");
    }

    // Overridden as the table location and column data type is different
    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE " + TpchTable.REGION.getTableName()))
                .matches(""
                        + "CREATE TABLE iceberg." + SNOWFLAKE_TEST_SCHEMA.toLowerCase(ENGLISH) + ".%s \\(\n".formatted(TpchTable.REGION.getTableName())
                        + "   regionkey decimal\\(38, 0\\),\n"
                        + "   name varchar,\n"
                        + "   comment varchar\n"
                        + "\\)\n"
                        + "WITH \\(\n"
                        + "   format = 'PARQUET',\n"
                        + "   format_version = 2,\n"
                        + "   location = 's3://.*/%s'\n".formatted(TpchTable.REGION.getTableName())
                        + "\\)");
    }

    @Test
    @Override
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testHiddenPathColumn()
    {
        assertThatThrownBy(super::testHiddenPathColumn)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        assertThatThrownBy(super::testDeleteRowsConcurrently)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testCreateOrReplaceTable()
    {
        assertThatThrownBy(super::testCreateOrReplaceTable)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        assertThatThrownBy(super::testCreateOrReplaceTableChangeColumnNamesAndTypes)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessageContaining("This connector does not support creating schemas");
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testCreateTableWithTrailingSpaceInLocation)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasStackTraceContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testUnregisterBrokenTable()
    {
        assertThatThrownBy(super::testUnregisterBrokenTable)
                .hasStackTraceContaining("Snowflake managed Iceberg tables do not support modifications");
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
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testCreateTableWithNonExistingSchemaVerifyLocation()
    {
        assertThatThrownBy(super::testCreateTableWithNonExistingSchemaVerifyLocation)
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @Override
    public void testSortedNationTable()
    {
        assertThatThrownBy(super::testSortedNationTable)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testFileSortingWithLargerTable()
    {
        assertThatThrownBy(super::testFileSortingWithLargerTable)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingDataFile)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testMetadataTables()
    {
        assertThatThrownBy(super::testMetadataTables)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testPartitionFilterRequired()
    {
        assertThatThrownBy(super::testPartitionFilterRequired)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testTableChangesFunction()
    {
        assertThatThrownBy(super::testTableChangesFunction)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testRowLevelDeletesWithTableChangesFunction()
    {
        assertThatThrownBy(super::testRowLevelDeletesWithTableChangesFunction)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    @Override
    public void testCreateOrReplaceWithTableChangesFunction()
    {
        assertThatThrownBy(super::testCreateOrReplaceWithTableChangesFunction)
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void testNation()
    {
        assertQuery("SELECT count(*) FROM " + TpchTable.NATION.getTableName(), "VALUES 25");
        assertTableColumnNames(TpchTable.NATION.getTableName(), "nationkey", "name", "regionkey", "comment");
    }

    @Test
    public void testListTables()
    {
        assertThat(computeActual("SHOW TABLES").getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .toList()).contains(TpchTable.REGION.getTableName(), TpchTable.NATION.getTableName());
    }

    @Test
    public void testRegion()
    {
        assertQuery("SELECT count(*) FROM " + TpchTable.REGION.getTableName(), "VALUES 5");
        assertTableColumnNames(TpchTable.REGION.getTableName(), "regionkey", "name", "comment");
        assertQuery("SELECT name FROM " + TpchTable.REGION.getTableName(), "VALUES ('AFRICA'), ('AMERICA'), ('ASIA'), ('EUROPE'), ('MIDDLE EAST')");
    }

    @Test
    public void testSetTableComment()
    {
        assertThatThrownBy(() -> assertUpdate("COMMENT ON TABLE " + TpchTable.REGION.getTableName() + " is 'my-table-comment'"))
                .hasMessage("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void testSetViewComment()
    {
        assertThatThrownBy(() -> assertUpdate("COMMENT ON VIEW temp_view is 'my-table-comment'"))
                .hasMessageMatching("line 1:1: View '.*' does not exist");
    }

    @Test
    public void testSetViewColumnComment()
    {
        assertThatThrownBy(() -> assertUpdate("COMMENT ON COLUMN temp_view.col1 is 'my-column-comment'"))
                .hasMessageMatching(".*Table does not exist: .*temp_view");
    }

    @Test
    public void testSetMaterializedViewColumnComment()
    {
        assertThatThrownBy(() -> assertUpdate("COMMENT ON COLUMN temp_view.col1 is 'my-column-comment'"))
                .hasMessageMatching(".*Table does not exist: .*temp_view");
    }

    @Test
    public void testSetTableProperties()
    {
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + TpchTable.REGION.getTableName() + " SET PROPERTIES format_version = 2"))
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void testAddColumn()
    {
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + TpchTable.REGION.getTableName() + " ADD COLUMN zip varchar"))
                .hasMessageMatching("Failed to add column: Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void testDropColumn()
    {
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + TpchTable.REGION.getTableName() + " DROP COLUMN name"))
                .hasMessageMatching("Failed to drop column: Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void testRenameColumn()
    {
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + TpchTable.REGION.getTableName() + " RENAME COLUMN name TO new_name"))
                .hasMessageMatching("Failed to rename column: Snowflake managed Iceberg tables do not support modifications");
    }
    @Test
    public void testBeginStatisticsCollection()
    {
        assertThatThrownBy(() -> assertUpdate("ANALYZE " + TpchTable.REGION.getTableName()))
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void testCreateView()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE VIEW temp_view AS SELECT * FROM " + TpchTable.REGION.getTableName()))
                .hasMessage("Views are not supported for the Snowflake Iceberg catalog");
    }

    @Test
    public void testRenameView()
    {
        assertThatThrownBy(() -> assertUpdate("ALTER VIEW non_existing_view RENAME TO existing_view"))
                .hasMessageMatching("line 1:1: View '.*' does not exist");
    }

    @Test
    public void testDropView()
    {
        assertThatThrownBy(() -> assertUpdate("DROP VIEW non_existing_view"))
                .hasMessageMatching("line 1:1: View '.*' does not exist");
    }

    @Test
    public void testListViews()
    {
        assertQuery("SELECT count(*) FROM information_schema.views", "VALUES 0");
    }

    @Test
    public void testExecuteDelete()
    {
        assertThatThrownBy(() -> assertUpdate("DELETE FROM " + TpchTable.REGION.getTableName()))
                .hasMessageMatching("Failed to close manifest writer");
    }

    @Test
    public void testGetTableStatistics()
    {
        assertQuery(
                "SHOW STATS FOR " + TpchTable.NATION.getTableName(),
                          """
                          VALUES
                          ('nationkey', null, null, 0, null, 0.0, '24.0'),
                          ('name', null, null, 0, null, null, null),
                          ('regionkey', null, null, 0, null, 0.0, '4.0'),
                          ('comment', null, null, 0, null, null, null),
                          (null, null, null, null, 25, null, null)""");
    }

    @Test
    public void testCreateMaterializedView()
    {
        assertThatThrownBy(() -> assertUpdate("CREATE MATERIALIZED VIEW mv_orders AS SELECT * FROM orders"))
                .hasMessageMatching(".* Table '.*orders' does not exist");
    }

    @Test
    public void testDropMaterializedView()
    {
        assertThatThrownBy(() -> assertUpdate("DROP MATERIALIZED VIEW mv_orders"))
                .hasMessageMatching(".*Materialized view '.*mv_orders' does not exist");
    }

    @Test
    public void testListMaterializedViews()
    {
        assertQuery("SELECT count(*) FROM information_schema.views", "VALUES 0");
    }

    @Test
    public void testRenameMaterializedView()
    {
        assertThatThrownBy(() -> assertUpdate("ALTER MATERIALIZED VIEW mv_orders RENAME TO mv_new_orders"))
                .hasMessageMatching(".*Materialized View '.*mv_orders' does not exist");
    }

    @Test
    public void testSetColumnComment()
    {
        assertThatThrownBy(() -> assertUpdate("COMMENT ON COLUMN " + TpchTable.REGION.getTableName() + ".name IS 'region name_col_comment'"))
                .hasMessageMatching("Snowflake managed Iceberg tables do not support modifications");
    }

    @Test
    public void testSnowflakeNativeTable()
            throws SQLException
    {
        String snowflakeNativeTableName = "snowflake_native_nation";
        if (!server.checkIfTableExists(NATIVE, SNOWFLAKE_TEST_SCHEMA, snowflakeNativeTableName)) {
            executeOnSnowflake("CREATE TABLE %s IF NOT EXISTS AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.%s"
                    .formatted(snowflakeNativeTableName, TpchTable.NATION.getTableName()));
        }
        assertThatThrownBy(() -> assertQuery("SELECT count(*) FROM " + snowflakeNativeTableName))
                .hasCauseInstanceOf(QueryFailedException.class)
                .hasRootCauseMessage("SQL compilation error:\ninvalid parameter 'table ? is not a Snowflake iceberg table'");
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        throw new UnsupportedOperationException("Only PARQUET file format is supported for Iceberg Snowflake catalogs");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        throw new UnsupportedOperationException("deleteDirectory is not supported for Iceberg snowflake catalog");
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        // used for register table, which is not supported for Iceberg Snowflake catalogs
        throw new UnsupportedOperationException("dropTableFromMetastore is not supported for Iceberg snowflake catalog");
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        // used for register table, which is not supported for Iceberg Snowflake catalogs
        throw new UnsupportedOperationException("getMetadataLocation is not supported for Iceberg snowflake catalog");
    }

    @Override
    protected String schemaPath()
    {
        throw new UnsupportedOperationException("schemaPath is not supported for Iceberg snowflake catalog");
    }

    @Override
    protected boolean locationExists(String location)
    {
        throw new UnsupportedOperationException("locationExists is not supported for Iceberg snowflake catalog");
    }

    private void executeOnSnowflake(String sql)
            throws SQLException
    {
        server.execute(SNOWFLAKE_TEST_SCHEMA, sql);
    }
}
