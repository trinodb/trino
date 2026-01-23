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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.execution.QueryManagerConfig;
import io.trino.filesystem.Location;
import io.trino.operator.OperatorStats;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.getParquetFileMetadata;
import static io.trino.plugin.iceberg.IcebergTestUtils.withSmallRowGroups;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergParquetConnectorTest
        extends BaseIcebergConnectorTest
{
    public TestIcebergParquetConnectorTest()
    {
        super(PARQUET);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return true;
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary") ||
                typeName.equalsIgnoreCase("time") ||
                typeName.equalsIgnoreCase("time(6)") ||
                typeName.equalsIgnoreCase("timestamp(3) with time zone") ||
                typeName.equalsIgnoreCase("timestamp(6) with time zone"));
    }

    @Test
    public void testRowGroupResetDictionary()
    {
        try (TestTable table = newTrinoTable(
                "test_row_group_reset_dictionary",
                "(plain_col varchar, dict_col int)")) {
            String tableName = table.getName();
            String values = IntStream.range(0, 100)
                    .mapToObj(i -> "('ABCDEFGHIJ" + i + "' , " + (i < 20 ? "1" : "null") + ")")
                    .collect(Collectors.joining(", "));
            assertUpdate(withSmallRowGroups(getSession()), "INSERT INTO " + tableName + " VALUES " + values, 100);

            MaterializedResult result = getDistributedQueryRunner().execute("SELECT * FROM " + tableName);
            assertThat(result.getRowCount()).isEqualTo(100);
        }
    }

    @Override
    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        switch ("%s -> %s".formatted(setup.sourceColumnType(), setup.newColumnType())) {
            case "row(x integer) -> row(\"y\" integer)":
                // TODO https://github.com/trinodb/trino/issues/15822 The connector returns incorrect NULL when a field in row type doesn't exist in Parquet files
                return Optional.of(setup.withNewValueLiteral("NULL"));
        }
        return super.filterSetColumnTypesDataProvider(setup);
    }

    @Test
    public void testIgnoreParquetStatistics()
    {
        try (TestTable table = newTrinoTable(
                "test_ignore_parquet_statistics",
                "WITH (sorted_by = ARRAY['custkey']) AS TABLE tpch.tiny.customer WITH NO DATA")) {
            assertUpdate(
                    withSmallRowGroups(getSession()),
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.customer",
                    "VALUES 1500");

            @Language("SQL") String query = "SELECT * FROM " + table.getName() + " WHERE custkey = 100";

            QueryRunner queryRunner = getDistributedQueryRunner();
            MaterializedResultWithPlan resultWithoutParquetStatistics = queryRunner.executeWithPlan(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "parquet_ignore_statistics", "true")
                            .build(),
                    query);
            OperatorStats queryStatsWithoutParquetStatistics = getOperatorStats(resultWithoutParquetStatistics.queryId());
            assertThat(queryStatsWithoutParquetStatistics.getPhysicalInputPositions()).isGreaterThan(0);

            MaterializedResultWithPlan resultWithParquetStatistics = queryRunner.executeWithPlan(getSession(), query);
            OperatorStats queryStatsWithParquetStatistics = getOperatorStats(resultWithParquetStatistics.queryId());
            assertThat(queryStatsWithParquetStatistics.getPhysicalInputPositions()).isGreaterThan(0);
            assertThat(queryStatsWithParquetStatistics.getPhysicalInputPositions())
                    .isLessThan(queryStatsWithoutParquetStatistics.getPhysicalInputPositions());

            assertEqualsIgnoreOrder(resultWithParquetStatistics.result(), resultWithoutParquetStatistics.result());
        }
    }

    @Test
    public void testPushdownPredicateToParquetAfterColumnRename()
    {
        try (TestTable table = newTrinoTable(
                "test_pushdown_predicate_statistics",
                "WITH (sorted_by = ARRAY['custkey']) AS TABLE tpch.tiny.customer WITH NO DATA")) {
            assertUpdate(
                    withSmallRowGroups(getSession()),
                    "INSERT INTO " + table.getName() + " TABLE tpch.tiny.customer",
                    "VALUES 1500");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN custkey TO custkey1");

            QueryRunner queryRunner = getDistributedQueryRunner();
            MaterializedResultWithPlan resultWithoutPredicate = queryRunner.executeWithPlan(getSession(), "TABLE " + table.getName());
            OperatorStats queryStatsWithoutPredicate = getOperatorStats(resultWithoutPredicate.queryId());
            assertThat(queryStatsWithoutPredicate.getPhysicalInputPositions()).isGreaterThan(0);
            assertThat(resultWithoutPredicate.result()).hasSize(1500);

            @Language("SQL") String selectiveQuery = "SELECT * FROM " + table.getName() + " WHERE custkey1 = 100";
            MaterializedResultWithPlan selectiveQueryResult = queryRunner.executeWithPlan(getSession(), selectiveQuery);
            OperatorStats queryStatsSelectiveQuery = getOperatorStats(selectiveQueryResult.queryId());
            assertThat(queryStatsSelectiveQuery.getPhysicalInputPositions()).isGreaterThan(0);
            assertThat(queryStatsSelectiveQuery.getPhysicalInputPositions())
                    .isLessThan(queryStatsWithoutPredicate.getPhysicalInputPositions());
            assertThat(selectiveQueryResult.result()).hasSize(1);
        }
    }

    @Test
    void testTableChangesOnMultiRowGroups()
            throws Exception
    {
        try (TestTable table = newTrinoTable(
                "test_table_changes_function_multi_row_groups_",
                "AS SELECT orderkey, partkey, suppkey FROM tpch.tiny.lineitem WITH NO DATA")) {
            long initialSnapshot = getMostRecentSnapshotId(table.getName());
            assertUpdate(
                    withSmallRowGroups(getSession()),
                    "INSERT INTO %s SELECT orderkey, partkey, suppkey FROM tpch.tiny.lineitem".formatted(table.getName()),
                    60175L);
            long snapshotAfterInsert = getMostRecentSnapshotId(table.getName());
            DateTimeFormatter instantMillisFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSVV").withZone(UTC);
            String snapshotAfterInsertTime = getSnapshotTime(table.getName(), snapshotAfterInsert).format(instantMillisFormatter);

            // make sure splits are processed in more than one batch
            // Decrease parquet row groups size or add more columns if this test fails
            String filePath = getOnlyTableFilePath(table.getName());
            ParquetMetadata parquetMetadata = getParquetFileMetadata(fileSystem.newInputFile(Location.of(filePath)));
            int blocksSize = parquetMetadata.getBlocks().size();
            int splitBatchSize = new QueryManagerConfig().getScheduleSplitBatchSize();
            assertThat(blocksSize > splitBatchSize && blocksSize % splitBatchSize != 0).isTrue();

            assertQuery(
                    """
                            SELECT orderkey, partkey, suppkey, _change_type, _change_version_id, to_iso8601(_change_timestamp), _change_ordinal
                            FROM TABLE(system.table_changes(CURRENT_SCHEMA, '%s', %s, %s))
                            """.formatted(table.getName(), initialSnapshot, snapshotAfterInsert),
                    "SELECT orderkey, partkey, suppkey, 'insert', %s, '%s', 0 FROM lineitem".formatted(snapshotAfterInsert, snapshotAfterInsertTime));
        }
    }

    private String getOnlyTableFilePath(String tableName)
    {
        return (String) computeScalar("SELECT file_path FROM \"" + tableName + "$files\"");
    }

    private long getMostRecentSnapshotId(String tableName)
    {
        return (long) computeScalar("SELECT snapshot_id FROM \"" + tableName + "$snapshots\" ORDER BY committed_at DESC LIMIT 1");
    }

    private ZonedDateTime getSnapshotTime(String tableName, long snapshotId)
    {
        return (ZonedDateTime) computeScalar("SELECT committed_at FROM \"" + tableName + "$snapshots\" WHERE snapshot_id = " + snapshotId);
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(Location.of(path)), sortColumnName);
    }
}
