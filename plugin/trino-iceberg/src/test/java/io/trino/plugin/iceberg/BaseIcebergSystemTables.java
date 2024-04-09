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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseIcebergSystemTables
        extends AbstractTestQueryFramework
{
    private final IcebergFileFormat format;

    protected BaseIcebergSystemTables(IcebergFileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of("iceberg.file-format", format.name()))
                .build();
    }

    @BeforeAll
    public void setUp()
    {
        assertUpdate("CREATE SCHEMA test_schema");
        assertUpdate("CREATE TABLE test_schema.test_table (_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table", "VALUES 6");

        assertUpdate("CREATE TABLE test_schema.test_table_multilevel_partitions (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_bigint', '_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_multilevel_partitions VALUES ('a', 0, CAST('2019-09-08' AS DATE)), ('a', 1, CAST('2019-09-08' AS DATE)), ('a', 0, CAST('2019-09-09' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_multilevel_partitions", "VALUES 3");

        assertUpdate("CREATE TABLE test_schema.test_table_drop_column (_varchar VARCHAR, _bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_drop_column VALUES ('a', 0, CAST('2019-09-08' AS DATE)), ('a', 1, CAST('2019-09-09' AS DATE)), ('b', 2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table_drop_column VALUES ('c', 3, CAST('2019-09-09' AS DATE)), ('a', 4, CAST('2019-09-10' AS DATE)), ('b', 5, CAST('2019-09-10' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_drop_column", "VALUES 6");
        assertUpdate("ALTER TABLE test_schema.test_table_drop_column DROP COLUMN _varchar");

        assertUpdate("CREATE TABLE test_schema.test_table_nan (_bigint BIGINT, _double DOUBLE, _real REAL, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_nan VALUES (1, 1.1, 1.2, CAST('2022-01-01' AS DATE)), (2, nan(), 2.2, CAST('2022-01-02' AS DATE)), (3, 3.3, nan(), CAST('2022-01-03' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table_nan VALUES (4, nan(), 4.1, CAST('2022-01-04' AS DATE)), (5, 4.2, nan(), CAST('2022-01-04' AS DATE)), (6, nan(), nan(), CAST('2022-01-04' AS DATE))", 3);
        assertQuery("SELECT count(*) FROM test_schema.test_table_nan", "VALUES 6");

        assertUpdate("CREATE TABLE test_schema.test_table_with_dml (_varchar VARCHAR, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate(
                "INSERT INTO test_schema.test_table_with_dml " +
                        "VALUES " +
                        "('a1', DATE '2022-01-01'), ('a2', DATE '2022-01-01'), " +
                        "('b1', DATE '2022-02-02'), ('b2', DATE '2022-02-02'), " +
                        "('c1', DATE '2022-03-03'), ('c2', DATE '2022-03-03')",
                6);
        assertUpdate("UPDATE test_schema.test_table_with_dml SET _varchar = 'a1.updated' WHERE _date = DATE '2022-01-01' AND _varchar = 'a1'", 1);
        assertUpdate("DELETE FROM test_schema.test_table_with_dml WHERE _date = DATE '2022-02-02' AND _varchar = 'b2'", 1);
        assertUpdate("INSERT INTO test_schema.test_table_with_dml VALUES ('c3', DATE '2022-03-03'), ('d1', DATE '2022-04-04')", 2);
        assertQuery("SELECT count(*) FROM test_schema.test_table_with_dml", "VALUES 7");
    }

    @AfterAll
    public void tearDown()
    {
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_multilevel_partitions");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_drop_column");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_nan");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_with_dml");
        assertUpdate("DROP TABLE IF EXISTS test_schema.test_metadata_log_entries");
        assertUpdate("DROP SCHEMA IF EXISTS test_schema");
    }

    @Test
    public void testPartitionTable()
    {
        assertQuery("SELECT count(*) FROM test_schema.test_table", "VALUES 6");
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$partitions\"",
                "VALUES ('partition', 'row(_date date)', '', '')," +
                        "('record_count', 'bigint', '', '')," +
                        "('file_count', 'bigint', '', '')," +
                        "('total_size', 'bigint', '', '')," +
                        "('data', 'row(_bigint row(min bigint, max bigint, null_count bigint, nan_count bigint))', '', '')");

        MaterializedResult result = computeActual("SELECT * from test_schema.\"test_table$partitions\"");
        assertThat(result.getRowCount()).isEqualTo(3);

        Map<LocalDate, MaterializedRow> rowsByPartition = result.getMaterializedRows().stream()
                .collect(toImmutableMap(row -> ((LocalDate) ((MaterializedRow) row.getField(0)).getField(0)), Function.identity()));

        // Test if row counts are computed correctly
        assertThat(rowsByPartition.get(LocalDate.parse("2019-09-08")).getField(1)).isEqualTo(1L);
        assertThat(rowsByPartition.get(LocalDate.parse("2019-09-09")).getField(1)).isEqualTo(3L);
        assertThat(rowsByPartition.get(LocalDate.parse("2019-09-10")).getField(1)).isEqualTo(2L);

        // Test if min/max values, null value count and nan value count are computed correctly.
        assertThat(rowsByPartition.get(LocalDate.parse("2019-09-08")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, new MaterializedRow(DEFAULT_PRECISION, 0L, 0L, 0L, null)));
        assertThat(rowsByPartition.get(LocalDate.parse("2019-09-09")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, new MaterializedRow(DEFAULT_PRECISION, 1L, 3L, 0L, null)));
        assertThat(rowsByPartition.get(LocalDate.parse("2019-09-10")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION, new MaterializedRow(DEFAULT_PRECISION, 4L, 5L, 0L, null)));
    }

    @Test
    public void testPartitionTableWithNan()
    {
        assertQuery("SELECT count(*) FROM test_schema.test_table_nan", "VALUES 6");

        MaterializedResult result = computeActual("SELECT * from test_schema.\"test_table_nan$partitions\"");
        assertThat(result.getRowCount()).isEqualTo(4);

        Map<LocalDate, MaterializedRow> rowsByPartition = result.getMaterializedRows().stream()
                .collect(toImmutableMap(row -> ((LocalDate) ((MaterializedRow) row.getField(0)).getField(0)), Function.identity()));

        // Test if row counts are computed correctly
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-01")).getField(1)).isEqualTo(1L);
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-02")).getField(1)).isEqualTo(1L);
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-03")).getField(1)).isEqualTo(1L);
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-04")).getField(1)).isEqualTo(3L);

        // Test if min/max values, null value count and nan value count are computed correctly.
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-01")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                new MaterializedRow(DEFAULT_PRECISION, 1L, 1L, 0L, null),
                new MaterializedRow(DEFAULT_PRECISION, 1.1d, 1.1d, 0L, null),
                new MaterializedRow(DEFAULT_PRECISION, 1.2f, 1.2f, 0L, null)));
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-02")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                new MaterializedRow(DEFAULT_PRECISION, 2L, 2L, 0L, null),
                new MaterializedRow(DEFAULT_PRECISION, null, null, 0L, nanCount(1L)),
                new MaterializedRow(DEFAULT_PRECISION, 2.2f, 2.2f, 0L, null)));
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-03")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                new MaterializedRow(DEFAULT_PRECISION, 3L, 3L, 0L, null),
                new MaterializedRow(DEFAULT_PRECISION, 3.3, 3.3d, 0L, null),
                new MaterializedRow(DEFAULT_PRECISION, null, null, 0L, nanCount(1L))));
        assertThat(rowsByPartition.get(LocalDate.parse("2022-01-04")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                new MaterializedRow(DEFAULT_PRECISION, 4L, 6L, 0L, null),
                new MaterializedRow(DEFAULT_PRECISION, null, null, 0L, nanCount(2L)),
                new MaterializedRow(DEFAULT_PRECISION, null, null, 0L, nanCount(2L))));
    }

    @Test
    public void testPartitionTableOnDropColumn()
    {
        MaterializedResult resultAfterDrop = computeActual("SELECT * from test_schema.\"test_table_drop_column$partitions\"");
        assertThat(resultAfterDrop.getRowCount()).isEqualTo(3);
        Map<LocalDate, MaterializedRow> rowsByPartitionAfterDrop = resultAfterDrop.getMaterializedRows().stream()
                .collect(toImmutableMap(row -> ((LocalDate) ((MaterializedRow) row.getField(0)).getField(0)), Function.identity()));
        assertThat(rowsByPartitionAfterDrop.get(LocalDate.parse("2019-09-08")).getField(4)).isEqualTo(new MaterializedRow(DEFAULT_PRECISION,
                new MaterializedRow(DEFAULT_PRECISION, 0L, 0L, 0L, null)));
    }

    @Test
    public void testFilesTableOnDropColumn()
    {
        assertQuery("SELECT sum(record_count) FROM test_schema.\"test_table_drop_column$files\"", "VALUES 6");
    }

    @Test
    public void testHistoryTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$history\"",
                "VALUES ('made_current_at', 'timestamp(3) with time zone', '', '')," +
                        "('snapshot_id', 'bigint', '', '')," +
                        "('parent_id', 'bigint', '', '')," +
                        "('is_current_ancestor', 'boolean', '', '')");

        // Test the number of history entries
        assertQuery("SELECT count(*) FROM test_schema.\"test_table$history\"", "VALUES 3");
    }

    @Test
    public void testMetadataLogEntriesTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$metadata_log_entries\"",
                "VALUES ('timestamp', 'timestamp(3) with time zone', '', '')," +
                        "('file', 'varchar', '', '')," +
                        "('latest_snapshot_id', 'bigint', '', '')," +
                        "('latest_schema_id', 'integer', '', '')," +
                        "('latest_sequence_number', 'bigint', '', '')");

        List<Integer> latestSchemaIds = new ArrayList<>();
        List<Long> latestSequenceNumbers = new ArrayList<>();

        assertUpdate("CREATE TABLE test_schema.test_metadata_log_entries (c1 BIGINT)");
        latestSchemaIds.add(0);
        latestSequenceNumbers.add(1L);
        assertMetadataLogEntries(latestSchemaIds, latestSequenceNumbers);

        assertUpdate("INSERT INTO test_schema.test_metadata_log_entries VALUES (1)", 1);
        // INSERT create two commits (https://github.com/trinodb/trino/issues/15439) and share a same snapshotId
        latestSchemaIds.add(0);
        latestSchemaIds.add(0);
        latestSequenceNumbers.add(2L);
        latestSequenceNumbers.add(2L);
        assertMetadataLogEntries(latestSchemaIds, latestSequenceNumbers);

        assertUpdate("ALTER TABLE test_schema.test_metadata_log_entries ADD COLUMN c2 VARCHAR");
        latestSchemaIds.add(0);
        latestSequenceNumbers.add(2L);
        assertMetadataLogEntries(latestSchemaIds, latestSequenceNumbers);

        assertUpdate("DELETE FROM test_schema.test_metadata_log_entries WHERE c1 = 1", 1);
        latestSchemaIds.add(1);
        latestSequenceNumbers.add(3L);
        assertMetadataLogEntries(latestSchemaIds, latestSequenceNumbers);

        // OPTIMIZE create two commits: update snapshot and rewrite statistics
        assertUpdate("ALTER TABLE test_schema.test_metadata_log_entries execute optimize");
        latestSchemaIds.add(1);
        latestSchemaIds.add(1);
        latestSequenceNumbers.add(4L);
        latestSequenceNumbers.add(4L);
        assertMetadataLogEntries(latestSchemaIds, latestSequenceNumbers);

        assertUpdate("CREATE OR REPLACE TABLE test_schema.test_metadata_log_entries (c3 INTEGER)");
        latestSchemaIds.add(2);
        latestSequenceNumbers.add(5L);
        assertMetadataLogEntries(latestSchemaIds, latestSequenceNumbers);

        assertUpdate("INSERT INTO test_schema.test_metadata_log_entries VALUES (1)", 1);
        latestSchemaIds.add(2);
        latestSequenceNumbers.add(6L);
        latestSchemaIds.add(2);
        latestSequenceNumbers.add(6L);
        assertMetadataLogEntries(latestSchemaIds, latestSequenceNumbers);

        assertUpdate("DROP TABLE IF EXISTS test_schema.test_metadata_log_entries");
    }

    private void assertMetadataLogEntries(List<Integer> latestSchemaIds, List<Long> latestSequenceNumbers)
    {
        MaterializedResult result = computeActual("SELECT latest_schema_id, latest_sequence_number FROM test_schema.\"test_metadata_log_entries$metadata_log_entries\" ORDER BY timestamp");
        List<MaterializedRow> materializedRows = result.getMaterializedRows();

        assertThat(result.getRowCount()).isEqualTo(latestSchemaIds.size());
        for (int i = 0; i < result.getRowCount(); i++) {
            assertThat(materializedRows.get(i).getField(0)).isEqualTo(latestSchemaIds.get(i));
            assertThat(materializedRows.get(i).getField(1)).isEqualTo(latestSequenceNumbers.get(i));
        }
    }

    @Test
    public void testSnapshotsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$snapshots\"",
                "VALUES ('committed_at', 'timestamp(3) with time zone', '', '')," +
                        "('snapshot_id', 'bigint', '', '')," +
                        "('parent_id', 'bigint', '', '')," +
                        "('operation', 'varchar', '', '')," +
                        "('manifest_list', 'varchar', '', '')," +
                        "('summary', 'map(varchar, varchar)', '', '')");

        assertQuery("SELECT operation FROM test_schema.\"test_table$snapshots\"", "VALUES 'append', 'append', 'append'");
        assertQuery("SELECT summary['total-records'] FROM test_schema.\"test_table$snapshots\"", "VALUES '0', '3', '6'");
    }

    @Test
    public void testManifestsTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$manifests\"",
                "VALUES ('path', 'varchar', '', '')," +
                        "('length', 'bigint', '', '')," +
                        "('partition_spec_id', 'integer', '', '')," +
                        "('added_snapshot_id', 'bigint', '', '')," +
                        "('added_data_files_count', 'integer', '', '')," +
                        "('added_rows_count', 'bigint', '', '')," +
                        "('existing_data_files_count', 'integer', '', '')," +
                        "('existing_rows_count', 'bigint', '', '')," +
                        "('deleted_data_files_count', 'integer', '', '')," +
                        "('deleted_rows_count', 'bigint', '', '')," +
                        "('partitions', 'array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))', '', '')");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$manifests\"");
        assertThat(query("SELECT added_data_files_count, existing_rows_count, added_rows_count, deleted_data_files_count, deleted_rows_count, partitions FROM test_schema.\"test_table$manifests\""))
                .matches(
                        "VALUES " +
                                "    (2, BIGINT '0', BIGINT '3', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2019-09-08', '2019-09-09')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)))) , " +
                                "    (2, BIGINT '0', BIGINT '3', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2019-09-09', '2019-09-10')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))))");

        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table_multilevel_partitions$manifests\"");
        assertThat(query("SELECT added_data_files_count, existing_rows_count, added_rows_count, deleted_data_files_count, deleted_rows_count, partitions FROM test_schema.\"test_table_multilevel_partitions$manifests\""))
                .matches(
                        "VALUES " +
                                "(3, BIGINT '0', BIGINT '3', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '0', '1'), ROW(false, false, '2019-09-08', '2019-09-09')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))))");

        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table_with_dml$manifests\"");
        assertThat(query("SELECT added_data_files_count, existing_rows_count, added_rows_count, deleted_data_files_count, deleted_rows_count, partitions FROM test_schema.\"test_table_with_dml$manifests\""))
                .matches(
                        "VALUES " +
                                // INSERT on '2022-01-01', '2022-02-02', '2022-03-03' partitions
                                "(3, BIGINT '0', BIGINT '6', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2022-01-01', '2022-03-03')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)))), " +
                                // UPDATE on '2022-01-01' partition
                                "(1, BIGINT '0', BIGINT '1', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2022-01-01', '2022-01-01')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)))), " +
                                "(1, BIGINT '0', BIGINT '1', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2022-01-01', '2022-01-01')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)))), " +
                                // DELETE from '2022-02-02' partition
                                "(1, BIGINT '0', BIGINT '1', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2022-02-02', '2022-02-02')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)))), " +
                                // INSERT on '2022-03-03', '2022-04-04' partitions
                                "(2, BIGINT '0', BIGINT '2', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2022-03-03', '2022-04-04')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))))");
    }

    @Test
    public void testFilesTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$files\"",
                "VALUES ('content', 'integer', '', '')," +
                        "('file_path', 'varchar', '', '')," +
                        "('file_format', 'varchar', '', '')," +
                        "('record_count', 'bigint', '', '')," +
                        "('file_size_in_bytes', 'bigint', '', '')," +
                        "('column_sizes', 'map(integer, bigint)', '', '')," +
                        "('value_counts', 'map(integer, bigint)', '', '')," +
                        "('null_value_counts', 'map(integer, bigint)', '', '')," +
                        "('nan_value_counts', 'map(integer, bigint)', '', '')," +
                        "('lower_bounds', 'map(integer, varchar)', '', '')," +
                        "('upper_bounds', 'map(integer, varchar)', '', '')," +
                        "('key_metadata', 'varbinary', '', '')," +
                        "('split_offsets', 'array(bigint)', '', '')," +
                        "('equality_ids', 'array(integer)', '', '')");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$files\"");
    }

    private Long nanCount(long value)
    {
        // Parquet does not have nan count metrics
        return format == PARQUET ? null : value;
    }
}
