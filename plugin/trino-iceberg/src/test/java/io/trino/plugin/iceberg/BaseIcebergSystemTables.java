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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.spi.type.ArrayType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.util.EqualityDeleteUtils.writeEqualityDeleteForTable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public abstract class BaseIcebergSystemTables
        extends AbstractTestQueryFramework
{
    private final IcebergFileFormat format;
    private HiveMetastore metastore;
    private TrinoFileSystemFactory fileSystemFactory;

    protected BaseIcebergSystemTables(IcebergFileFormat format)
    {
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of("iceberg.file-format", format.name()))
                .build();
        metastore = getHiveMetastore(queryRunner);
        fileSystemFactory = getFileSystemFactory(queryRunner);
        return queryRunner;
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
    void testAllManifests()
    {
        try (TestTable table = newTrinoTable("test_all_manifests", "AS SELECT 1 x")) {
            assertThat(query("SHOW COLUMNS FROM \"" + table.getName() + "$all_manifests\""))
                    .skippingTypesCheck()
                    .matches("VALUES " +
                            "('path', 'varchar', '', '')," +
                            "('length', 'bigint', '', '')," +
                            "('partition_spec_id', 'integer', '', '')," +
                            "('added_snapshot_id', 'bigint', '', '')," +
                            "('added_data_files_count', 'integer', '', '')," +
                            "('existing_data_files_count', 'integer', '', '')," +
                            "('deleted_data_files_count', 'integer', '', '')," +
                            "('partition_summaries', 'array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))', '', '')");

            assertThat((String) computeScalar("SELECT path FROM \"" + table.getName() + "$all_manifests\"")).endsWith("-m0.avro");
            assertThat((Long) computeScalar("SELECT length FROM \"" + table.getName() + "$all_manifests\"")).isPositive();
            assertThat((Integer) computeScalar("SELECT partition_spec_id FROM \"" + table.getName() + "$all_manifests\"")).isZero();
            assertThat((Long) computeScalar("SELECT added_snapshot_id FROM \"" + table.getName() + "$all_manifests\"")).isPositive();
            assertThat((Integer) computeScalar("SELECT added_data_files_count FROM \"" + table.getName() + "$all_manifests\"")).isEqualTo(1);
            assertThat((Integer) computeScalar("SELECT existing_data_files_count FROM \"" + table.getName() + "$all_manifests\"")).isZero();
            assertThat((Integer) computeScalar("SELECT deleted_data_files_count FROM \"" + table.getName() + "$all_manifests\"")).isZero();
            assertThat((List<?>) computeScalar("SELECT partition_summaries FROM \"" + table.getName() + "$all_manifests\"")).isEmpty();

            assertUpdate("DELETE FROM " + table.getName(), 1);
            assertThat((Long) computeScalar("SELECT count(1) FROM \"" + table.getName() + "$all_manifests\"")).isEqualTo(2);
        }
    }

    @Test
    void testAllManifestsWithPartitionTable()
    {
        try (TestTable table = newTrinoTable("test_all_manifests", "WITH (partitioning = ARRAY['dt']) AS SELECT 1 x, DATE '2021-01-01' dt")) {
            assertThat(query("SELECT partition_summaries FROM \"" + table.getName() + "$all_manifests\""))
                    .matches("VALUES CAST(ARRAY[ROW(false, false, VARCHAR '2021-01-01', VARCHAR '2021-01-01')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)))");
        }
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
                        "('partition_summaries', 'array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))', '', '')");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$manifests\"");
        assertThat(query("SELECT added_data_files_count, existing_rows_count, added_rows_count, deleted_data_files_count, deleted_rows_count, partition_summaries FROM test_schema.\"test_table$manifests\""))
                .matches(
                        "VALUES " +
                                "    (2, BIGINT '0', BIGINT '3', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2019-09-08', '2019-09-09')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar)))) , " +
                                "    (2, BIGINT '0', BIGINT '3', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '2019-09-09', '2019-09-10')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))))");

        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table_multilevel_partitions$manifests\"");
        assertThat(query("SELECT added_data_files_count, existing_rows_count, added_rows_count, deleted_data_files_count, deleted_rows_count, partition_summaries FROM test_schema.\"test_table_multilevel_partitions$manifests\""))
                .matches(
                        "VALUES " +
                                "(3, BIGINT '0', BIGINT '3', 0, BIGINT '0', CAST(ARRAY[ROW(false, false, '0', '1'), ROW(false, false, '2019-09-08', '2019-09-09')] AS array(row(contains_null boolean, contains_nan boolean, lower_bound varchar, upper_bound varchar))))");

        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table_with_dml$manifests\"");
        assertThat(query("SELECT added_data_files_count, existing_rows_count, added_rows_count, deleted_data_files_count, deleted_rows_count, partition_summaries FROM test_schema.\"test_table_with_dml$manifests\""))
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
        try (TestTable table = newTrinoTable("test_files_table", "AS SELECT 1 x")) {
            MaterializedResult result = computeActual("DESCRIBE " + table.getName());
            assertThat(result.getMaterializedRows().stream().map(row -> (String) row.getField(0)))
                    .doesNotContain("partition");
            assertQuerySucceeds("SELECT * FROM \"" + table.getName() + "$files\"");
        }
    }

    @Test
    public void testFilesPartitionTable()
    {
        assertQuery("SHOW COLUMNS FROM test_schema.\"test_table$files\"",
                "VALUES ('content', 'integer', '', '')," +
                        "('file_path', 'varchar', '', '')," +
                        "('file_format', 'varchar', '', '')," +
                        "('spec_id', 'integer', '', '')," +
                        "('partition', 'row(_date date)', '', '')," +
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
                        "('equality_ids', 'array(integer)', '', '')," +
                        "('sort_order_id', 'integer', '', '')," +
                        "('readable_metrics', 'json', '', '')");
        assertQuerySucceeds("SELECT * FROM test_schema.\"test_table$files\"");

        long offset = format == PARQUET ? 4L : 3L;
        assertThat(computeActual("SELECT split_offsets FROM test_schema.\"test_table$files\""))
                .isEqualTo(resultBuilder(getSession(), ImmutableList.of(new ArrayType(BIGINT)))
                        .row(ImmutableList.of(offset))
                        .row(ImmutableList.of(offset))
                        .row(ImmutableList.of(offset))
                        .row(ImmutableList.of(offset))
                        .build());
    }

    @Test
    void testFilesTableReadableMetrics()
    {
        testFilesTableReadableMetrics(
                "boolean",
                "VALUES  true, false, NULL",
                "{\"x\":{\"column_size\":" + columnSize(33) + ",\"value_count\":3,\"null_value_count\":1,\"nan_value_count\":null,\"lower_bound\":false,\"upper_bound\":true}}");
        testFilesTableReadableMetrics(
                "int",
                "VALUES -1, 1",
                "{\"x\":{\"column_size\":" + columnSize(40) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":-1,\"upper_bound\":1}}");
        testFilesTableReadableMetrics(
                "bigint",
                "VALUES -123, 999",
                "{\"x\":{\"column_size\":" + columnSize(48) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":-123,\"upper_bound\":999}}");
        testFilesTableReadableMetrics(
                "real",
                "VALUES -1.1, 1.1, nan()",
                "{\"x\":{\"column_size\":" + columnSize(44) + ",\"value_count\":3,\"null_value_count\":0,\"nan_value_count\":" + nanCount(1) + ",\"lower_bound\":null,\"upper_bound\":null}}");
        testFilesTableReadableMetrics(
                "double",
                "VALUES -1.1, 1.1, nan()",
                "{\"x\":{\"column_size\":" + columnSize(53) + ",\"value_count\":3,\"null_value_count\":0,\"nan_value_count\":" + nanCount(1) + ",\"lower_bound\":null,\"upper_bound\":null}}");
        testFilesTableReadableMetrics(
                "decimal(3,1)",
                "VALUES -3.14, 3.14",
                "{\"x\":{\"column_size\":" + columnSize(40) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":\"-3.1\",\"upper_bound\":\"3.1\"}}");
        testFilesTableReadableMetrics(
                "date",
                "VALUES DATE '1960-01-01', DATE '9999-12-31'",
                "{\"x\":{\"column_size\":" + columnSize(40) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":\"1960-01-01\",\"upper_bound\":\"9999-12-31\"}}");
        testFilesTableReadableMetrics(
                "time",
                "VALUES TIME '00:00:00.000', TIME '12:34:56.999999'",
                "{\"x\":{\"column_size\":" + columnSize(48) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":\"00:00:00\",\"upper_bound\":\"12:34:56.999999\"}}");
        testFilesTableReadableMetrics(
                "timestamp",
                "VALUES TIMESTAMP '1960-01-01 00:00:00', TIMESTAMP '9999-12-31 12:34:56.999999'",
                "{\"x\":{\"column_size\":" + columnSize(48) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":\"1960-01-01T00:00:00\",\"upper_bound\":\"9999-12-31T12:34:56.999999\"}}");
        testFilesTableReadableMetrics(
                "timestamp with time zone",
                "VALUES TIMESTAMP '1960-01-01 00:00:00 UTC', TIMESTAMP '9999-12-31 12:34:56.999999 UTC'",
                "{\"x\":{\"column_size\":" + columnSize(48) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":\"1960-01-01T00:00:00+00:00\",\"upper_bound\":\"9999-12-31T12:34:56.999999+00:00\"}}");
        testFilesTableReadableMetrics(
                "varchar",
                "VALUES 'alice', 'bob'",
                "{\"x\":{\"column_size\":" + columnSize(48) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":\"alice\",\"upper_bound\":\"bob\"}}");
        testFilesTableReadableMetrics(
                "uuid",
                "VALUES UUID '09e1efb9-9e87-465e-abaf-0c67f4841114', UUID '0f2ef2b3-3c5a-4834-ba91-61be53ff8fbb'",
                "{\"x\":{\"column_size\":" + columnSize(64) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":" + value("\"09e1efb9-9e87-465e-abaf-0c67f4841114\"", null) + ",\"upper_bound\":" + value("\"0f2ef2b3-3c5a-4834-ba91-61be53ff8fbb\"", null) + "}}");
        testFilesTableReadableMetrics(
                "varbinary",
                "VALUES x'12', x'34'",
                "{\"x\":{\"column_size\":" + columnSize(42) + ",\"value_count\":2,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":" + value("\"12\"", null) + ",\"upper_bound\":" + value("\"34\"", null) + "}}");
        testFilesTableReadableMetrics(
                "row(y int)",
                "SELECT (CAST(ROW(123) AS ROW(y int)))",
                "{\"x.y\":{\"column_size\":" + columnSize(37) + ",\"value_count\":1,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":123,\"upper_bound\":123}}");
        testFilesTableReadableMetrics(
                "array(int)",
                "VALUES ARRAY[123]",
                "{\"x.element\":{\"column_size\":" + columnSize(43) + ",\"value_count\":" + value(1, null) + ",\"null_value_count\":" + value(0, null) + ",\"nan_value_count\":null,\"lower_bound\":null,\"upper_bound\":null}}");
        testFilesTableReadableMetrics(
                "map(int, int)",
                "VALUES map(ARRAY[1,3], ARRAY[2,4])",
                "{" +
                        "\"x.key\":{\"column_size\":" + columnSize(47) + ",\"value_count\":" + value(2, null) + ",\"null_value_count\":" + value(0, null) + ",\"nan_value_count\":null,\"lower_bound\":null,\"upper_bound\":null}," +
                        "\"x.value\":{\"column_size\":" + columnSize(47) + ",\"value_count\":" + value(2, null) + ",\"null_value_count\":" + value(0, null) + ",\"nan_value_count\":null,\"lower_bound\":null,\"upper_bound\":null}" +
                        "}");
    }

    private void testFilesTableReadableMetrics(@Language("SQL") String type, @Language("SQL") String values, @Language("JSON") String... readableMetrics)
    {
        try (TestTable table = newTrinoTable("test_files_table", "(x " + type + ")")) {
            getQueryRunner().execute("INSERT INTO " + table.getName() + " " + values);
            assertThat(computeActual("SELECT readable_metrics FROM \"" + table.getName() + "$files\"").getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder(readableMetrics);
        }
    }

    @Test
    public void testFilesSchemaEvolution()
    {
        try (TestTable table = newTrinoTable("test_files_table", "WITH (partitioning = ARRAY['part']) AS SELECT 1 x, 2 part")) {
            assertThat(query("SELECT partition FROM \"" + table.getName() + "$files\""))
                    .matches("SELECT CAST(ROW(2) AS ROW(part int))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN another_part int");
            assertUpdate("ALTER TABLE " + table.getName() + " SET PROPERTIES partitioning = ARRAY['part', 'another_part']");
            assertThat(query("SELECT partition FROM \"" + table.getName() + "$files\""))
                    .matches("SELECT CAST(ROW(2, NULL) AS ROW(part int, another_part int))");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN part TO part_renamed");
            assertThat(query("SELECT partition FROM \"" + table.getName() + "$files\""))
                    .matches("SELECT CAST(ROW(2, NULL) AS ROW(part int, another_part int))");
        }
    }

    @Test
    public void testFilesNestedPartition()
    {
        try (TestTable table = newTrinoTable(
                "test_files_table",
                "WITH (partitioning = ARRAY['\"part.nested\"']) AS SELECT 1 x, CAST(ROW(2) AS ROW(nested int)) part")) {
            assertThat(query("SELECT partition.\"part.nested\" FROM \"" + table.getName() + "$files\""))
                    .matches("VALUES 2");
        }
    }

    @Test
    public void testFilesTableWithDelete()
    {
        assertUpdate("CREATE TABLE test_schema.test_table_with_delete (_bigint BIGINT, _date DATE) WITH (partitioning = ARRAY['_date'])");
        assertUpdate("INSERT INTO test_schema.test_table_with_delete VALUES (0, CAST('2019-09-08' AS DATE)), (1, CAST('2019-09-09' AS DATE)), (2, CAST('2019-09-09' AS DATE))", 3);
        assertUpdate("INSERT INTO test_schema.test_table_with_delete VALUES (3, CAST('2019-09-09' AS DATE)), (4, CAST('2019-09-10' AS DATE)), (5, CAST('2019-09-10' AS DATE))", 3);
        assertUpdate("DELETE FROM test_schema.test_table_with_delete WHERE _bigint = 5", 1);
        assertUpdate("DELETE FROM test_schema.test_table_with_delete WHERE _bigint = 2", 1);

        assertQuery("SELECT count(*) FROM test_schema.test_table_with_delete", "VALUES 4");
        assertQuery("SELECT count(*) FROM test_schema.\"test_table_with_delete$files\" WHERE content = " + FileContent.DATA.id(), "VALUES 4");
        assertQuery("SELECT count(*) FROM test_schema.\"test_table_with_delete$files\" WHERE content = " + FileContent.POSITION_DELETES.id(), "VALUES 2");
        assertQuery("SELECT count(*) FROM test_schema.\"test_table_with_delete$files\" WHERE content = " + FileContent.EQUALITY_DELETES.id(), "VALUES 0");

        assertUpdate("DROP TABLE IF EXISTS test_schema.test_table_with_delete");
    }

    @Test
    void testAllEntriesTable()
    {
        try (TestTable table = newTrinoTable("test_all_entries", "AS SELECT 1 id, DATE '2014-01-01' dt")) {
            assertThat(query("DESCRIBE \"" + table.getName() + "$all_entries\""))
                    .matches("DESCRIBE \"" + table.getName() + "$entries\"");

            assertThat(query("SELECT * FROM \"" + table.getName() + "$all_entries\""))
                    .matches("SELECT * FROM \"" + table.getName() + "$entries\"");

            assertUpdate("DELETE FROM " + table.getName(), 1);

            assertThat(computeActual("SELECT status FROM \"" + table.getName() + "$all_entries\"").getOnlyColumnAsSet())
                    .containsExactly(1, 2);
            assertThat(computeActual("SELECT status FROM \"" + table.getName() + "$entries\"").getOnlyColumnAsSet())
                    .containsExactly(2);
            assertThat(query("SELECT * FROM \"" + table.getName() + "$all_entries\" WHERE status = 2"))
                    .matches("SELECT * FROM \"" + table.getName() + "$entries\"");
        }
    }

    @Test
    void testEntriesTable()
    {
        try (TestTable table = newTrinoTable("test_entries", "AS SELECT 1 id, DATE '2014-01-01' dt")) {
            assertQuery("SHOW COLUMNS FROM \"" + table.getName() + "$entries\"",
                    "VALUES ('status', 'integer', '', '')," +
                            "('snapshot_id', 'bigint', '', '')," +
                            "('sequence_number', 'bigint', '', '')," +
                            "('file_sequence_number', 'bigint', '', '')," +
                            "('data_file', 'row(content integer, file_path varchar, file_format varchar, spec_id integer, record_count bigint, file_size_in_bytes bigint, " +
                            "column_sizes map(integer, bigint), value_counts map(integer, bigint), null_value_counts map(integer, bigint), nan_value_counts map(integer, bigint), " +
                            "lower_bounds map(integer, varchar), upper_bounds map(integer, varchar), key_metadata varbinary, split_offsets array(bigint), " +
                            "equality_ids array(integer), sort_order_id integer)', '', '')," +
                            "('readable_metrics', 'json', '', '')");

            Table icebergTable = loadTable(table.getName());
            Snapshot snapshot = icebergTable.currentSnapshot();
            long snapshotId = snapshot.snapshotId();
            long sequenceNumber = snapshot.sequenceNumber();

            assertThat(computeScalar("SELECT status FROM \"" + table.getName() + "$entries\""))
                    .isEqualTo(1);
            assertThat(computeScalar("SELECT snapshot_id FROM \"" + table.getName() + "$entries\""))
                    .isEqualTo(snapshotId);
            assertThat(computeScalar("SELECT sequence_number FROM \"" + table.getName() + "$entries\""))
                    .isEqualTo(sequenceNumber);
            assertThat(computeScalar("SELECT file_sequence_number FROM \"" + table.getName() + "$entries\""))
                    .isEqualTo(1L);

            MaterializedRow dataFile = (MaterializedRow) computeScalar("SELECT data_file FROM \"" + table.getName() + "$entries\"");
            assertThat(dataFile.getFieldCount()).isEqualTo(16);
            assertThat(dataFile.getField(0)).isEqualTo(0); // content
            assertThat((String) dataFile.getField(1)).endsWith(format.toString().toLowerCase(ENGLISH)); // file_path
            assertThat(dataFile.getField(2)).isEqualTo(format.toString()); // file_format
            assertThat(dataFile.getField(3)).isEqualTo(0); // spec_id
            assertThat(dataFile.getField(4)).isEqualTo(1L); // record_count
            assertThat((long) dataFile.getField(5)).isPositive(); // file_size_in_bytes
            assertThat(dataFile.getField(6)).isEqualTo(value(Map.of(1, 36L, 2, 36L), null)); // column_sizes
            assertThat(dataFile.getField(7)).isEqualTo(Map.of(1, 1L, 2, 1L)); // value_counts
            assertThat(dataFile.getField(8)).isEqualTo(Map.of(1, 0L, 2, 0L)); // null_value_counts
            assertThat(dataFile.getField(9)).isEqualTo(value(Map.of(), null)); // nan_value_counts
            assertThat(dataFile.getField(10)).isEqualTo(Map.of(1, "1", 2, "2014-01-01")); // lower_bounds
            assertThat(dataFile.getField(11)).isEqualTo(Map.of(1, "1", 2, "2014-01-01")); // upper_bounds
            assertThat(dataFile.getField(12)).isNull(); // key_metadata
            assertThat(dataFile.getField(13)).isEqualTo(List.of(value(4L, 3L))); // split_offsets
            assertThat(dataFile.getField(14)).isNull(); // equality_ids
            assertThat(dataFile.getField(15)).isEqualTo(0); // sort_order_id

            assertThat(computeScalar("SELECT readable_metrics FROM \"" + table.getName() + "$entries\""))
                    .isEqualTo("{" +
                            "\"dt\":{\"column_size\":" + value(36, null) + ",\"value_count\":1,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":\"2014-01-01\",\"upper_bound\":\"2014-01-01\"}," +
                            "\"id\":{\"column_size\":" + value(36, null) + ",\"value_count\":1,\"null_value_count\":0,\"nan_value_count\":null,\"lower_bound\":1,\"upper_bound\":1}" +
                            "}");
        }
    }

    @Test
    void testEntriesAfterPositionDelete()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_entries", "AS SELECT 1 id, DATE '2014-01-01' dt")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE id = 1", 1);

            Table icebergTable = loadTable(table.getName());
            Snapshot snapshot = icebergTable.currentSnapshot();
            long snapshotId = snapshot.snapshotId();
            long sequenceNumber = snapshot.sequenceNumber();

            assertThat(computeScalar("SELECT status FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(1);
            assertThat(computeScalar("SELECT snapshot_id FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(snapshotId);
            assertThat(computeScalar("SELECT sequence_number FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(sequenceNumber);
            assertThat(computeScalar("SELECT file_sequence_number FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(2L);

            MaterializedRow deleteFile = (MaterializedRow) computeScalar("SELECT data_file FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId);
            assertThat(deleteFile.getFieldCount()).isEqualTo(16);
            assertThat(deleteFile.getField(0)).isEqualTo(1); // content
            assertThat((String) deleteFile.getField(1)).endsWith(format.toString().toLowerCase(ENGLISH)); // file_path
            assertThat(deleteFile.getField(2)).isEqualTo(format.toString()); // file_format
            assertThat(deleteFile.getField(3)).isEqualTo(0); // spec_id
            assertThat(deleteFile.getField(4)).isEqualTo(1L); // record_count
            assertThat((long) deleteFile.getField(5)).isPositive(); // file_size_in_bytes

            //noinspection unchecked
            Map<Integer, Long> columnSizes = (Map<Integer, Long>) deleteFile.getField(6);
            switch (format) {
                case ORC -> assertThat(columnSizes).isNull();
                case PARQUET -> assertThat(columnSizes)
                        .hasSize(2)
                        .satisfies(_ -> assertThat(columnSizes.get(DELETE_FILE_POS.fieldId())).isPositive())
                        .satisfies(_ -> assertThat(columnSizes.get(DELETE_FILE_PATH.fieldId())).isPositive());
                default -> throw new IllegalArgumentException("Unsupported format: " + format);
            }

            assertThat(deleteFile.getField(7)).isEqualTo(Map.of(DELETE_FILE_POS.fieldId(), 1L, DELETE_FILE_PATH.fieldId(), 1L)); // value_counts
            assertThat(deleteFile.getField(8)).isEqualTo(Map.of(DELETE_FILE_POS.fieldId(), 0L, DELETE_FILE_PATH.fieldId(), 0L)); // null_value_counts
            assertThat(deleteFile.getField(9)).isEqualTo(value(Map.of(), null)); // nan_value_counts

            // lower_bounds
            //noinspection unchecked
            Map<Integer, String> lowerBounds = (Map<Integer, String>) deleteFile.getField(10);
            assertThat(lowerBounds)
                    .hasSize(2)
                    .satisfies(_ -> assertThat(lowerBounds.get(DELETE_FILE_POS.fieldId())).isEqualTo("0"))
                    .satisfies(_ -> assertThat(lowerBounds.get(DELETE_FILE_PATH.fieldId())).contains(table.getName()));

            // upper_bounds
            //noinspection unchecked
            Map<Integer, String> upperBounds = (Map<Integer, String>) deleteFile.getField(11);
            assertThat(upperBounds)
                    .hasSize(2)
                    .satisfies(_ -> assertThat(upperBounds.get(DELETE_FILE_POS.fieldId())).isEqualTo("0"))
                    .satisfies(_ -> assertThat(upperBounds.get(DELETE_FILE_PATH.fieldId())).contains(table.getName()));

            assertThat(deleteFile.getField(12)).isNull(); // key_metadata
            assertThat(deleteFile.getField(13)).isEqualTo(List.of(value(4L, 3L))); // split_offsets
            assertThat(deleteFile.getField(14)).isNull(); // equality_ids
            assertThat(deleteFile.getField(15)).isNull(); // sort_order_id

            assertThat(computeScalar("SELECT readable_metrics FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo("""
                            {\
                            "dt":{"column_size":null,"value_count":null,"null_value_count":null,"nan_value_count":null,"lower_bound":null,"upper_bound":null},\
                            "id":{"column_size":null,"value_count":null,"null_value_count":null,"nan_value_count":null,"lower_bound":null,"upper_bound":null}\
                            }""");
        }
    }

    @Test
    void testEntriesAfterEqualityDelete()
            throws Exception
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_entries", "AS SELECT 1 id, DATE '2014-01-01' dt")) {
            Table icebergTable = loadTable(table.getName());
            assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "0");
            writeEqualityDeleteForTable(icebergTable, fileSystemFactory, Optional.empty(), Optional.empty(), ImmutableMap.of("id", 1), Optional.empty());
            assertThat(icebergTable.currentSnapshot().summary()).containsEntry("total-equality-deletes", "1");

            Snapshot snapshot = icebergTable.currentSnapshot();
            long snapshotId = snapshot.snapshotId();
            long sequenceNumber = snapshot.sequenceNumber();

            assertThat(computeScalar("SELECT status FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(1);
            assertThat(computeScalar("SELECT snapshot_id FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(snapshotId);
            assertThat(computeScalar("SELECT sequence_number FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(sequenceNumber);
            assertThat(computeScalar("SELECT file_sequence_number FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo(2L);

            MaterializedRow dataFile = (MaterializedRow) computeScalar("SELECT data_file FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId);
            assertThat(dataFile.getFieldCount()).isEqualTo(16);
            assertThat(dataFile.getField(0)).isEqualTo(2); // content
            assertThat(dataFile.getField(3)).isEqualTo(0); // spec_id
            assertThat(dataFile.getField(4)).isEqualTo(1L); // record_count
            assertThat((long) dataFile.getField(5)).isPositive(); // file_size_in_bytes
            assertThat(dataFile.getField(6)).isEqualTo(Map.of(1, 45L)); // column_sizes
            assertThat(dataFile.getField(7)).isEqualTo(Map.of(1, 1L)); // value_counts
            assertThat(dataFile.getField(8)).isEqualTo(Map.of(1, 0L)); // null_value_counts
            assertThat(dataFile.getField(9)).isEqualTo(Map.of()); // nan_value_counts
            assertThat(dataFile.getField(10)).isEqualTo(Map.of(1, "1")); // lower_bounds
            assertThat(dataFile.getField(11)).isEqualTo(Map.of(1, "1")); // upper_bounds
            assertThat(dataFile.getField(12)).isNull(); // key_metadata
            assertThat(dataFile.getField(13)).isEqualTo(List.of(4L)); // split_offsets
            assertThat(dataFile.getField(14)).isEqualTo(List.of(1)); // equality_ids
            assertThat(dataFile.getField(15)).isEqualTo(0); // sort_order_id

            assertThat(computeScalar("SELECT readable_metrics FROM \"" + table.getName() + "$entries\"" + " WHERE snapshot_id = " + snapshotId))
                    .isEqualTo("""
                            {\
                            "dt":{"column_size":null,"value_count":null,"null_value_count":null,"nan_value_count":null,"lower_bound":null,"upper_bound":null},\
                            "id":{"column_size":45,"value_count":1,"null_value_count":0,"nan_value_count":null,"lower_bound":1,"upper_bound":1}\
                            }""");
        }
    }

    @Test
    public void testPartitionColumns()
    {
        try (TestTable testTable = newTrinoTable("test_partition_columns", """
                WITH (partitioning = ARRAY[
                    '"r1.f1"',
                    'bucket(b1, 4)'
                ]) AS
                SELECT
                    CAST(ROW(1, 2) AS ROW(f1 INTEGER, f2 integeR)) as r1
                    , CAST('b' AS VARCHAR) as b1""")) {
            assertThat(query("SELECT partition FROM \"" + testTable.getName() + "$partitions\""))
                    .matches("SELECT CAST(ROW(1, 3) AS ROW(\"r1.f1\" INTEGER, b1_bucket INTEGER))");
        }

        try (TestTable testTable = newTrinoTable("test_partition_columns", """
                WITH (partitioning = ARRAY[
                    '"r1.f2"',
                    'bucket(b1, 4)',
                    '"r1.f1"'
                ]) AS
                SELECT
                    CAST(ROW('f1', 'f2') AS ROW(f1 VARCHAR, f2 VARCHAR)) as r1
                    , CAST('b' AS VARCHAR) as b1""")) {
            assertThat(query("SELECT partition FROM \"" + testTable.getName() + "$partitions\""))
                    .matches("SELECT CAST(ROW('f2', 3, 'f1') AS ROW(\"r1.f2\" VARCHAR, b1_bucket INTEGER, \"r1.f1\" VARCHAR))");
        }
    }

    @Test
    void testEntriesPartitionTable()
    {
        try (TestTable table = newTrinoTable(
                "test_entries_partition",
                "WITH (partitioning = ARRAY['dt']) AS SELECT 1 id, DATE '2014-01-01' dt")) {
            assertQuery("SHOW COLUMNS FROM \"" + table.getName() + "$entries\"",
                    "VALUES ('status', 'integer', '', '')," +
                            "('snapshot_id', 'bigint', '', '')," +
                            "('sequence_number', 'bigint', '', '')," +
                            "('file_sequence_number', 'bigint', '', '')," +
                            "('data_file', 'row(content integer, file_path varchar, file_format varchar, spec_id integer, partition row(dt date), record_count bigint, file_size_in_bytes bigint, " +
                            "column_sizes map(integer, bigint), value_counts map(integer, bigint), null_value_counts map(integer, bigint), nan_value_counts map(integer, bigint), " +
                            "lower_bounds map(integer, varchar), upper_bounds map(integer, varchar), key_metadata varbinary, split_offsets array(bigint), " +
                            "equality_ids array(integer), sort_order_id integer)', '', '')," +
                            "('readable_metrics', 'json', '', '')");

            assertThat(query("SELECT data_file.partition FROM \"" + table.getName() + "$entries\""))
                    .matches("SELECT CAST(ROW(DATE '2014-01-01') AS ROW(dt date))");
        }
    }

    private Long nanCount(long value)
    {
        // Parquet does not have nan count metrics
        return format == PARQUET ? null : value;
    }

    private Long columnSize(long value)
    {
        // ORC does not have column size in readable metrics
        return format == ORC ? null : value;
    }

    private Object value(Object parquet, Object orc)
    {
        return format == PARQUET ? parquet : orc;
    }

    private BaseTable loadTable(String tableName)
    {
        return IcebergTestUtils.loadTable(tableName, metastore, fileSystemFactory, "hive", "tpch");
    }
}
