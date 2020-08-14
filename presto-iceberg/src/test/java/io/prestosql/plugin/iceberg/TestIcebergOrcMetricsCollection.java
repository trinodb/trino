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
package io.prestosql.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsConfiguration;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveHdfsConfiguration;
import io.prestosql.plugin.hive.authentication.NoHdfsAuthentication;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static io.prestosql.SystemSessionProperties.MAX_DRIVERS_PER_TASK;
import static io.prestosql.SystemSessionProperties.TASK_CONCURRENCY;
import static io.prestosql.SystemSessionProperties.TASK_WRITER_COUNT;
import static io.prestosql.plugin.iceberg.TestIcebergOrcMetricsCollection.DataFileRecord.toDataFileRecord;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestIcebergOrcMetricsCollection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .setSystemProperty(TASK_WRITER_COUNT, "1")
                .setSystemProperty(MAX_DRIVERS_PER_TASK, "1")
                .setCatalogSessionProperty("iceberg", "orc_string_statistics_limit", Integer.MAX_VALUE + "B")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();

        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        HiveMetastore metastore = new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");

        queryRunner.installPlugin(new TestingIcebergPlugin(metastore));
        queryRunner.createCatalog("iceberg", "iceberg");

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @Test
    public void testBasic()
    {
        assertUpdate("CREATE TABLE orders WITH (format = 'ORC') AS SELECT * FROM tpch.tiny.orders", 15000);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"orders$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0), 17);

        // Check file format
        assertEquals(datafile.getFileFormat(), "ORC");

        // Check file row count
        assertEquals(datafile.getRecordCount(), 15000L);

        List<ColumnStats> stats = datafile.getColumnStats();
        // Check per-column value count
        stats.stream()
                .map(columnStats -> columnStats.valueCount)
                .forEach(valueCount -> assertEquals(valueCount, (Long) 15000L));

        // Check per-column null value count
        stats.stream()
                .map(columnStats -> columnStats.nullCount)
                .forEach(nullValueCount -> assertEquals(nullValueCount, (Long) 0L));

        // Check per-column lower bound
        assertQuery("SELECT min(orderkey) FROM tpch.tiny.orders", "VALUES " + stats.get(0).lowerBound);
        assertQuery("SELECT min(custkey) FROM tpch.tiny.orders", "VALUES " + stats.get(1).lowerBound);
        assertQuery("SELECT min(orderstatus) FROM tpch.tiny.orders", "VALUES '" + stats.get(2).lowerBound + "'");
        assertQuery("SELECT min(totalprice) FROM tpch.tiny.orders", "VALUES " + stats.get(3).lowerBound);
        assertQuery("SELECT min(orderdate) FROM tpch.tiny.orders", "VALUES DATE '" + stats.get(4).lowerBound + "'");
        assertQuery("SELECT min(orderpriority) FROM tpch.tiny.orders", "VALUES '" + stats.get(5).lowerBound + "'");
        assertQuery("SELECT min(clerk) FROM tpch.tiny.orders", "VALUES '" + stats.get(6).lowerBound + "'");
        assertQuery("SELECT min(shippriority) FROM tpch.tiny.orders", "VALUES " + stats.get(7).lowerBound);
        assertQuery("SELECT min(comment) FROM tpch.tiny.orders", "VALUES '" + stats.get(8).lowerBound + "'");

        // Check per-column upper bound
        assertQuery("SELECT max(orderkey) FROM tpch.tiny.orders", "VALUES " + stats.get(0).upperBound);
        assertQuery("SELECT max(custkey) FROM tpch.tiny.orders", "VALUES " + stats.get(1).upperBound);
        assertQuery("SELECT max(orderstatus) FROM tpch.tiny.orders", "VALUES '" + stats.get(2).upperBound + "'");
        assertQuery("SELECT max(totalprice) FROM tpch.tiny.orders", "VALUES " + stats.get(3).upperBound);
        assertQuery("SELECT max(orderdate) FROM tpch.tiny.orders", "VALUES DATE '" + stats.get(4).upperBound + "'");
        assertQuery("SELECT max(orderpriority) FROM tpch.tiny.orders", "VALUES '" + stats.get(5).upperBound + "'");
        assertQuery("SELECT max(clerk) FROM tpch.tiny.orders", "VALUES '" + stats.get(6).upperBound + "'");
        assertQuery("SELECT max(shippriority) FROM tpch.tiny.orders", "VALUES " + stats.get(7).upperBound);
        assertQuery("SELECT max(comment) FROM tpch.tiny.orders", "VALUES '" + stats.get(8).upperBound + "'");

        assertUpdate("DROP TABLE orders");
    }

    @Test
    public void testWithNulls()
    {
        assertUpdate("CREATE TABLE test_with_nulls (_integer INTEGER, _real REAL, _string VARCHAR)");
        assertUpdate("INSERT INTO test_with_nulls VALUES (7, 3.4, 'aaa'), (3, 4.5, 'bbb'), (4, null, 'ccc'), (null, null, 'ddd')", 4);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_with_nulls$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0), 11);

        List<ColumnStats> stats = datafile.getColumnStats();
        // Check per-column value count
        stats.stream()
                .map(columnStats -> columnStats.valueCount)
                .forEach(valueCount -> assertEquals(valueCount, (Long) 4L));

        // Check per-column null value count
        assertEquals(stats.get(0).nullCount, (Long) 1L);
        assertEquals(stats.get(1).nullCount, (Long) 2L);
        assertEquals(stats.get(2).nullCount, (Long) 0L);

        // Check per-column lower bound
        assertEquals(stats.get(0).lowerBound, 3);
        assertEquals(stats.get(1).lowerBound, 3.4f);
        assertEquals(stats.get(2).lowerBound, "aaa");

        assertUpdate("DROP TABLE test_with_nulls");

        assertUpdate("CREATE TABLE test_all_nulls (_integer INTEGER)");
        assertUpdate("INSERT INTO test_all_nulls VALUES null, null, null", 3);
        materializedResult = computeActual("SELECT * FROM \"test_all_nulls$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0), 9);

        stats = datafile.getColumnStats();
        // Check per-column value count
        assertEquals(stats.get(0).valueCount, (Long) 3L);

        // Check per-column null value count
        assertEquals(stats.get(0).nullCount, (Long) 3L);

        // Check that lower bounds and upper bounds are nulls. (There's no non-null record)
        assertNull(stats.get(0).lowerBound);
        assertNull(stats.get(0).upperBound);

        assertUpdate("DROP TABLE test_all_nulls");
    }

    @Test
    public void testNestedTypes()
    {
        assertUpdate("CREATE TABLE test_nested_types (col1 INTEGER, col2 ROW (f1 INTEGER, f2 ARRAY(INTEGER), f3 DOUBLE))");
        assertUpdate("INSERT INTO test_nested_types VALUES " +
                "(7, ROW(3, ARRAY[10, 11, 19], 1.9)), " +
                "(-9, ROW(4, ARRAY[13, 16, 20], -2.9)), " +
                "(8, ROW(0, ARRAY[14, 17, 21], 3.9)), " +
                "(3, ROW(10, ARRAY[15, 18, 22], 4.9))", 4);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_nested_types$files\"");
        assertEquals(materializedResult.getRowCount(), 1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0), 14);

        List<ColumnStats> columnStats = datafile.getColumnStats();

        // Only
        // 1. top-level primitive columns
        // 2. and nested primitive fields that are not descendants of LISTs or MAPs
        // should appear in lowerBounds or UpperBounds

        // col1
        assertEquals(columnStats.get(0).lowerBound, -9);
        assertEquals(columnStats.get(0).upperBound, 8);

        // col2.f1
        assertEquals(columnStats.get(2).lowerBound, 0);
        assertEquals(columnStats.get(2).upperBound, 10);

        // col2.f3
        assertEquals(columnStats.get(5).lowerBound, -2.9);
        assertEquals(columnStats.get(5).upperBound, 4.9);

        assertUpdate("DROP TABLE test_nested_types");
    }

    public static class DataFileRecord
    {
        private final String filePath;
        private final String fileFormat;
        private final long recordCount;
        private final long fileSizeInBytes;
        private final List<ColumnStats> columnStats;

        public static DataFileRecord toDataFileRecord(MaterializedRow row, int expectedFieldCount)
        {
            assertEquals(row.getFieldCount(), expectedFieldCount);
            ImmutableList.Builder<ColumnStats> builder = new ImmutableList.Builder<ColumnStats>();
            for (int i = 8; i < row.getFieldCount(); i++) {
                MaterializedRow statsRow = (MaterializedRow) row.getField(i);

                // expect each row to have size, rowcount, nullvalueCount, lowerBound, upperBound
                assertEquals(statsRow.getFieldCount(), 5);
                builder.add(new ColumnStats((Long) statsRow.getField(0), (Long) statsRow.getField(1), (Long) statsRow.getField(2), statsRow.getField(3), statsRow.getField(4)));
            }

            return new DataFileRecord(
                    (String) row.getField(0),
                    (String) row.getField(1),
                    (long) row.getField(2),
                    (long) row.getField(3),
                    builder.build());
        }

        private DataFileRecord(
                String filePath,
                String fileFormat,
                long recordCount,
                long fileSizeInBytes,
                List<ColumnStats> columnStats)
        {
            this.filePath = filePath;
            this.fileFormat = fileFormat;
            this.recordCount = recordCount;
            this.fileSizeInBytes = fileSizeInBytes;
            this.columnStats = columnStats;
        }

        public String getFileFormat()
        {
            return fileFormat;
        }

        public long getRecordCount()
        {
            return recordCount;
        }

        public List<ColumnStats> getColumnStats()
        {
            return columnStats;
        }
    }

    private static class ColumnStats
    {
        final Long size;
        final Long valueCount;
        final Long nullCount;
        final Object lowerBound;
        final Object upperBound;

        private ColumnStats(Long size, Long valueCount, Long nullCount, Object lowerBound, Object upperBound)
        {
            this.size = size;
            this.valueCount = valueCount;
            this.nullCount = nullCount;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }
    }
}
