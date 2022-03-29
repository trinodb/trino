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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointSchemaManager;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.deltalake.util.DockerizedMinioDataLake;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorContext;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeDockerizedMinioDataLake.createDockerizedMinioDataLakeForDeltaLake;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public abstract class AbstractTestDeltaLakeCreateTableStatistics
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "default";
    private String bucketName;

    abstract Map<String, String> additionalProperties();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "delta-test-create-table-statistics-" + randomTableSuffix();
        DockerizedMinioDataLake dockerizedMinioDataLake = closeAfterClass(createDockerizedMinioDataLakeForDeltaLake(bucketName));
        ImmutableMap.Builder<String, String> queryRunnerProperties = ImmutableMap.builder();
        queryRunnerProperties.putAll(additionalProperties());
        QueryRunner queryRunner = DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                queryRunnerProperties.buildOrThrow(),
                dockerizedMinioDataLake.getMinioAddress(),
                dockerizedMinioDataLake.getTestingHadoop());

        return queryRunner;
    }

    @Test
    public void testComplexDataTypes()
            throws Exception
    {
        try (TestTable table = new TestTable(
                "test_complex_data_types_",
                ImmutableList.of("a", "b", "c", "d"),
                "VALUES (CAST(ROW(1, 2) AS ROW(x BIGINT, y BIGINT)), ARRAY[1, 2, 3], MAP(ARRAY[1, 2], ARRAY['a', 'b']), 'foo'), " +
                        "(CAST(ROW(3, 4) AS ROW(x BIGINT, y BIGINT)), ARRAY[4, 5], MAP(ARRAY[3], ARRAY['c']), 'moo')")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle("d", createUnboundedVarcharType(), REGULAR);
            assertEquals(fileStatistics.getNumRecords(), Optional.of(2L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("foo")));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("moo")));
            assertEquals(fileStatistics.getNullCount("d"), Optional.of(0L));

            for (String complexColumn : ImmutableList.of("a", "b", "c")) {
                columnHandle = new DeltaLakeColumnHandle(complexColumn, createUnboundedVarcharType(), REGULAR);
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEmpty();
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEmpty();
                assertThat(fileStatistics.getNullCount(complexColumn)).isEmpty();
            }
        }
    }

    @DataProvider
    public static Object[][] doubleTypes()
    {
        return new Object[][] {{"DOUBLE"}, {"REAL"}};
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesNaN(String type)
            throws Exception
    {
        String columnName = "t_double";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, REGULAR);
        try (TestTable table = new TestTable("test_nan_", ImmutableList.of(columnName), format("VALUES CAST(nan() AS %1$s), CAST(0.0 AS %1$s)", type))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(2L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnName), Optional.empty());
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesInf(String type)
            throws Exception
    {
        String columnName = "t_double";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, REGULAR);
        try (TestTable table = new TestTable(
                "test_inf_",
                ImmutableList.of(columnName),
                format("VALUES CAST(infinity() AS %1$s), CAST(0.0 AS %1$s), CAST((infinity() * -1) AS %1$s)", type))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(NEGATIVE_INFINITY));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(POSITIVE_INFINITY));
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(0L));
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesInfAndNaN(String type)
            throws Exception
    {
        String columnName = "t_double";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, REGULAR);
        try (TestTable table = new TestTable(
                "test_inf_nan_",
                ImmutableList.of(columnName),
                format("VALUES CAST(nan() AS %1$s), CAST(0.0 AS %1$s), CAST(infinity() AS %1$s), CAST((infinity() * -1) AS %1$s)", type))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnName), Optional.empty());
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesNaNPositive(String type)
            throws Exception
    {
        String columnName = "t_double";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, REGULAR);
        try (TestTable table = new TestTable(
                "test_nan_positive_",
                ImmutableList.of(columnName),
                format("VALUES CAST(nan() AS %1$s), CAST(1.0 AS %1$s), CAST(100.0 AS %1$s), CAST(0.0001 AS %1$s)", type))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnName), Optional.empty());
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesNaNNegative(String type)
            throws Exception
    {
        String columnName = "t_double";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, REGULAR);
        try (TestTable table = new TestTable(
                "test_nan_positive_",
                ImmutableList.of(columnName),
                format("VALUES CAST(nan() AS %1$s), CAST(-1.0 AS %1$s), CAST(-100.0 AS %1$s), CAST(-0.0001 AS %1$s)", type))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnName), Optional.empty());
        }
    }

    @Test
    public void testDecimalLowPrecisionRecords()
            throws Exception
    {
        testDecimal(5, 1);
    }

    @Test
    public void testDecimalMediumPrecisionRecords()
            throws Exception
    {
        testDecimal(10, 2);
    }

    @Test
    public void testDecimalHighPrecisionRecords()
            throws Exception
    {
        testDecimal(25, 3);
    }

    private void testDecimal(int precision, int scale)
            throws Exception
    {
        String low = "1" + "0".repeat(precision - scale) + "." + "0".repeat(scale - 1) + "1";
        String high = "2" + "0".repeat(precision - scale) + "." + "0".repeat(scale - 1) + "2";
        String negative = "-1" + "0".repeat(precision - scale) + "." + "0".repeat(scale - 1) + "1";

        String columnName = "t_decimal";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DecimalType.createDecimalType(precision, scale), REGULAR);
        try (TestTable table = new TestTable(
                "test_decimal_records_",
                ImmutableList.of(columnName),
                format("VALUES DECIMAL '%s', DECIMAL '%s', DECIMAL '%s'", negative, high, low))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            Optional<Object> expectedMin;
            Optional<Object> expectedMax;
            if (precision <= MAX_SHORT_PRECISION) {
                expectedMin = Optional.of(new BigDecimal(negative).unscaledValue().longValueExact());
                expectedMax = Optional.of(new BigDecimal(high).unscaledValue().longValueExact());
            }
            else {
                expectedMin = Optional.of(encodeScaledValue(new BigDecimal(negative), scale));
                expectedMax = Optional.of(encodeScaledValue(new BigDecimal(high), scale));
            }
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), expectedMin);
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), expectedMax);
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(0L));
        }
    }

    @Test
    public void testNullRecords()
            throws Exception
    {
        String columnName = "t_double";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, REGULAR);
        try (TestTable table = new TestTable("test_null_records_", ImmutableList.of(columnName), "VALUES null, 0, null, 1")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(0.0));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(1.0));
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(2L));
        }
    }

    @Test
    public void testOnlyNullRecords()
            throws Exception
    {
        String columnName = "t_varchar";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, createUnboundedVarcharType(), REGULAR);
        try (TestTable table = new TestTable(
                "test_only_null_records_",
                ImmutableList.of(columnName),
                "VALUES CAST(null AS VARCHAR), CAST(null AS VARCHAR), CAST(null AS VARCHAR), CAST(null AS VARCHAR)")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(4L));
        }
    }

    @Test
    public void testDateRecords()
            throws Exception
    {
        String columnName = "t_date";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DateType.DATE, REGULAR);
        try (TestTable table = new TestTable(
                "test_date_records_",
                ImmutableList.of(columnName),
                "VALUES DATE '2011-08-08', DATE '2012-08-08', DATE '2013-08-08', DATE '2013-08-09'")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(LocalDate.parse("2011-08-08").toEpochDay()));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(LocalDate.parse("2013-08-09").toEpochDay()));
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(0L));
        }
    }

    @Test
    public void testTimestampMilliRecords()
            throws Exception
    {
        String columnName = "t_timestamp";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, TIMESTAMP_TZ_MILLIS, REGULAR);
        try (TestTable table = new TestTable(
                "test_timestamp_records_",
                ImmutableList.of(columnName),
                "VALUES timestamp '2012-10-31 01:00:00.123 America/New_York', timestamp '2012-10-31 01:00:00.123 America/Los_Angeles', timestamp '2012-10-31 01:00:00.123 UTC'")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            assertEquals(
                    fileStatistics.getMinColumnValue(columnHandle),
                    Optional.of(packDateTimeWithZone(ZonedDateTime.parse("2012-10-31T01:00:00.123Z").toInstant().toEpochMilli(), UTC_KEY)));
            assertEquals(
                    fileStatistics.getMaxColumnValue(columnHandle),
                    Optional.of(packDateTimeWithZone(ZonedDateTime.parse("2012-10-31T08:00:00.123Z").toInstant().toEpochMilli(), UTC_KEY)));
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(0L));
        }
    }

    @Test
    public void testUnicodeValues()
            throws Exception
    {
        String columnName = "t_string";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, createUnboundedVarcharType(), REGULAR);
        try (TestTable table = new TestTable("test_unicode_", ImmutableList.of(columnName), "VALUES 'ab\uFAD8', 'ab\uD83D\uDD74'")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(2L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("ab\uFAD8")));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("ab\uD83D\uDD74")));
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(0L));
        }
    }

    @Test
    public void testPartitionedTable()
            throws Exception
    {
        String columnName = "t_string";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, createUnboundedVarcharType(), REGULAR);
        String partitionColumn = "t_int";

        try (TestTable table = new TestTable(
                "test_partitioned_table_",
                ImmutableList.of(columnName, partitionColumn),
                ImmutableList.of(partitionColumn),
                "VALUES ('a', 1), ('b', 1), ('c', 1), ('c', 2), ('d', 2), ('e', 2), (null, 1)")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            assertEquals(addFileEntries.size(), 2);

            for (AddFileEntry addFileEntry : addFileEntries) {
                assertThat(addFileEntry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = addFileEntry.getStats().get();
                if (addFileEntry.getPartitionValues().get(partitionColumn).equals("1")) {
                    assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("a")));
                    assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("c")));
                    assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
                    assertEquals(fileStatistics.getNullCount(columnName), Optional.of(1L));
                }
                else if (addFileEntry.getPartitionValues().get(partitionColumn).equals("2")) {
                    assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("c")));
                    assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("e")));
                    assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
                    assertEquals(fileStatistics.getNullCount(columnName), Optional.of(0L));
                }
            }
        }
    }

    @Test
    public void testMultiFileTable()
            throws Exception
    {
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle("name", createUnboundedVarcharType(), REGULAR);
        try (TestTable table = new TestTable(
                "test_partitioned_table_",
                ImmutableList.of(),
                "SELECT name FROM tpch.tiny.nation UNION select name from tpch.tiny.customer")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            assertThat(addFileEntries.size()).isGreaterThan(1);

            List<DeltaLakeFileStatistics> statistics = addFileEntries.stream().map(entry -> entry.getStats().get()).collect(toImmutableList());

            List<Object> minValues = statistics.stream().map(stat -> stat.getMinColumnValue(columnHandle).get()).collect(toImmutableList());
            List<Object> maxValues = statistics.stream().map(stat -> stat.getMaxColumnValue(columnHandle).get()).collect(toImmutableList());

            // All values in the table are distinct, so the min and max values should all be different
            assertEquals(minValues.size(), minValues.stream().distinct().count());
            assertEquals(maxValues.size(), maxValues.stream().distinct().count());
        }
    }

    @Test
    public void testMultiFileTableWithNaNValue()
            throws Exception
    {
        // assertEventually because sometimes write from tpch.tiny.orders creates one file only and the test requires at least two files
        assertEventually(() -> {
            String columnName = "orderkey";
            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, REGULAR);
            try (TestTable table = new TestTable(
                    "test_partitioned_table_",
                    ImmutableList.of(columnName),
                    "SELECT IF(orderkey = 50597, nan(), CAST(orderkey AS double)) FROM tpch.tiny.orders")) {
                List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
                assertThat(addFileEntries.size()).isGreaterThan(1);

                List<DeltaLakeFileStatistics> statistics = addFileEntries.stream().map(entry -> entry.getStats().get()).collect(toImmutableList());

                assertEquals(statistics.stream().filter(stat -> stat.getMinColumnValue(columnHandle).isEmpty() && stat.getMaxColumnValue(columnHandle).isEmpty()).count(), 1);
                assertEquals(
                        statistics.stream().filter(stat -> stat.getMinColumnValue(columnHandle).isPresent() && stat.getMaxColumnValue(columnHandle).isPresent()).count(),
                        statistics.size() - 1);
            }
        });
    }

    protected class TestTable
            implements AutoCloseable
    {
        private final String name;

        public TestTable(String name, List<String> columnNames, String values)
        {
            this(name, columnNames, ImmutableList.of(), values);
        }

        public TestTable(String name, List<String> columnNames, List<String> partitionNames, String values)
        {
            this.name = name + randomTableSuffix();
            String columns = columnNames.isEmpty() ? "" :
                    "(" + String.join(",", columnNames) + ")";
            String partitionedBy = partitionNames.isEmpty() ? "" :
                    format(", partitioned_by = ARRAY[%s]", partitionNames.stream().map(partitionName -> "'" + partitionName + "'").collect(Collectors.joining(",")));
            computeActual(format("CREATE TABLE %s %s WITH (location = 's3://%s/%1$s' %s) AS %s",
                    this.name, columns, bucketName, partitionedBy, values));
        }

        public String getName()
        {
            return name;
        }

        @Override
        public void close()
        {
            computeActual("DROP TABLE " + name);
        }
    }

    protected List<AddFileEntry> getAddFileEntries(String tableName)
            throws IOException
    {
        TestingConnectorContext context = new TestingConnectorContext();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());

        TransactionLogAccess transactionLogAccess = new TransactionLogAccess(
                context.getTypeManager(),
                new CheckpointSchemaManager(context.getTypeManager()),
                new DeltaLakeConfig(),
                new FileFormatDataSourceStats(),
                hdfsEnvironment,
                new ParquetReaderConfig());

        return transactionLogAccess.getActiveFiles(
                transactionLogAccess.loadSnapshot(new SchemaTableName(SCHEMA, tableName), new Path(format("s3://%s/%s", bucketName, tableName)), SESSION), SESSION);
    }
}
