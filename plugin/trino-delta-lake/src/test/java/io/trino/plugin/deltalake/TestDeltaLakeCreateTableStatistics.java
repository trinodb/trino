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
import io.trino.Session;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.containers.Hive3MinioDataLake;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getConnectorService;
import static io.trino.plugin.deltalake.TestingDeltaLakeUtils.getTableActiveFiles;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

@Isolated
public class TestDeltaLakeCreateTableStatistics
        extends AbstractTestQueryFramework
{
    private String bucketName;
    private TransactionLogAccess transactionLogAccess;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "delta-test-create-table-statistics-" + randomNameSuffix();
        Hive3MinioDataLake hiveMinioDataLake = closeAfterClass(new Hive3MinioDataLake(bucketName));
        hiveMinioDataLake.start();

        return DeltaLakeQueryRunner.builder()
                .addMetastoreProperties(hiveMinioDataLake.getHiveHadoop())
                .addS3Properties(hiveMinioDataLake.getMinio(), bucketName)
                .addDeltaProperty("delta.enable-non-concurrent-writes", "true")
                .build();
    }

    @BeforeAll
    public void initTransactionLogAccess()
    {
        transactionLogAccess = getConnectorService(getQueryRunner(), TransactionLogAccess.class);
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

            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle("d", createUnboundedVarcharType(), OptionalInt.empty(), "d", createUnboundedVarcharType(), REGULAR, Optional.empty());
            assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(2L));
            assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("foo")));
            assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("moo")));
            assertThat(fileStatistics.getNullCount("d")).isEqualTo(Optional.of(0L));

            for (String complexColumn : ImmutableList.of("a", "b", "c")) {
                columnHandle = new DeltaLakeColumnHandle(complexColumn, createUnboundedVarcharType(), OptionalInt.empty(), complexColumn, createUnboundedVarcharType(), REGULAR, Optional.empty());
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEmpty();
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEmpty();
                assertThat(fileStatistics.getNullCount(complexColumn)).isEmpty();
            }
        }
    }

    @Test
    public void testDoubleTypesNaN()
            throws Exception
    {
        for (String type : Arrays.asList("DOUBLE", "REAL")) {
            String columnName = "t_double";
            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
            try (TestTable table = new TestTable("test_nan_", ImmutableList.of(columnName), format("VALUES CAST(nan() AS %1$s), CAST(0.0 AS %1$s)", type))) {
                List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
                AddFileEntry entry = getOnlyElement(addFileEntries);
                assertThat(entry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

                assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(2L));
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.empty());
            }
        }
    }

    @Test
    public void testDoubleTypesInf()
            throws Exception
    {
        for (String type : Arrays.asList("DOUBLE", "REAL")) {
            String columnName = "t_double";
            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
            try (TestTable table = new TestTable(
                    "test_inf_",
                    ImmutableList.of(columnName),
                    format("VALUES CAST(infinity() AS %1$s), CAST(0.0 AS %1$s), CAST((infinity() * -1) AS %1$s)", type))) {
                List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
                AddFileEntry entry = getOnlyElement(addFileEntries);
                assertThat(entry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

                assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(3L));
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(NEGATIVE_INFINITY));
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(POSITIVE_INFINITY));
                assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(0L));
            }
        }
    }

    @Test
    public void testDoubleTypesInfAndNaN()
            throws Exception
    {
        for (String type : Arrays.asList("DOUBLE", "REAL")) {
            String columnName = "t_double";
            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
            try (TestTable table = new TestTable(
                    "test_inf_nan_",
                    ImmutableList.of(columnName),
                    format("VALUES CAST(nan() AS %1$s), CAST(0.0 AS %1$s), CAST(infinity() AS %1$s), CAST((infinity() * -1) AS %1$s)", type))) {
                List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
                AddFileEntry entry = getOnlyElement(addFileEntries);
                assertThat(entry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

                assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(4L));
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.empty());
            }
        }
    }

    @Test
    public void testDoubleTypesNaNPositive()
            throws Exception
    {
        for (String type : Arrays.asList("DOUBLE", "REAL")) {
            String columnName = "t_double";
            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
            try (TestTable table = new TestTable(
                    "test_nan_positive_",
                    ImmutableList.of(columnName),
                    format("VALUES CAST(nan() AS %1$s), CAST(1.0 AS %1$s), CAST(100.0 AS %1$s), CAST(0.0001 AS %1$s)", type))) {
                List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
                AddFileEntry entry = getOnlyElement(addFileEntries);
                assertThat(entry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

                assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(4L));
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.empty());
            }
        }
    }

    @Test
    public void testDoubleTypesNaNNegative()
            throws Exception
    {
        for (String type : Arrays.asList("DOUBLE", "REAL")) {
            String columnName = "t_double";
            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
            try (TestTable table = new TestTable(
                    "test_nan_positive_",
                    ImmutableList.of(columnName),
                    format("VALUES CAST(nan() AS %1$s), CAST(-1.0 AS %1$s), CAST(-100.0 AS %1$s), CAST(-0.0001 AS %1$s)", type))) {
                List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
                AddFileEntry entry = getOnlyElement(addFileEntries);
                assertThat(entry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

                assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(4L));
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.empty());
                assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.empty());
            }
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
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DecimalType.createDecimalType(precision, scale), OptionalInt.empty(), columnName, DecimalType.createDecimalType(precision, scale), REGULAR, Optional.empty());
        try (TestTable table = new TestTable(
                "test_decimal_records_",
                ImmutableList.of(columnName),
                format("VALUES DECIMAL '%s', DECIMAL '%s', DECIMAL '%s'", negative, high, low))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(3L));
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
            assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(expectedMin);
            assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(expectedMax);
            assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(0L));
        }
    }

    @Test
    public void testNullRecords()
            throws Exception
    {
        String columnName = "t_double";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
        try (TestTable table = new TestTable("test_null_records_", ImmutableList.of(columnName), "VALUES null, 0, null, 1")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(4L));
            assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(0.0));
            assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(1.0));
            assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(2L));
        }
    }

    @Test
    public void testOnlyNullRecords()
            throws Exception
    {
        String columnName = "t_varchar";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, createUnboundedVarcharType(), OptionalInt.empty(), columnName, createUnboundedVarcharType(), REGULAR, Optional.empty());
        try (TestTable table = new TestTable(
                "test_only_null_records_",
                ImmutableList.of(columnName),
                "VALUES CAST(null AS VARCHAR), CAST(null AS VARCHAR), CAST(null AS VARCHAR), CAST(null AS VARCHAR)")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(4L));
            assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.empty());
            assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.empty());
            assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(4L));
        }
    }

    @Test
    public void testDateRecords()
            throws Exception
    {
        String columnName = "t_date";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DateType.DATE, OptionalInt.empty(), columnName, DateType.DATE, REGULAR, Optional.empty());
        try (TestTable table = new TestTable(
                "test_date_records_",
                ImmutableList.of(columnName),
                "VALUES DATE '2011-08-08', DATE '2012-08-08', DATE '2013-08-08', DATE '2013-08-09'")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(4L));
            assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(LocalDate.parse("2011-08-08").toEpochDay()));
            assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(LocalDate.parse("2013-08-09").toEpochDay()));
            assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(0L));
        }
    }

    @Test
    public void testTimestampMilliRecords()
            throws Exception
    {
        String columnName = "t_timestamp";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, TIMESTAMP_TZ_MILLIS, OptionalInt.empty(), columnName, TIMESTAMP_TZ_MILLIS, REGULAR, Optional.empty());
        try (TestTable table = new TestTable(
                "test_timestamp_records_",
                ImmutableList.of(columnName),
                "VALUES timestamp '2012-10-31 01:00:00.123 America/New_York', timestamp '2012-10-31 01:00:00.123 America/Los_Angeles', timestamp '2012-10-31 01:00:00.123 UTC'")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(3L));
            assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(packDateTimeWithZone(ZonedDateTime.parse("2012-10-31T01:00:00.123Z").toInstant().toEpochMilli(), UTC_KEY)));
            assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(packDateTimeWithZone(ZonedDateTime.parse("2012-10-31T08:00:00.123Z").toInstant().toEpochMilli(), UTC_KEY)));
            assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(0L));
        }
    }

    @Test
    public void testUnicodeValues()
            throws Exception
    {
        String columnName = "t_string";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, createUnboundedVarcharType(), OptionalInt.empty(), columnName, createUnboundedVarcharType(), REGULAR, Optional.empty());
        try (TestTable table = new TestTable("test_unicode_", ImmutableList.of(columnName), "VALUES 'ab\uFAD8', 'ab\uD83D\uDD74'")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(2L));
            assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("ab\uFAD8")));
            assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("ab\uD83D\uDD74")));
            assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(0L));
        }
    }

    @Test
    public void testPartitionedTable()
            throws Exception
    {
        String columnName = "t_string";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, createUnboundedVarcharType(), OptionalInt.empty(), columnName, createUnboundedVarcharType(), REGULAR, Optional.empty());
        String partitionColumn = "t_int";

        try (TestTable table = new TestTable(
                "test_partitioned_table_",
                ImmutableList.of(columnName, partitionColumn),
                ImmutableList.of(partitionColumn),
                "VALUES ('a', 1), ('b', 1), ('c', 1), ('c', 2), ('d', 2), ('e', 2), (null, 1)")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            assertThat(addFileEntries).hasSize(2);

            for (AddFileEntry addFileEntry : addFileEntries) {
                assertThat(addFileEntry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = addFileEntry.getStats().get();
                if (addFileEntry.getPartitionValues().get(partitionColumn).equals("1")) {
                    assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("a")));
                    assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("c")));
                    assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(4L));
                    assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(1L));
                }
                else if (addFileEntry.getPartitionValues().get(partitionColumn).equals("2")) {
                    assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("c")));
                    assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEqualTo(Optional.of(utf8Slice("e")));
                    assertThat(fileStatistics.getNumRecords()).isEqualTo(Optional.of(3L));
                    assertThat(fileStatistics.getNullCount(columnName)).isEqualTo(Optional.of(0L));
                }
            }
        }
    }

    @Test
    public void testMultiFileTableWithNaNValue()
            throws Exception
    {
        String columnName = "key";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
        try (TestTable table = new TestTable(
                "test_multi_file_table_nan_value_",
                ImmutableList.of(columnName),
                ImmutableList.of(),
                "SELECT IF(custkey = 1143, nan(), CAST(custkey AS double)) FROM tpch.tiny.customer")) {
            assertUpdate("INSERT INTO %s SELECT CAST(nationkey AS double) FROM tpch.tiny.nation".formatted(table.getName()), 25);
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            assertThat(addFileEntries.size()).isGreaterThan(1);

            List<DeltaLakeFileStatistics> statistics = addFileEntries.stream().map(entry -> entry.getStats().get()).collect(toImmutableList());

            assertThat(statistics.stream().filter(stat -> stat.getMinColumnValue(columnHandle).isEmpty() && stat.getMaxColumnValue(columnHandle).isEmpty()).count()).isEqualTo(1);
            assertThat(statistics.stream().filter(stat -> stat.getMinColumnValue(columnHandle).isPresent() && stat.getMaxColumnValue(columnHandle).isPresent()).count()).isEqualTo(statistics.size() - 1);
        }
    }

    protected class TestTable
            implements AutoCloseable
    {
        private final String name;

        public TestTable(String name, List<String> columnNames, String values)
        {
            this(name, columnNames, ImmutableList.of(), values);
        }

        public TestTable(String name, List<String> columnNames, List<String> partitionNames, String values, Session session)
        {
            this.name = name + randomNameSuffix();
            String columns = columnNames.isEmpty() ? "" :
                    "(" + String.join(",", columnNames) + ")";
            String partitionedBy = partitionNames.isEmpty() ? "" :
                    format(", partitioned_by = ARRAY[%s]", partitionNames.stream().map(partitionName -> "'" + partitionName + "'").collect(Collectors.joining(",")));
            computeActual(session, format("CREATE TABLE %s %s WITH (location = 's3://%s/%1$s' %s) AS %s",
                    this.name, columns, bucketName, partitionedBy, values));
        }

        public TestTable(String name, List<String> columnNames, List<String> partitionNames, String values)
        {
            this(name, columnNames, partitionNames, values, getSession());
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
        return getTableActiveFiles(transactionLogAccess, format("s3://%s/%s", bucketName, tableName));
    }
}
