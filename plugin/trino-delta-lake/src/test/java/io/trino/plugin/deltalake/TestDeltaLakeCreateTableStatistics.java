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
import io.trino.Session;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeCreateTableStatistics
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA = "default";
    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "delta-test-create-table-statistics-" + randomNameSuffix();
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName));
        hiveMinioDataLake.start();
        return DeltaLakeQueryRunner.createS3DeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.of("delta.enable-non-concurrent-writes", "true"),
                hiveMinioDataLake.getMinio().getMinioAddress(),
                hiveMinioDataLake.getHiveHadoop());
    }

    @Test
    public void testComplexDataTypes()
            throws Exception
    {
        try (TestTable table = new TestTable(
                "test_complex_data_types_",
                ImmutableList.of("a", "b", "c", "d"),
                "VALUES (CAST(ROW(1, 2) AS ROW(x BIGINT, y BIGINT)), ARRAY[1, 2, 3], MAP(ARRAY[1, 2], ARRAY['a', 'b']), 'foo'), " +
                        "(CAST(ROW(null, 4) AS ROW(x BIGINT, y BIGINT)), ARRAY[-1, -2, -3], MAP(ARRAY[4], ARRAY['d']), null), " +
                        "(CAST(ROW(3, null) AS ROW(x BIGINT, y BIGINT)), ARRAY[4, 5], MAP(ARRAY[3], ARRAY['c']), 'moo')")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle("d", createUnboundedVarcharType(), OptionalInt.empty(), "d", createUnboundedVarcharType(), REGULAR, Optional.empty());
            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("foo")));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("moo")));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(1L));

            for (String complexColumn : ImmutableList.of("a", "b", "c")) {
                columnHandle = new DeltaLakeColumnHandle(complexColumn, createUnboundedVarcharType(), OptionalInt.empty(), complexColumn, createUnboundedVarcharType(), REGULAR, Optional.empty());
                assertThat(fileStatistics.getMaxColumnValue(columnHandle)).isEmpty();
                assertThat(fileStatistics.getMinColumnValue(columnHandle)).isEmpty();
                assertThat(fileStatistics.getNullCount(columnHandle)).isEmpty();
            }

            // dereference column statistics
            Type rowBaseType = RowType.rowType(RowType.field("x", BIGINT), RowType.field("x", BIGINT));

            columnHandle = new DeltaLakeColumnHandle("a", rowBaseType, OptionalInt.empty(), "a", rowBaseType, REGULAR, Optional.of(new DeltaLakeColumnProjectionInfo(BIGINT, ImmutableList.of(0), ImmutableList.of("x"))));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(1L));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(3L));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(1L));

            columnHandle = new DeltaLakeColumnHandle("a", rowBaseType, OptionalInt.empty(), "a", rowBaseType, REGULAR, Optional.of(new DeltaLakeColumnProjectionInfo(BIGINT, ImmutableList.of(1), ImmutableList.of("y"))));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(2L));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(4L));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(1L));
        }
    }

    @Test
    public void testPrimitiveTypeInsideRowColumn()
            throws Exception
    {
        try (TestTable table = new TestTable(
                "test_primitive_type_inside_row_column_",
                ImmutableList.of("x"),
                "VALUES " +
                        "ROW(CAST(ROW(1, 'stringValue', ARRAY[1, 2, 3], true, MAP(ARRAY[1], ARRAY['mapValue1'])) AS ROW(a BIGINT, b VARCHAR, c ARRAY(BIGINT), d BOOLEAN, e MAP(BIGINT, VARCHAR)))), " +
                        "ROW(CAST(ROW(2, null, ARRAY[4, null, 6], true, MAP(ARRAY[2], ARRAY[null])) AS ROW(a BIGINT, b VARCHAR, c ARRAY(BIGINT), d BOOLEAN, e MAP(BIGINT, VARCHAR)))), " +
                        "ROW(CAST(null AS ROW(a BIGINT, b VARCHAR, c ARRAY(BIGINT), d BOOLEAN, e MAP(BIGINT, VARCHAR))))")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            Type rowBaseType = RowType.rowType(
                    RowType.field("a", BIGINT),
                    RowType.field("b", createUnboundedVarcharType()),
                    RowType.field("c", new ArrayType(BIGINT)),
                    RowType.field("d", BOOLEAN),
                    RowType.field("e", new MapType(BIGINT, VARCHAR, new TypeOperators())));

            // x.a
            DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle("x", rowBaseType, OptionalInt.empty(), "x", rowBaseType, REGULAR, Optional.of(new DeltaLakeColumnProjectionInfo(BIGINT, ImmutableList.of(0), ImmutableList.of("a"))));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(1L));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(2L));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(1L));

            // x.b
            columnHandle = new DeltaLakeColumnHandle("x", rowBaseType, OptionalInt.empty(), "x", rowBaseType, REGULAR, Optional.of(new DeltaLakeColumnProjectionInfo(createUnboundedVarcharType(), ImmutableList.of(1), ImmutableList.of("b"))));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("stringValue")));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("stringValue")));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(2L));

            // x.c
            columnHandle = new DeltaLakeColumnHandle("x", rowBaseType, OptionalInt.empty(), "x", rowBaseType, REGULAR, Optional.of(new DeltaLakeColumnProjectionInfo(new ArrayType(BIGINT), ImmutableList.of(2), ImmutableList.of("c"))));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.empty());

            // x.d
            columnHandle = new DeltaLakeColumnHandle("x", rowBaseType, OptionalInt.empty(), "x", rowBaseType, REGULAR, Optional.of(new DeltaLakeColumnProjectionInfo(BOOLEAN, ImmutableList.of(3), ImmutableList.of("d"))));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(1L));

            // x.e
            columnHandle = new DeltaLakeColumnHandle("x", rowBaseType, OptionalInt.empty(), "x", rowBaseType, REGULAR, Optional.of(new DeltaLakeColumnProjectionInfo(new MapType(BIGINT, VARCHAR, new TypeOperators()), ImmutableList.of(4), ImmutableList.of("e"))));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.empty());
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
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, DoubleType.DOUBLE, OptionalInt.empty(), columnName, DoubleType.DOUBLE, REGULAR, Optional.empty());
        try (TestTable table = new TestTable("test_nan_", ImmutableList.of(columnName), format("VALUES CAST(nan() AS %1$s), CAST(0.0 AS %1$s)", type))) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(2L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.empty());
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesInf(String type)
            throws Exception
    {
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(NEGATIVE_INFINITY));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(POSITIVE_INFINITY));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(0L));
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesInfAndNaN(String type)
            throws Exception
    {
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.empty());
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesNaNPositive(String type)
            throws Exception
    {
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.empty());
        }
    }

    @Test(dataProvider = "doubleTypes")
    public void testDoubleTypesNaNNegative(String type)
            throws Exception
    {
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.empty());
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
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(0L));
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(0.0));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(1.0));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(2L));
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(4L));
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(LocalDate.parse("2011-08-08").toEpochDay()));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(LocalDate.parse("2013-08-09").toEpochDay()));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(0L));
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            assertEquals(
                    fileStatistics.getMinColumnValue(columnHandle),
                    Optional.of(packDateTimeWithZone(ZonedDateTime.parse("2012-10-31T01:00:00.123Z").toInstant().toEpochMilli(), UTC_KEY)));
            assertEquals(
                    fileStatistics.getMaxColumnValue(columnHandle),
                    Optional.of(packDateTimeWithZone(ZonedDateTime.parse("2012-10-31T08:00:00.123Z").toInstant().toEpochMilli(), UTC_KEY)));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(0L));
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

            assertEquals(fileStatistics.getNumRecords(), Optional.of(2L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("ab\uFAD8")));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("ab\uD83D\uDD74")));
            assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(0L));
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
            assertEquals(addFileEntries.size(), 2);

            for (AddFileEntry addFileEntry : addFileEntries) {
                assertThat(addFileEntry.getStats()).isPresent();
                DeltaLakeFileStatistics fileStatistics = addFileEntry.getStats().get();
                if (addFileEntry.getPartitionValues().get(partitionColumn).equals("1")) {
                    assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("a")));
                    assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("c")));
                    assertEquals(fileStatistics.getNumRecords(), Optional.of(4L));
                    assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(1L));
                }
                else if (addFileEntry.getPartitionValues().get(partitionColumn).equals("2")) {
                    assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(utf8Slice("c")));
                    assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(utf8Slice("e")));
                    assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
                    assertEquals(fileStatistics.getNullCount(columnHandle), Optional.of(0L));
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

            assertEquals(statistics.stream().filter(stat -> stat.getMinColumnValue(columnHandle).isEmpty() && stat.getMaxColumnValue(columnHandle).isEmpty()).count(), 1);
            assertEquals(
                    statistics.stream().filter(stat -> stat.getMinColumnValue(columnHandle).isPresent() && stat.getMaxColumnValue(columnHandle).isPresent()).count(),
                    statistics.size() - 1);
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
        return TestingDeltaLakeUtils.getAddFileEntries(format("s3://%s/%s", bucketName, tableName));
    }
}
