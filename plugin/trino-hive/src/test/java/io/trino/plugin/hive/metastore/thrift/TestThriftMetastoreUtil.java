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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import io.trino.hive.thrift.metastore.BinaryColumnStatsData;
import io.trino.hive.thrift.metastore.BooleanColumnStatsData;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.Date;
import io.trino.hive.thrift.metastore.DateColumnStatsData;
import io.trino.hive.thrift.metastore.DecimalColumnStatsData;
import io.trino.hive.thrift.metastore.DoubleColumnStatsData;
import io.trino.hive.thrift.metastore.LongColumnStatsData;
import io.trino.hive.thrift.metastore.StringColumnStatsData;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.TrinoPrincipal;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.binaryStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.booleanStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.dateStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.decimalStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.doubleStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.longStats;
import static io.trino.hive.thrift.metastore.ColumnStatisticsData.stringStats;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreApiColumnStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getBasicStatisticsWithSparkFallback;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.toMetastoreDecimal;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.updateStatisticsParameters;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static org.apache.hadoop.hive.serde.serdeConstants.BIGINT_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BINARY_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.BOOLEAN_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DATE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DECIMAL_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.DOUBLE_TYPE_NAME;
import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestThriftMetastoreUtil
{
    @Test
    public void testLongStatsToColumnStatistics()
    {
        LongColumnStatsData longColumnStatsData = new LongColumnStatsData();
        longColumnStatsData.setLowValue(0);
        longColumnStatsData.setHighValue(100);
        longColumnStatsData.setNumNulls(1);
        longColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BIGINT_TYPE_NAME, longStats(longColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(1000));

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(100)))
                        .setNullsCount(1)
                        .setDistinctValuesCount(19)
                        .build());
    }

    @Test
    public void testEmptyLongStatsToColumnStatistics()
    {
        LongColumnStatsData emptyLongColumnStatsData = new LongColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BIGINT_TYPE_NAME, longStats(emptyLongColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty()))
                        .build());
    }

    @Test
    public void testDoubleStatsToColumnStatistics()
    {
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setLowValue(0);
        doubleColumnStatsData.setHighValue(100);
        doubleColumnStatsData.setNumNulls(1);
        doubleColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(doubleColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(1000));

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(100)))
                        .setNullsCount(1)
                        .setDistinctValuesCount(19)
                        .build());
    }

    @Test
    public void testEmptyDoubleStatsToColumnStatistics()
    {
        DoubleColumnStatsData emptyDoubleColumnStatsData = new DoubleColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(emptyDoubleColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()))
                        .build());
    }

    @Test
    public void testDecimalStatsToColumnStatistics()
    {
        DecimalColumnStatsData decimalColumnStatsData = new DecimalColumnStatsData();
        BigDecimal low = new BigDecimal("0");
        decimalColumnStatsData.setLowValue(toMetastoreDecimal(low));
        BigDecimal high = new BigDecimal("100");
        decimalColumnStatsData.setHighValue(toMetastoreDecimal(high));
        decimalColumnStatsData.setNumNulls(1);
        decimalColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DECIMAL_TYPE_NAME, decimalStats(decimalColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(1000));

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDecimalStatistics(new DecimalStatistics(Optional.of(low), Optional.of(high)))
                        .setNullsCount(1)
                        .setDistinctValuesCount(19)
                        .build());
    }

    @Test
    public void testEmptyDecimalStatsToColumnStatistics()
    {
        DecimalColumnStatsData emptyDecimalColumnStatsData = new DecimalColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DECIMAL_TYPE_NAME, decimalStats(emptyDecimalColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty()))
                        .build());
    }

    @Test
    public void testBooleanStatsToColumnStatistics()
    {
        BooleanColumnStatsData booleanColumnStatsData = new BooleanColumnStatsData();
        booleanColumnStatsData.setNumTrues(100);
        booleanColumnStatsData.setNumFalses(10);
        booleanColumnStatsData.setNumNulls(0);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BOOLEAN_TYPE_NAME, booleanStats(booleanColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setBooleanStatistics(new BooleanStatistics(OptionalLong.of(100), OptionalLong.of(10)))
                        .setNullsCount(0)
                        .build());
    }

    @Test
    public void testImpalaGeneratedBooleanStatistics()
    {
        BooleanColumnStatsData statsData = new BooleanColumnStatsData(1L, -1L, 2L);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BOOLEAN_TYPE_NAME, booleanStats(statsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                        .setNullsCount(2)
                        .build());
    }

    @Test
    public void testEmptyBooleanStatsToColumnStatistics()
    {
        BooleanColumnStatsData emptyBooleanColumnStatsData = new BooleanColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BOOLEAN_TYPE_NAME, booleanStats(emptyBooleanColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                        .build());
    }

    @Test
    public void testDateStatsToColumnStatistics()
    {
        DateColumnStatsData dateColumnStatsData = new DateColumnStatsData();
        dateColumnStatsData.setLowValue(new Date(1000));
        dateColumnStatsData.setHighValue(new Date(2000));
        dateColumnStatsData.setNumNulls(1);
        dateColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DATE_TYPE_NAME, dateStats(dateColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(1000));

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1000)), Optional.of(LocalDate.ofEpochDay(2000))))
                        .setNullsCount(1)
                        .setDistinctValuesCount(19)
                        .build());
    }

    @Test
    public void testEmptyDateStatsToColumnStatistics()
    {
        DateColumnStatsData emptyDateColumnStatsData = new DateColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DATE_TYPE_NAME, dateStats(emptyDateColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty()))
                        .build());
    }

    @Test
    public void testStringStatsToColumnStatistics()
    {
        StringColumnStatsData stringColumnStatsData = new StringColumnStatsData();
        stringColumnStatsData.setMaxColLen(100);
        stringColumnStatsData.setAvgColLen(23.333);
        stringColumnStatsData.setNumNulls(1);
        stringColumnStatsData.setNumDVs(20);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", STRING_TYPE_NAME, stringStats(stringColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(2));

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setMaxValueSizeInBytes(100)
                        .setTotalSizeInBytes(23)
                        .setNullsCount(1)
                        .setDistinctValuesCount(1)
                        .build());
    }

    @Test
    public void testEmptyStringColumnStatsData()
    {
        StringColumnStatsData emptyStringColumnStatsData = new StringColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", STRING_TYPE_NAME, stringStats(emptyStringColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(actual, HiveColumnStatistics.builder().build());
    }

    @Test
    public void testBinaryStatsToColumnStatistics()
    {
        BinaryColumnStatsData binaryColumnStatsData = new BinaryColumnStatsData();
        binaryColumnStatsData.setMaxColLen(100);
        binaryColumnStatsData.setAvgColLen(22.2);
        binaryColumnStatsData.setNumNulls(2);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BINARY_TYPE_NAME, binaryStats(binaryColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(4));

        assertEquals(
                actual,
                HiveColumnStatistics.builder()
                        .setMaxValueSizeInBytes(100)
                        .setTotalSizeInBytes(44)
                        .setNullsCount(2)
                        .build());
    }

    @Test
    public void testEmptyBinaryStatsToColumnStatistics()
    {
        BinaryColumnStatsData emptyBinaryColumnStatsData = new BinaryColumnStatsData();
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", BINARY_TYPE_NAME, binaryStats(emptyBinaryColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.empty());

        assertEquals(actual, HiveColumnStatistics.builder().build());
    }

    @Test
    public void testSingleDistinctValue()
    {
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setNumNulls(10);
        doubleColumnStatsData.setNumDVs(1);
        ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(doubleColumnStatsData));
        HiveColumnStatistics actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(10));

        assertEquals(actual.getNullsCount(), OptionalLong.of(10));
        assertEquals(actual.getDistinctValuesCount(), OptionalLong.of(0));

        doubleColumnStatsData = new DoubleColumnStatsData();
        doubleColumnStatsData.setNumNulls(10);
        doubleColumnStatsData.setNumDVs(1);
        columnStatisticsObj = new ColumnStatisticsObj("my_col", DOUBLE_TYPE_NAME, doubleStats(doubleColumnStatsData));
        actual = fromMetastoreApiColumnStatistics(columnStatisticsObj, OptionalLong.of(11));

        assertEquals(actual.getNullsCount(), OptionalLong.of(10));
        assertEquals(actual.getDistinctValuesCount(), OptionalLong.of(1));
    }

    @Test
    public void testSparkFallbackGetBasicStatistics()
    {
        // only spark stats
        Map<String, String> tableParameters = Map.of(
                "spark.sql.statistics.numFiles", "1",
                "spark.sql.statistics.numRows", "2",
                "spark.sql.statistics.rawDataSize", "3",
                "spark.sql.statistics.totalSize", "4");
        HiveBasicStatistics actual = getBasicStatisticsWithSparkFallback(tableParameters);
        assertEquals(actual, new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)));
        actual = getHiveBasicStatistics(tableParameters);
        assertEquals(actual, new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()));
        // empty hive and not empty spark stats
        tableParameters = Map.of(
                "numFiles", "0",
                "numRows", "0",
                "rawDataSize", "0",
                "totalSize", "0",
                "spark.sql.statistics.numFiles", "1",
                "spark.sql.statistics.numRows", "2",
                "spark.sql.statistics.rawDataSize", "3",
                "spark.sql.statistics.totalSize", "4");
        actual = getBasicStatisticsWithSparkFallback(tableParameters);
        assertEquals(actual, new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)));
        actual = getHiveBasicStatistics(tableParameters);
        assertEquals(actual, new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0)));
        //  not empty hive and not empty spark stats
        tableParameters = Map.of(
                "numFiles", "10",
                "numRows", "20",
                "rawDataSize", "30",
                "totalSize", "40",
                "spark.sql.statistics.numFiles", "1",
                "spark.sql.statistics.numRows", "2",
                "spark.sql.statistics.rawDataSize", "3",
                "spark.sql.statistics.totalSize", "4");
        actual = getBasicStatisticsWithSparkFallback(tableParameters);
        assertEquals(actual, new HiveBasicStatistics(OptionalLong.of(10), OptionalLong.of(20), OptionalLong.of(30), OptionalLong.of(40)));
    }

    @Test
    public void testBasicStatisticsRoundTrip()
    {
        testBasicStatisticsRoundTrip(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()));
        testBasicStatisticsRoundTrip(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.empty(), OptionalLong.of(2), OptionalLong.empty()));
        testBasicStatisticsRoundTrip(new HiveBasicStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)));
    }

    private static void testBasicStatisticsRoundTrip(HiveBasicStatistics expected)
    {
        assertEquals(getHiveBasicStatistics(updateStatisticsParameters(ImmutableMap.of(), expected)), expected);
    }

    @Test
    public void testListApplicableRoles()
    {
        TrinoPrincipal admin = new TrinoPrincipal(USER, "admin");

        Multimap<String, String> inheritance = ImmutableMultimap.<String, String>builder()
                .put("a", "b1")
                .put("a", "b2")
                .put("b1", "d")
                .put("b1", "e")
                .put("b2", "d")
                .put("b2", "e")
                .put("d", "u")
                .put("e", "w")
                .build();

        assertThat(ThriftMetastoreUtil.listApplicableRoles(
                new HivePrincipal(ROLE, "a"),
                principal -> inheritance.get(principal.getName()).stream()
                        .map(name -> new RoleGrant(admin, name, false))
                        .collect(toImmutableSet())))
                .containsOnly(
                        new RoleGrant(admin, "b1", false),
                        new RoleGrant(admin, "b2", false),
                        new RoleGrant(admin, "d", false),
                        new RoleGrant(admin, "e", false),
                        new RoleGrant(admin, "u", false),
                        new RoleGrant(admin, "w", false));
    }
}
