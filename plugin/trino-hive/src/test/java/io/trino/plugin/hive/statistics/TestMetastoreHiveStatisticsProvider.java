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
package io.trino.plugin.hive.statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metastore.DateStatistics;
import io.trino.metastore.DecimalStatistics;
import io.trino.metastore.DoubleStatistics;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HivePartition;
import io.trino.metastore.IntegerStatistics;
import io.trino.metastore.PartitionStatistics;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static io.trino.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.metastore.HivePartition.UNPARTITIONED_ID;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.Partitions.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS;
import static io.trino.plugin.hive.HivePartitionManager.parsePartition;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.PartitionsRowCount;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateDataSize;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateDataSizeForPartitioningKey;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateDistinctPartitionKeys;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateDistinctValuesCount;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateNullsFraction;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateNullsFractionForPartitioningKey;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculatePartitionsRowCount;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateRange;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.calculateRangeForPartitioningKey;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.convertPartitionValueToDouble;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.createDataColumnStatistics;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.getDistinctValuesCount;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.getPartitionsSample;
import static io.trino.plugin.hive.statistics.AbstractHiveStatisticsProvider.validatePartitionStatistics;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.NaN;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMetastoreHiveStatisticsProvider
{
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final String PARTITION = "partition";
    private static final String COLUMN = "column";
    private static final DecimalType DECIMAL = createDecimalType(5, 3);

    private static final HiveColumnHandle PARTITION_COLUMN_1 = createBaseColumn("p1", 0, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
    private static final HiveColumnHandle PARTITION_COLUMN_2 = createBaseColumn("p2", 1, HIVE_LONG, BIGINT, PARTITION_KEY, Optional.empty());

    @Test
    public void testGetPartitionsSample()
    {
        HivePartition p1 = partition("p1=string1/p2=1234");
        HivePartition p2 = partition("p1=string2/p2=2345");
        HivePartition p3 = partition("p1=string3/p2=3456");
        HivePartition p4 = partition("p1=string4/p2=4567");
        HivePartition p5 = partition("p1=string5/p2=5678");

        assertThat(getPartitionsSample(ImmutableList.of(p1), 1)).isEqualTo(ImmutableList.of(p1));
        assertThat(getPartitionsSample(ImmutableList.of(p1), 2)).isEqualTo(ImmutableList.of(p1));
        assertThat(getPartitionsSample(ImmutableList.of(p1, p2), 2)).isEqualTo(ImmutableList.of(p1, p2));
        assertThat(getPartitionsSample(ImmutableList.of(p1, p2, p3), 2)).isEqualTo(ImmutableList.of(p1, p3));
        assertThat(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 1)).isEqualTo(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 1));
        assertThat(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 3)).isEqualTo(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 3));
        assertThat(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4, p5), 3)).isEqualTo(ImmutableList.of(p1, p5, p4));
    }

    @Test
    public void testValidatePartitionStatistics()
    {
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(-1, 0, 0, 0))
                        .build(),
                invalidPartitionStatistics("fileCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, -1, 0, 0))
                        .build(),
                invalidPartitionStatistics("rowCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, -1, 0))
                        .build(),
                invalidPartitionStatistics("inMemoryDataSizeInBytes must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, -1))
                        .build(),
                invalidPartitionStatistics("onDiskDataSizeInBytes must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setMaxValueSizeInBytes(-1).build()))
                        .build(),
                invalidColumnStatistics("maxValueSizeInBytes must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setAverageColumnLength(-1).build()))
                        .build(),
                invalidColumnStatistics("averageColumnLength must be greater than or equal to zero: -1.0"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(-1).build()))
                        .build(),
                invalidColumnStatistics("nullsCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(1).build()))
                        .build(),
                invalidColumnStatistics("nullsCount must be less than or equal to rowCount. nullsCount: 1. rowCount: 0."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setDistinctValuesWithNullCount(-1).build()))
                        .build(),
                invalidColumnStatistics("distinctValuesCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(-1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("integerStatistics.min must be less than or equal to integerStatistics.max. integerStatistics.min: 1. integerStatistics.max: -1."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(-1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("doubleStatistics.min must be less than or equal to doubleStatistics.max. doubleStatistics.min: 1.0. doubleStatistics.max: -1.0."));
        validatePartitionStatistics(
                TABLE,
                ImmutableMap.of(
                        PARTITION,
                        PartitionStatistics.builder()
                                .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                                .setColumnStatistics(ImmutableMap.of(COLUMN, createDoubleColumnStatistics(OptionalDouble.of(NaN), OptionalDouble.of(NaN), OptionalLong.empty(), OptionalLong.empty())))
                                .build()));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createDecimalColumnStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(-1)), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("decimalStatistics.min must be less than or equal to decimalStatistics.max. decimalStatistics.min: 1. decimalStatistics.max: -1."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createDateColumnStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(-1)), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("dateStatistics.min must be less than or equal to dateStatistics.max. dateStatistics.min: 1970-01-02. dateStatistics.max: 1969-12-31."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createBooleanColumnStatistics(OptionalLong.of(-1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("trueCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.of(-1), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("falseCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("booleanStatistics.trueCount must be less than or equal to rowCount. booleanStatistics.trueCount: 1. rowCount: 0."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.of(1), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("booleanStatistics.falseCount must be less than or equal to rowCount. booleanStatistics.falseCount: 1. rowCount: 0."));
    }

    @Test
    public void testCalculatePartitionsRowCount()
    {
        assertThat(calculatePartitionsRowCount(ImmutableList.of(), 0)).isEmpty();
        assertThat(calculatePartitionsRowCount(ImmutableList.of(PartitionStatistics.empty()), 1)).isEmpty();
        assertThat(calculatePartitionsRowCount(ImmutableList.of(PartitionStatistics.empty(), PartitionStatistics.empty()), 2)).isEmpty();
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10)), 1))
                .isEqualTo(Optional.of(new PartitionsRowCount(10, 10)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10)), 2))
                .isEqualTo(Optional.of(new PartitionsRowCount(10, 20)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), PartitionStatistics.empty()), 2))
                .isEqualTo(Optional.of(new PartitionsRowCount(10, 20)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(20)), 2))
                .isEqualTo(Optional.of(new PartitionsRowCount(15, 30)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(20)), 3))
                .isEqualTo(Optional.of(new PartitionsRowCount(15, 45)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(20), PartitionStatistics.empty()), 3))
                .isEqualTo(Optional.of(new PartitionsRowCount(15, 45)));

        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(100), rowsCount(1000)), 3))
                .isEqualTo(Optional.of(new PartitionsRowCount((10 + 100 + 1000) / 3.0, 10 + 100 + 1000)));
        // Exclude outliers from average row count
        assertThat(calculatePartitionsRowCount(ImmutableList.<PartitionStatistics>builder()
                        .addAll(nCopies(10, rowsCount(100)))
                        .add(rowsCount(1))
                        .add(rowsCount(1000))
                        .build(),
                50))
                .isEqualTo(Optional.of(new PartitionsRowCount(100, (100 * 48) + 1 + 1000)));
    }

    @Test
    public void testCalculateDistinctPartitionKeys()
    {
        assertThat(calculateDistinctPartitionKeys(PARTITION_COLUMN_1, ImmutableList.of())).isEqualTo(0);
        assertThat(calculateDistinctPartitionKeys(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=string1/p2=1234")))).isEqualTo(1);
        assertThat(calculateDistinctPartitionKeys(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string2/p2=1234")))).isEqualTo(2);
        assertThat(calculateDistinctPartitionKeys(
                PARTITION_COLUMN_2,
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string2/p2=1234")))).isEqualTo(1);
        assertThat(calculateDistinctPartitionKeys(
                PARTITION_COLUMN_2,
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string1/p2=1235")))).isEqualTo(2);
        assertThat(calculateDistinctPartitionKeys(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=string1/p2=1235")))).isEqualTo(1);
        assertThat(calculateDistinctPartitionKeys(
                PARTITION_COLUMN_2,
                ImmutableList.of(partition("p1=123/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=1235")))).isEqualTo(1);
        assertThat(calculateDistinctPartitionKeys(
                PARTITION_COLUMN_2,
                ImmutableList.of(partition("p1=123/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__")))).isEqualTo(0);
    }

    @Test
    public void testCalculateNullsFractionForPartitioningKey()
    {
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=string1/p2=1234")),
                ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                2000,
                0)).isEqualTo(0.0);
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=string1/p2=1234")),
                ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                2000,
                4000)).isEqualTo(0.0);
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234")),
                ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000)),
                2000,
                4000)).isEqualTo(0.25);
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234")),
                ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", PartitionStatistics.empty()),
                2000,
                4000)).isEqualTo(0.5);
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234")),
                ImmutableMap.of(),
                2000,
                4000)).isEqualTo(0.5);
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=__HIVE_DEFAULT_PARTITION__/p2=4321")),
                ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000), "p1=__HIVE_DEFAULT_PARTITION__/p2=4321", rowsCount(2000)),
                3000,
                4000)).isEqualTo(0.75);
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=__HIVE_DEFAULT_PARTITION__/p2=4321")),
                ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000), "p1=__HIVE_DEFAULT_PARTITION__/p2=4321", PartitionStatistics.empty()),
                3000,
                4000)).isEqualTo(1.0);
        assertThat(calculateNullsFractionForPartitioningKey(
                PARTITION_COLUMN_1,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=__HIVE_DEFAULT_PARTITION__/p2=4321")),
                ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000), "p1=__HIVE_DEFAULT_PARTITION__/p2=4321", PartitionStatistics.empty()),
                4000,
                4000)).isEqualTo(1.0);
    }

    @Test
    public void testCalculateDataSizeForPartitioningKey()
    {
        assertThat(calculateDataSizeForPartitioningKey(
                PARTITION_COLUMN_2,
                BIGINT,
                ImmutableList.of(partition("p1=string1/p2=1234")),
                ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                2000)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSizeForPartitioningKey(
                PARTITION_COLUMN_1,
                VARCHAR,
                ImmutableList.of(partition("p1=string1/p2=1234")),
                ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                2000)).isEqualTo(Estimate.of(7000));
        assertThat(calculateDataSizeForPartitioningKey(
                PARTITION_COLUMN_1,
                VARCHAR,
                ImmutableList.of(partition("p1=string1/p2=1234")),
                ImmutableMap.of("p1=string1/p2=1234", PartitionStatistics.empty()),
                2000)).isEqualTo(Estimate.of(14000));
        assertThat(calculateDataSizeForPartitioningKey(
                PARTITION_COLUMN_1,
                VARCHAR,
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=str2/p2=1234")),
                ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000), "p1=str2/p2=1234", rowsCount(2000)),
                3000)).isEqualTo(Estimate.of(15000));
        assertThat(calculateDataSizeForPartitioningKey(
                PARTITION_COLUMN_1,
                VARCHAR,
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=str2/p2=1234")),
                ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000), "p1=str2/p2=1234", PartitionStatistics.empty()),
                3000)).isEqualTo(Estimate.of(19000));
        assertThat(calculateDataSizeForPartitioningKey(
                PARTITION_COLUMN_1,
                VARCHAR,
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=str2/p2=1234")),
                ImmutableMap.of(),
                3000)).isEqualTo(Estimate.of(33000));
        assertThat(calculateDataSizeForPartitioningKey(
                PARTITION_COLUMN_1,
                VARCHAR,
                ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=str2/p2=1234")),
                ImmutableMap.of(),
                3000)).isEqualTo(Estimate.of(12000));
    }

    @Test
    public void testCalculateRangeForPartitioningKey()
    {
        assertThat(calculateRangeForPartitioningKey(
                PARTITION_COLUMN_1,
                VARCHAR,
                ImmutableList.of(partition("p1=string1/p2=1234")))).isEqualTo(Optional.empty());
        assertThat(calculateRangeForPartitioningKey(
                PARTITION_COLUMN_2,
                BIGINT,
                ImmutableList.of(partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__")))).isEqualTo(Optional.empty());
        assertThat(calculateRangeForPartitioningKey(
                PARTITION_COLUMN_2,
                BIGINT,
                ImmutableList.of(partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__")))).isEqualTo(Optional.empty());
        assertThat(calculateRangeForPartitioningKey(
                PARTITION_COLUMN_2,
                BIGINT,
                ImmutableList.of(partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=1")))).isEqualTo(Optional.of(new DoubleRange(1, 1)));
        assertThat(calculateRangeForPartitioningKey(
                PARTITION_COLUMN_2,
                BIGINT,
                ImmutableList.of(partition("p1=string1/p2=2"), partition("p1=string1/p2=1")))).isEqualTo(Optional.of(new DoubleRange(1, 2)));
    }

    @Test
    public void testConvertPartitionValueToDouble()
    {
        assertConvertPartitionValueToDouble(BIGINT, "123456", 123456);
        assertConvertPartitionValueToDouble(INTEGER, "12345", 12345);
        assertConvertPartitionValueToDouble(SMALLINT, "1234", 1234);
        assertConvertPartitionValueToDouble(TINYINT, "123", 123);
        assertConvertPartitionValueToDouble(DOUBLE, "0.1", 0.1);
        assertConvertPartitionValueToDouble(REAL, "0.2", 0.2f);
        assertConvertPartitionValueToDouble(createDecimalType(5, 2), "123.45", 123.45);
        assertConvertPartitionValueToDouble(createDecimalType(25, 5), "12345678901234567890.12345", 12345678901234567890.12345);
        assertConvertPartitionValueToDouble(DATE, "1970-01-02", 1);
    }

    private static void assertConvertPartitionValueToDouble(Type type, String value, double expected)
    {
        Object trinoValue = parsePartitionValue(format("p=%s", value), value, type).getValue();
        assertThat(convertPartitionValueToDouble(type, trinoValue)).isEqualTo(OptionalDouble.of(expected));
    }

    @Test
    public void testCreateDataColumnStatistics()
    {
        assertThat(createDataColumnStatistics(COLUMN, BIGINT, 1000, ImmutableList.of())).isEqualTo(ColumnStatistics.empty());
        assertThat(createDataColumnStatistics(COLUMN, BIGINT, 1000, ImmutableList.of(PartitionStatistics.empty(), PartitionStatistics.empty()))).isEqualTo(ColumnStatistics.empty());
        assertThat(createDataColumnStatistics(
                COLUMN,
                BIGINT,
                1000,
                ImmutableList.of(new PartitionStatistics(HiveBasicStatistics.createZeroStatistics(), ImmutableMap.of("column2", HiveColumnStatistics.empty()))))).isEqualTo(ColumnStatistics.empty());
    }

    @Test
    public void testCalculateDistinctValuesCount()
    {
        assertThat(getDistinctValuesCount(COLUMN, PartitionStatistics.empty())).isEmpty();
        assertThat(getDistinctValuesCount(COLUMN, distinctValuesCount(1))).hasValue(1);

        // Hive includes null in distinct values count. If column has nulls, decrease distinct values count by 1.
        assertThat(getDistinctValuesCount(
                COLUMN,
                new PartitionStatistics(
                        new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(10), OptionalLong.empty(), OptionalLong.empty()),
                        ImmutableMap.of(COLUMN, HiveColumnStatistics.builder()
                                .setNullsCount(3)
                                .setDistinctValuesWithNullCount(5)
                                .build()))))
                .hasValue(4);

        // if column only has a non-null row, count should not be zero
        assertThat(getDistinctValuesCount(
                COLUMN,
                new PartitionStatistics(
                        new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(10), OptionalLong.empty(), OptionalLong.empty()),
                        ImmutableMap.of(COLUMN, HiveColumnStatistics.builder()
                                .setNullsCount(3)
                                .setDistinctValuesWithNullCount(1)
                                .build()))))
                .hasValue(1);

        // Hive may have a distinct value much larger than the row count
        assertThat(getDistinctValuesCount(
                COLUMN,
                new PartitionStatistics(
                        new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(5), OptionalLong.empty(), OptionalLong.empty()),
                        ImmutableMap.of(COLUMN, HiveColumnStatistics.builder()
                                .setDistinctValuesWithNullCount(10)
                                .build()))))
                .hasValue(5);
        assertThat(getDistinctValuesCount(
                COLUMN,
                new PartitionStatistics(
                        new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(5), OptionalLong.empty(), OptionalLong.empty()),
                        ImmutableMap.of(COLUMN, HiveColumnStatistics.builder()
                                .setNullsCount(3)
                                .setDistinctValuesWithNullCount(10)
                                .build()))))
                .hasValue(2);

        assertThat(getDistinctValuesCount(COLUMN, booleanDistinctValuesCount(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()))).isEmpty();
        assertThat(getDistinctValuesCount(COLUMN, booleanDistinctValuesCount(OptionalLong.of(1), OptionalLong.of(0), OptionalLong.empty()))).hasValue(1);
        assertThat(getDistinctValuesCount(COLUMN, booleanDistinctValuesCount(OptionalLong.of(10), OptionalLong.empty(), OptionalLong.empty()))).isEmpty();
        assertThat(getDistinctValuesCount(COLUMN, booleanDistinctValuesCount(OptionalLong.of(10), OptionalLong.of(10), OptionalLong.empty()))).hasValue(2);
        assertThat(getDistinctValuesCount(COLUMN, booleanDistinctValuesCount(OptionalLong.empty(), OptionalLong.of(10), OptionalLong.empty()))).isEmpty();
        assertThat(getDistinctValuesCount(COLUMN, booleanDistinctValuesCount(OptionalLong.of(0), OptionalLong.of(10), OptionalLong.empty()))).hasValue(1);
        assertThat(getDistinctValuesCount(COLUMN, booleanDistinctValuesCount(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.empty()))).hasValue(0);

        assertThat(calculateDistinctValuesCount(COLUMN, ImmutableList.of())).isEqualTo(Estimate.unknown());
        assertThat(calculateDistinctValuesCount(COLUMN, ImmutableList.of(PartitionStatistics.empty(), PartitionStatistics.empty()))).isEqualTo(Estimate.unknown());
        assertThat(calculateDistinctValuesCount(COLUMN, ImmutableList.of(distinctValuesCount(1), distinctValuesCount(2)))).isEqualTo(Estimate.of(2));
        assertThat(calculateDistinctValuesCount(COLUMN, ImmutableList.of(distinctValuesCount(1), PartitionStatistics.empty()))).isEqualTo(Estimate.of(1));
        assertThat(calculateDistinctValuesCount(COLUMN, ImmutableList.of(
                booleanDistinctValuesCount(OptionalLong.of(0), OptionalLong.of(10), OptionalLong.empty()),
                booleanDistinctValuesCount(OptionalLong.of(1), OptionalLong.of(10), OptionalLong.empty())))).isEqualTo(Estimate.of(2));
    }

    @Test
    public void testCalculateNullsFraction()
    {
        assertThat(calculateNullsFraction(COLUMN, ImmutableList.of())).isEqualTo(Estimate.unknown());
        assertThat(calculateNullsFraction(COLUMN, ImmutableList.of(PartitionStatistics.empty()))).isEqualTo(Estimate.unknown());
        assertThat(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCount(1000)))).isEqualTo(Estimate.unknown());
        assertThat(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCount(1000), nullsCount(500)))).isEqualTo(Estimate.unknown());
        assertThat(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCount(1000), nullsCount(500), rowsCountAndNullsCount(1000, 500)))).isEqualTo(Estimate.of(0.5));
        assertThat(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCountAndNullsCount(2000, 200), rowsCountAndNullsCount(1000, 100)))).isEqualTo(Estimate.of(0.1));
        assertThat(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCountAndNullsCount(0, 0), rowsCountAndNullsCount(0, 0)))).isEqualTo(Estimate.of(0));
    }

    @Test
    public void testCalculateDataSize()
    {
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(), 0)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(), 1000)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(PartitionStatistics.empty()), 1000)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(rowsCount(1000)), 1000)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(dataSize(1000)), 1000)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(dataSize(1000), rowsCount(1000)), 1000)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(500, 2)), 2000)).isEqualTo(Estimate.of(4000));
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(0, 0)), 2000)).isEqualTo(Estimate.unknown());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(0, 0)), 0)).isEqualTo(Estimate.zero());
        assertThat(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(1000, 0)), 2000)).isEqualTo(Estimate.of(0));
        assertThat(calculateDataSize(
                COLUMN,
                ImmutableList.of(
                        rowsCountAndDataSize(500, 2),
                        rowsCountAndDataSize(1000, 5)),
                5000)).isEqualTo(Estimate.of(20000));
        assertThat(calculateDataSize(
                COLUMN,
                ImmutableList.of(
                        dataSize(1000),
                        rowsCountAndDataSize(500, 2),
                        rowsCount(3000),
                        rowsCountAndDataSize(1000, 5)),
                5000)).isEqualTo(Estimate.of(20000));
    }

    @Test
    public void testCalculateRange()
    {
        assertThat(calculateRange(VARCHAR, ImmutableList.of())).isEqualTo(Optional.empty());
        assertThat(calculateRange(VARCHAR, ImmutableList.of(integerRange(OptionalLong.empty(), OptionalLong.empty())))).isEqualTo(Optional.empty());
        assertThat(calculateRange(VARCHAR, ImmutableList.of(integerRange(1, 2)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(BIGINT, ImmutableList.of(integerRange(1, 2)))).isEqualTo(Optional.of(new DoubleRange(1, 2)));
        assertThat(calculateRange(BIGINT, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE)))).isEqualTo(Optional.of(new DoubleRange(Long.MIN_VALUE, Long.MAX_VALUE)));
        assertThat(calculateRange(INTEGER, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE)))).isEqualTo(Optional.of(new DoubleRange(Integer.MIN_VALUE, Integer.MAX_VALUE)));
        assertThat(calculateRange(SMALLINT, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE)))).isEqualTo(Optional.of(new DoubleRange(Short.MIN_VALUE, Short.MAX_VALUE)));
        assertThat(calculateRange(TINYINT, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE)))).isEqualTo(Optional.of(new DoubleRange(Byte.MIN_VALUE, Byte.MAX_VALUE)));
        assertThat(calculateRange(BIGINT, ImmutableList.of(integerRange(1, 5), integerRange(3, 7)))).isEqualTo(Optional.of(new DoubleRange(1, 7)));
        assertThat(calculateRange(BIGINT, ImmutableList.of(integerRange(OptionalLong.empty(), OptionalLong.empty()), integerRange(3, 7)))).isEqualTo(Optional.of(new DoubleRange(3, 7)));
        assertThat(calculateRange(BIGINT, ImmutableList.of(integerRange(OptionalLong.empty(), OptionalLong.of(8)), integerRange(3, 7)))).isEqualTo(Optional.of(new DoubleRange(3, 7)));
        assertThat(calculateRange(DOUBLE, ImmutableList.of(integerRange(1, 2)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(REAL, ImmutableList.of(integerRange(1, 2)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(OptionalDouble.empty(), OptionalDouble.empty())))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(0.1, 0.2)))).isEqualTo(Optional.of(new DoubleRange(0.1, 0.2)));
        assertThat(calculateRange(BIGINT, ImmutableList.of(doubleRange(0.1, 0.2)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(0.1, 0.2), doubleRange(0.15, 0.25)))).isEqualTo(Optional.of(new DoubleRange(0.1, 0.25)));
        assertThat(calculateRange(REAL, ImmutableList.of(doubleRange(0.1, 0.2), doubleRange(0.15, 0.25)))).isEqualTo(Optional.of(new DoubleRange(0.1, 0.25)));
        assertThat(calculateRange(REAL, ImmutableList.of(doubleRange(OptionalDouble.empty(), OptionalDouble.of(0.2)), doubleRange(0.15, 0.25)))).isEqualTo(Optional.of(new DoubleRange(0.15, 0.25)));
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(NaN, 0.2)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(0.1, NaN)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(NaN, NaN)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)))).isEqualTo(Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertThat(calculateRange(REAL, ImmutableList.of(doubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)))).isEqualTo(Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY)))).isEqualTo(Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertThat(calculateRange(DOUBLE, ImmutableList.of(doubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), doubleRange(0.1, 0.2)))).isEqualTo(Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertThat(calculateRange(DATE, ImmutableList.of(doubleRange(0.1, 0.2)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DATE, ImmutableList.of(dateRange("1970-01-01", "1970-01-02")))).isEqualTo(Optional.of(new DoubleRange(0, 1)));
        assertThat(calculateRange(DATE, ImmutableList.of(dateRange(Optional.empty(), Optional.empty())))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DATE, ImmutableList.of(dateRange(Optional.of("1970-01-01"), Optional.empty())))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DATE, ImmutableList.of(dateRange("1970-01-01", "1970-01-05"), dateRange("1970-01-03", "1970-01-07")))).isEqualTo(Optional.of(new DoubleRange(0, 6)));
        assertThat(calculateRange(DECIMAL, ImmutableList.of(doubleRange(0.1, 0.2)))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DECIMAL, ImmutableList.of(decimalRange(BigDecimal.valueOf(1), BigDecimal.valueOf(5))))).isEqualTo(Optional.of(new DoubleRange(1, 5)));
        assertThat(calculateRange(DECIMAL, ImmutableList.of(decimalRange(Optional.empty(), Optional.empty())))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DECIMAL, ImmutableList.of(decimalRange(Optional.of(BigDecimal.valueOf(1)), Optional.empty())))).isEqualTo(Optional.empty());
        assertThat(calculateRange(DECIMAL, ImmutableList.of(decimalRange(BigDecimal.valueOf(1), BigDecimal.valueOf(5)), decimalRange(BigDecimal.valueOf(3), BigDecimal.valueOf(7))))).isEqualTo(Optional.of(new DoubleRange(1, 7)));
    }

    @Test
    public void testGetTableStatistics()
    {
        String partitionName = "p1=string1/p2=1234";
        // Hive includes the null in distinct count, so provided value is 301, but actual distinct count is 300
        PartitionStatistics statistics = PartitionStatistics.builder()
                .setBasicStatistics(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(1000), OptionalLong.empty(), OptionalLong.empty()))
                .setColumnStatistics(ImmutableMap.of(COLUMN, createIntegerColumnStatistics(OptionalLong.of(-100), OptionalLong.of(100), OptionalLong.of(500), OptionalLong.of(301))))
                .build();
        HiveStatisticsProvider statisticsProvider = new AbstractHiveStatisticsProvider()
        {
            @Override
            protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
            {
                return ImmutableMap.of(partitionName, statistics);
            }
        };
        HiveColumnHandle columnHandle = createBaseColumn(COLUMN, 2, HIVE_LONG, BIGINT, REGULAR, Optional.empty());
        TableStatistics expected = TableStatistics.builder()
                .setRowCount(Estimate.of(1000))
                .setColumnStatistics(
                        PARTITION_COLUMN_1,
                        ColumnStatistics.builder()
                                .setDataSize(Estimate.of(7000))
                                .setNullsFraction(Estimate.of(0))
                                .setDistinctValuesCount(Estimate.of(1))
                                .build())
                .setColumnStatistics(
                        PARTITION_COLUMN_2,
                        ColumnStatistics.builder()
                                .setRange(new DoubleRange(1234, 1234))
                                .setNullsFraction(Estimate.of(0))
                                .setDistinctValuesCount(Estimate.of(1))
                                .build())
                .setColumnStatistics(
                        columnHandle,
                        ColumnStatistics.builder()
                                .setRange(new DoubleRange(-100, 100))
                                .setNullsFraction(Estimate.of(0.5))
                                .setDistinctValuesCount(Estimate.of(300))
                                .build())
                .build();
        assertThat(statisticsProvider.getTableStatistics(
                SESSION,
                TABLE,
                ImmutableMap.of(
                        "p1", PARTITION_COLUMN_1,
                        "p2", PARTITION_COLUMN_2,
                        COLUMN, columnHandle),
                ImmutableMap.of(
                        "p1", VARCHAR,
                        "p2", BIGINT,
                        COLUMN, BIGINT),
                ImmutableList.of(partition(partitionName)))).isEqualTo(expected);
    }

    @Test
    public void testGetTableStatisticsUnpartitioned()
    {
        // Hive includes the null in distinct count, so provided value is 301, but actual distinct count is 300
        PartitionStatistics statistics = PartitionStatistics.builder()
                .setBasicStatistics(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(1000), OptionalLong.empty(), OptionalLong.empty()))
                .setColumnStatistics(ImmutableMap.of(COLUMN, createIntegerColumnStatistics(OptionalLong.of(-100), OptionalLong.of(100), OptionalLong.of(500), OptionalLong.of(301))))
                .build();
        HiveStatisticsProvider statisticsProvider = new AbstractHiveStatisticsProvider()
        {
            @Override
            protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
            {
                return ImmutableMap.of(UNPARTITIONED_ID, statistics);
            }
        };

        HiveColumnHandle columnHandle = createBaseColumn(COLUMN, 2, HIVE_LONG, BIGINT, REGULAR, Optional.empty());

        TableStatistics expected = TableStatistics.builder()
                .setRowCount(Estimate.of(1000))
                .setColumnStatistics(
                        columnHandle,
                        ColumnStatistics.builder()
                                .setRange(new DoubleRange(-100, 100))
                                .setNullsFraction(Estimate.of(0.5))
                                .setDistinctValuesCount(Estimate.of(300))
                                .build())
                .build();
        assertThat(statisticsProvider.getTableStatistics(
                SESSION,
                TABLE,
                ImmutableMap.of(COLUMN, columnHandle),
                ImmutableMap.of(COLUMN, BIGINT),
                ImmutableList.of(new HivePartition(TABLE)))).isEqualTo(expected);
    }

    @Test
    public void testGetTableStatisticsEmpty()
    {
        String partitionName = "p1=string1/p2=1234";
        HiveStatisticsProvider statisticsProvider = new AbstractHiveStatisticsProvider()
        {
            @Override
            protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
            {
                return ImmutableMap.of(partitionName, PartitionStatistics.empty());
            }
        };
        assertThat(statisticsProvider.getTableStatistics(
                SESSION,
                TABLE,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(partition(partitionName)))).isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsSampling()
    {
        HiveStatisticsProvider statisticsProvider = new AbstractHiveStatisticsProvider() {
            @Override
            protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
            {
                assertThat(table).isEqualTo(TABLE);
                assertThat(hivePartitions).hasSize(1);
                return ImmutableMap.of();
            }
        };
        ConnectorSession session = getHiveSession(new HiveConfig()
                .setPartitionStatisticsSampleSize(1));
        statisticsProvider.getTableStatistics(
                session,
                TABLE,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string1/p2=1235")));
    }

    @Test
    public void testGetTableStatisticsValidationFailure()
    {
        PartitionStatistics corruptedStatistics = PartitionStatistics.builder()
                .setBasicStatistics(new HiveBasicStatistics(-1, 0, 0, 0))
                .build();
        String partitionName = "p1=string1/p2=1234";
        HiveStatisticsProvider statisticsProvider = new AbstractHiveStatisticsProvider()
        {
            @Override
            protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
            {
                return ImmutableMap.of(partitionName, corruptedStatistics);
            }
        };
        assertThatThrownBy(() -> statisticsProvider.getTableStatistics(
                getHiveSession(new HiveConfig().setIgnoreCorruptedStatistics(false)),
                TABLE,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(partition(partitionName))))
                .isInstanceOf(TrinoException.class)
                .hasFieldOrPropertyWithValue("errorCode", HIVE_CORRUPTED_COLUMN_STATISTICS.toErrorCode());
        assertThat(statisticsProvider.getTableStatistics(
                getHiveSession(new HiveConfig().setIgnoreCorruptedStatistics(true)),
                TABLE,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(partition(partitionName)))).isEqualTo(TableStatistics.empty());
    }

    @Test
    public void testEmptyTableStatisticsForPartitionColumnsWhenStatsAreEmpty()
    {
        HiveStatisticsProvider statisticsProvider = new AbstractHiveStatisticsProvider()
        {
            @Override
            protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
            {
                return ImmutableMap.of("p1=string1/p2=1234", PartitionStatistics.empty());
            }
        };
        testEmptyTableStatisticsForPartitionColumns(statisticsProvider);
    }

    @Test
    public void testEmptyTableStatisticsForPartitionColumnsWhenStatsAreMissing()
    {
        HiveStatisticsProvider statisticsProvider = new AbstractHiveStatisticsProvider()
        {
            @Override
            protected Map<String, PartitionStatistics> getPartitionsStatistics(ConnectorSession session, SchemaTableName table, List<HivePartition> hivePartitions, Set<String> columns)
            {
                return ImmutableMap.of();
            }
        };
        testEmptyTableStatisticsForPartitionColumns(statisticsProvider);
    }

    private void testEmptyTableStatisticsForPartitionColumns(HiveStatisticsProvider statisticsProvider)
    {
        String partitionName1 = "p1=string1/p2=1234";
        String partitionName2 = "p1=string2/p2=1235";
        String partitionName3 = "p1=string3/p2=1236";
        String partitionName4 = "p1=string4/p2=1237";
        String partitionName5 = format("p1=%s/p2=1237", HIVE_DEFAULT_DYNAMIC_PARTITION);
        String partitionName6 = format("p1=string5/p2=%s", HIVE_DEFAULT_DYNAMIC_PARTITION);

        HiveColumnHandle columnHandle = createBaseColumn(COLUMN, 2, HIVE_LONG, BIGINT, REGULAR, Optional.empty());

        TableStatistics expected = TableStatistics.builder()
                .setColumnStatistics(
                        PARTITION_COLUMN_1,
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.16666666666666666))
                                .setDistinctValuesCount(Estimate.of(5.0))
                                .build())
                .setColumnStatistics(
                        PARTITION_COLUMN_2,
                        ColumnStatistics.builder()
                                .setRange(new DoubleRange(1234.0, 1237.0))
                                .setNullsFraction(Estimate.of(0.16666666666666666))
                                .setDistinctValuesCount(Estimate.of(4.0))
                                .build())
                .build();

        assertThat(statisticsProvider.getTableStatistics(
                getHiveSession(new HiveConfig().setIgnoreCorruptedStatistics(true)),
                TABLE,
                ImmutableMap.of(
                        "p1", PARTITION_COLUMN_1,
                        "p2", PARTITION_COLUMN_2,
                        COLUMN, columnHandle),
                ImmutableMap.of(
                        "p1", VARCHAR,
                        "p2", BIGINT,
                        COLUMN, BIGINT),
                ImmutableList.of(
                        partition(partitionName1),
                        partition(partitionName2),
                        partition(partitionName3),
                        partition(partitionName4),
                        partition(partitionName5),
                        partition(partitionName6)))).isEqualTo(expected);
    }

    private static void assertInvalidStatistics(PartitionStatistics partitionStatistics, String expectedMessage)
    {
        assertThatThrownBy(() -> validatePartitionStatistics(TABLE, ImmutableMap.of(PARTITION, partitionStatistics)))
                .isInstanceOf(TrinoException.class)
                .hasFieldOrPropertyWithValue("errorCode", HIVE_CORRUPTED_COLUMN_STATISTICS.toErrorCode())
                .hasMessage(expectedMessage);
    }

    private static String invalidPartitionStatistics(String message)
    {
        return format("Corrupted partition statistics (Table: %s Partition: [%s]): %s", TABLE, PARTITION, message);
    }

    private static String invalidColumnStatistics(String message)
    {
        return format("Corrupted partition statistics (Table: %s Partition: [%s] Column: %s): %s", TABLE, PARTITION, COLUMN, message);
    }

    private static HivePartition partition(String name)
    {
        return parsePartition(TABLE, name, ImmutableList.of(PARTITION_COLUMN_1, PARTITION_COLUMN_2));
    }

    private static PartitionStatistics rowsCount(long rowsCount)
    {
        return new PartitionStatistics(new HiveBasicStatistics(0, rowsCount, 0, 0), ImmutableMap.of());
    }

    private static PartitionStatistics nullsCount(long nullsCount)
    {
        return new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(nullsCount).build()));
    }

    private static PartitionStatistics dataSize(long dataSize)
    {
        return new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setAverageColumnLength(dataSize).build()));
    }

    private static PartitionStatistics rowsCountAndNullsCount(long rowsCount, long nullsCount)
    {
        return new PartitionStatistics(
                new HiveBasicStatistics(0, rowsCount, 0, 0),
                ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(nullsCount).build()));
    }

    private static PartitionStatistics rowsCountAndDataSize(long rowsCount, long averageColumnLength)
    {
        return new PartitionStatistics(
                new HiveBasicStatistics(0, rowsCount, 0, 0),
                ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setAverageColumnLength(averageColumnLength).build()));
    }

    private static PartitionStatistics distinctValuesCount(long count)
    {
        return new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of(COLUMN, HiveColumnStatistics.builder()
                .setDistinctValuesWithNullCount(count)
                .build()));
    }

    private static PartitionStatistics booleanDistinctValuesCount(OptionalLong trueCount, OptionalLong falseCount, OptionalLong nullsCount)
    {
        return new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(),
                ImmutableMap.of(COLUMN, createBooleanColumnStatistics(trueCount, falseCount, nullsCount)));
    }

    private static HiveColumnStatistics integerRange(long min, long max)
    {
        return integerRange(OptionalLong.of(min), OptionalLong.of(max));
    }

    private static HiveColumnStatistics integerRange(OptionalLong min, OptionalLong max)
    {
        return HiveColumnStatistics.builder()
                .setIntegerStatistics(new IntegerStatistics(min, max))
                .build();
    }

    private static HiveColumnStatistics doubleRange(double min, double max)
    {
        return doubleRange(OptionalDouble.of(min), OptionalDouble.of(max));
    }

    private static HiveColumnStatistics doubleRange(OptionalDouble min, OptionalDouble max)
    {
        return HiveColumnStatistics.builder()
                .setDoubleStatistics(new DoubleStatistics(min, max))
                .build();
    }

    private static HiveColumnStatistics dateRange(String min, String max)
    {
        return dateRange(Optional.of(min), Optional.of(max));
    }

    private static HiveColumnStatistics dateRange(Optional<String> min, Optional<String> max)
    {
        return HiveColumnStatistics.builder()
                .setDateStatistics(new DateStatistics(min.map(TestMetastoreHiveStatisticsProvider::parseDate), max.map(TestMetastoreHiveStatisticsProvider::parseDate)))
                .build();
    }

    private static LocalDate parseDate(String date)
    {
        return LocalDate.parse(date);
    }

    private static HiveColumnStatistics decimalRange(BigDecimal min, BigDecimal max)
    {
        return decimalRange(Optional.of(min), Optional.of(max));
    }

    private static HiveColumnStatistics decimalRange(Optional<BigDecimal> min, Optional<BigDecimal> max)
    {
        return HiveColumnStatistics.builder()
                .setDecimalStatistics(new DecimalStatistics(min, max))
                .build();
    }
}
