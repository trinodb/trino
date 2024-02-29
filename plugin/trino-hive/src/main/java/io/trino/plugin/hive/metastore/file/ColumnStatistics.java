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
package io.trino.plugin.hive.metastore.file;

import com.google.errorprone.annotations.Immutable;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public record ColumnStatistics(
        Optional<IntegerStatistics> integerStatistics,
        Optional<DoubleStatistics> doubleStatistics,
        Optional<DecimalStatistics> decimalStatistics,
        Optional<DateStatistics> dateStatistics,
        Optional<BooleanStatistics> booleanStatistics,
        OptionalLong maxValueSizeInBytes,
        OptionalDouble averageColumnLength,
        OptionalLong totalSizeInBytes,
        OptionalLong nullsCount,
        OptionalLong distinctValuesCount)
{
    public ColumnStatistics
    {
        requireNonNull(integerStatistics, "integerStatistics is null");
        requireNonNull(doubleStatistics, "doubleStatistics is null");
        requireNonNull(decimalStatistics, "decimalStatistics is null");
        requireNonNull(dateStatistics, "dateStatistics is null");
        requireNonNull(booleanStatistics, "booleanStatistics is null");
        requireNonNull(maxValueSizeInBytes, "maxValueSizeInBytes is null");
        requireNonNull(averageColumnLength, "averageColumnLength is null");
        requireNonNull(totalSizeInBytes, "totalSizeInBytes is null");
        checkArgument(averageColumnLength.isEmpty() || totalSizeInBytes.isEmpty(), "both averageColumnLength and totalSizeInBytes are present");
        requireNonNull(nullsCount, "nullsCount is null");
        requireNonNull(distinctValuesCount, "distinctValuesCount is null");

        Set<String> presentStatistics = new LinkedHashSet<>();
        integerStatistics.ifPresent(ignored -> presentStatistics.add("integerStatistics"));
        doubleStatistics.ifPresent(ignored -> presentStatistics.add("doubleStatistics"));
        decimalStatistics.ifPresent(ignored -> presentStatistics.add("decimalStatistics"));
        dateStatistics.ifPresent(ignored -> presentStatistics.add("dateStatistics"));
        booleanStatistics.ifPresent(ignored -> presentStatistics.add("booleanStatistics"));
        checkArgument(presentStatistics.size() <= 1, "multiple type specific statistic objects are present: %s", presentStatistics);
    }

    public static ColumnStatistics fromHiveColumnStatistics(HiveColumnStatistics hiveColumnStatistics)
    {
        return new ColumnStatistics(
                hiveColumnStatistics.getIntegerStatistics().map(stat -> new IntegerStatistics(stat.getMin(), stat.getMax())),
                hiveColumnStatistics.getDoubleStatistics().map(stat -> new DoubleStatistics(stat.getMin(), stat.getMax())),
                hiveColumnStatistics.getDecimalStatistics().map(stat -> new DecimalStatistics(stat.getMin(), stat.getMax())),
                hiveColumnStatistics.getDateStatistics().map(stat -> new DateStatistics(stat.getMin(), stat.getMax())),
                hiveColumnStatistics.getBooleanStatistics().map(stat -> new BooleanStatistics(stat.getTrueCount(), stat.getFalseCount())),
                hiveColumnStatistics.getMaxValueSizeInBytes(),
                hiveColumnStatistics.getAverageColumnLength(),
                OptionalLong.empty(),
                hiveColumnStatistics.getNullsCount(),
                hiveColumnStatistics.getDistinctValuesWithNullCount());
    }

    public HiveColumnStatistics toHiveColumnStatistics(HiveBasicStatistics basicStatistics)
    {
        OptionalDouble averageColumnLength = this.averageColumnLength;
        if (totalSizeInBytes.isPresent() && basicStatistics.getRowCount().orElse(0) > 0 && nullsCount().isPresent()) {
            long nonNullCount = basicStatistics.getRowCount().getAsLong() - nullsCount().orElseThrow();
            if (nonNullCount > 0) {
                averageColumnLength = OptionalDouble.of(totalSizeInBytes.getAsLong() / (double) nonNullCount);
            }
        }
        return new HiveColumnStatistics(
                integerStatistics.map(stat -> new io.trino.plugin.hive.metastore.IntegerStatistics(stat.min(), stat.max())),
                doubleStatistics.map(stat -> new io.trino.plugin.hive.metastore.DoubleStatistics(stat.min(), stat.max())),
                decimalStatistics.map(stat -> new io.trino.plugin.hive.metastore.DecimalStatistics(stat.min(), stat.max())),
                dateStatistics.map(stat -> new io.trino.plugin.hive.metastore.DateStatistics(stat.min(), stat.max())),
                booleanStatistics.map(stat -> new io.trino.plugin.hive.metastore.BooleanStatistics(stat.trueCount(), stat.falseCount())),
                maxValueSizeInBytes,
                averageColumnLength,
                nullsCount,
                distinctValuesCount);
    }

    public record IntegerStatistics(OptionalLong min, OptionalLong max)
    {
        public IntegerStatistics
        {
            requireNonNull(min, "min is null");
            requireNonNull(max, "max is null");
        }
    }

    public record DoubleStatistics(OptionalDouble min, OptionalDouble max)
    {
        public DoubleStatistics
        {
            requireNonNull(min, "min is null");
            requireNonNull(max, "max is null");
        }
    }

    public record DecimalStatistics(Optional<BigDecimal> min, Optional<BigDecimal> max)
    {
        public DecimalStatistics
        {
            requireNonNull(min, "min is null");
            requireNonNull(max, "max is null");
        }
    }

    public record DateStatistics(Optional<LocalDate> min, Optional<LocalDate> max)
    {
        public DateStatistics
        {
            requireNonNull(min, "min is null");
            requireNonNull(max, "max is null");
        }
    }

    public record BooleanStatistics(OptionalLong trueCount, OptionalLong falseCount)
    {
        public BooleanStatistics
        {
            requireNonNull(trueCount, "trueCount is null");
            requireNonNull(falseCount, "falseCount is null");
        }
    }
}
