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

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.metastore.BooleanStatistics;
import io.trino.plugin.hive.metastore.DateStatistics;
import io.trino.plugin.hive.metastore.DecimalStatistics;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.IntegerStatistics;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.assertj.core.api.Assertions.assertThat;

class TestColumnStatistics
{
    private static final JsonCodec<Map<String, ColumnStatistics>> MAP_COLUMN_STATISTICS_CODEC = JsonCodec.mapJsonCodec(String.class, ColumnStatistics.class);

    @Test
    void testRoundTrip()
    {
        assertThat(MAP_COLUMN_STATISTICS_CODEC.fromJson(MAP_COLUMN_STATISTICS_CODEC.toJson(ALL_KINDS)))
                .isEqualTo(ALL_KINDS);
    }

    @Test
    void testDeserialize()
    {
        assertThat(MAP_COLUMN_STATISTICS_CODEC.fromJson(ALL_KINDS_JSON))
                .isEqualTo(ALL_KINDS);
    }

    @Test
    void testRoundHive()
    {
        Map<String, ColumnStatistics> fromHive = HIVE_ALL_KINDS.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, e -> ColumnStatistics.fromHiveColumnStatistics(e.getValue())));
        assertThat(fromHive).isEqualTo(ALL_KINDS);

        assertThat(fromHive.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toHiveColumnStatistics())))
                .isEqualTo(HIVE_ALL_KINDS);
    }

    private static final String ALL_KINDS_JSON =
            """
            {
                "empty": {},
                "integer": {
                    "integerStatistics": {
                        "min": 1,
                        "max": 11
                    },
                    "maxValueSizeInBytes": 1,
                    "totalSizeInBytes": 110,
                    "nullsCount": 11,
                    "distinctValuesCount": 111
                },
                "double": {
                    "doubleStatistics": {
                        "min": 2.0,
                        "max": 12.0
                    },
                    "maxValueSizeInBytes": 2,
                    "totalSizeInBytes": 220,
                    "nullsCount": 22,
                    "distinctValuesCount": 222
                },
                "decimal": {
                    "decimalStatistics": {
                        "min": 3.0,
                        "max": 13.0
                    },
                    "maxValueSizeInBytes": 3,
                    "totalSizeInBytes": 330,
                    "nullsCount": 33,
                    "distinctValuesCount": 333
                },
                "date": {
                    "dateStatistics": {
                        "min": "0004-04-04",
                        "max": "0014-04-04"
                    },
                    "maxValueSizeInBytes": 4,
                    "totalSizeInBytes": 440,
                    "nullsCount": 44,
                    "distinctValuesCount": 444
                },
                "boolean": {
                    "booleanStatistics": {
                        "trueCount": 5,
                        "falseCount": 5
                    },
                    "maxValueSizeInBytes": 5,
                    "totalSizeInBytes": 550,
                    "nullsCount": 55,
                    "distinctValuesCount": 555
                },
                "basic": {
                    "maxValueSizeInBytes": 6,
                    "totalSizeInBytes": 660,
                    "nullsCount": 66,
                    "distinctValuesCount": 666
                }
            }
            """;

    private static final Map<String, ColumnStatistics> ALL_KINDS = ImmutableMap.<String, ColumnStatistics>builder()
            .put("empty", new ColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty()))
            .put("integer", new ColumnStatistics(
                    Optional.of(new ColumnStatistics.IntegerStatistics(OptionalLong.of(1), OptionalLong.of(11))),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(1),
                    OptionalLong.of(110),
                    OptionalLong.of(11),
                    OptionalLong.of(111)))
            .put("double", new ColumnStatistics(
                    Optional.empty(),
                    Optional.of(new ColumnStatistics.DoubleStatistics(OptionalDouble.of(2.0), OptionalDouble.of(12.0))),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(2),
                    OptionalLong.of(220),
                    OptionalLong.of(22),
                    OptionalLong.of(222)))
            .put("decimal", new ColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new ColumnStatistics.DecimalStatistics(Optional.of(new BigDecimal("3.0")), Optional.of(new BigDecimal("13.0")))),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(3),
                    OptionalLong.of(330),
                    OptionalLong.of(33),
                    OptionalLong.of(333)))
            .put("date", new ColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new ColumnStatistics.DateStatistics(Optional.of(LocalDate.of(4, 4, 4)), Optional.of(LocalDate.of(14, 4, 4)))),
                    Optional.empty(),
                    OptionalLong.of(4),
                    OptionalLong.of(440),
                    OptionalLong.of(44),
                    OptionalLong.of(444)))
            .put("boolean", new ColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new ColumnStatistics.BooleanStatistics(OptionalLong.of(5), OptionalLong.of(5))),
                    OptionalLong.of(5),
                    OptionalLong.of(550),
                    OptionalLong.of(55),
                    OptionalLong.of(555)))
            .put("basic", new ColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(6),
                    OptionalLong.of(660),
                    OptionalLong.of(66),
                    OptionalLong.of(666)))
            .buildOrThrow();

    private static final Map<String, HiveColumnStatistics> HIVE_ALL_KINDS = ImmutableMap.<String, HiveColumnStatistics>builder()
            .put("empty", new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty()))
            .put("integer", new HiveColumnStatistics(
                    Optional.of(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(11))),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(1),
                    OptionalLong.of(110),
                    OptionalLong.of(11),
                    OptionalLong.of(111)))
            .put("double", new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.of(new DoubleStatistics(OptionalDouble.of(2.0), OptionalDouble.of(12.0))),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(2),
                    OptionalLong.of(220),
                    OptionalLong.of(22),
                    OptionalLong.of(222)))
            .put("decimal", new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new DecimalStatistics(Optional.of(new BigDecimal("3.0")), Optional.of(new BigDecimal("13.0")))),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(3),
                    OptionalLong.of(330),
                    OptionalLong.of(33),
                    OptionalLong.of(333)))
            .put("date", new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new DateStatistics(Optional.of(LocalDate.of(4, 4, 4)), Optional.of(LocalDate.of(14, 4, 4)))),
                    Optional.empty(),
                    OptionalLong.of(4),
                    OptionalLong.of(440),
                    OptionalLong.of(44),
                    OptionalLong.of(444)))
            .put("boolean", new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(new BooleanStatistics(OptionalLong.of(5), OptionalLong.of(5))),
                    OptionalLong.of(5),
                    OptionalLong.of(550),
                    OptionalLong.of(55),
                    OptionalLong.of(555)))
            .put("basic", new HiveColumnStatistics(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.of(6),
                    OptionalLong.of(660),
                    OptionalLong.of(66),
                    OptionalLong.of(666)))
            .buildOrThrow();
}
