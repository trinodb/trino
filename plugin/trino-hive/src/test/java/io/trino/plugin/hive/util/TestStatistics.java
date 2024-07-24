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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.metastore.DoubleStatistics;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.spi.block.Block;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalDouble;

import static io.trino.plugin.hive.HiveColumnStatisticType.MAX_VALUE;
import static io.trino.plugin.hive.HiveColumnStatisticType.MIN_VALUE;
import static io.trino.plugin.hive.util.Statistics.createHiveColumnStatistics;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatistics
{
    @Test
    public void testCreateRealHiveColumnStatistics()
    {
        HiveColumnStatistics statistics;

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(-2391f)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(42f))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-2391d), OptionalDouble.of(42)));

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.NEGATIVE_INFINITY)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.POSITIVE_INFINITY))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.NaN)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(Float.NaN))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createRealColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(-15f)),
                MAX_VALUE, nativeValueToBlock(REAL, (long) floatToIntBits(-0f))));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-15d), OptionalDouble.of(-0d))); // TODO should we distinguish between -0 and 0?
    }

    private static HiveColumnStatistics createRealColumnStatistics(Map<HiveColumnStatisticType, Block> computedStatistics)
    {
        return createHiveColumnStatistics(computedStatistics, REAL, 1);
    }

    @Test
    public void testCreateDoubleHiveColumnStatistics()
    {
        HiveColumnStatistics statistics;

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, -2391d),
                MAX_VALUE, nativeValueToBlock(DOUBLE, 42d)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-2391d), OptionalDouble.of(42)));

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, Double.NEGATIVE_INFINITY),
                MAX_VALUE, nativeValueToBlock(DOUBLE, Double.POSITIVE_INFINITY)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, Double.NaN),
                MAX_VALUE, nativeValueToBlock(DOUBLE, Double.NaN)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty()));

        statistics = createDoubleColumnStatistics(ImmutableMap.of(
                MIN_VALUE, nativeValueToBlock(DOUBLE, -15d),
                MAX_VALUE, nativeValueToBlock(DOUBLE, -0d)));
        assertThat(statistics.getDoubleStatistics().get()).isEqualTo(new DoubleStatistics(OptionalDouble.of(-15d), OptionalDouble.of(-0d))); // TODO should we distinguish between -0 and 0?
    }

    private static HiveColumnStatistics createDoubleColumnStatistics(Map<HiveColumnStatisticType, Block> computedStatistics)
    {
        return createHiveColumnStatistics(computedStatistics, DOUBLE, 1);
    }
}
