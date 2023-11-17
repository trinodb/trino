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
package io.trino.orc.metadata.statistics;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import org.junit.jupiter.api.Test;

import static io.trino.orc.metadata.statistics.AbstractStatisticsBuilderTest.StatisticsType.TIMESTAMP;
import static io.trino.orc.metadata.statistics.TimestampStatistics.TIMESTAMP_VALUE_BYTES;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampStatisticsBuilder
        extends AbstractStatisticsBuilderTest<TimestampStatisticsBuilder, Long>
{
    public TestTimestampStatisticsBuilder()
    {
        super(TIMESTAMP, () -> new TimestampStatisticsBuilder(new NoOpBloomFilterBuilder()), TimestampStatisticsBuilder::addValue);
    }

    @Test
    public void testMinMaxValues()
    {
        assertMinMaxValues(0L, 0L);
        assertMinMaxValues(42L, 42L);
        assertMinMaxValues(MIN_VALUE, MIN_VALUE);
        assertMinMaxValues(MAX_VALUE, MAX_VALUE);

        assertMinMaxValues(0L, 42L);
        assertMinMaxValues(42L, 42L);
        assertMinMaxValues(MIN_VALUE, 42L);
        assertMinMaxValues(42L, MAX_VALUE);
        assertMinMaxValues(MIN_VALUE, MAX_VALUE);

        assertValues(-42L, 0L, ContiguousSet.create(Range.closed(-42L, 0L), DiscreteDomain.longs()).asList());
        assertValues(-42L, 42L, ContiguousSet.create(Range.closed(-42L, 42L), DiscreteDomain.longs()).asList());
        assertValues(0L, 42L, ContiguousSet.create(Range.closed(0L, 42L), DiscreteDomain.longs()).asList());
        assertValues(MIN_VALUE, MIN_VALUE + 42, ContiguousSet.create(Range.closed(MIN_VALUE, MIN_VALUE + 42), DiscreteDomain.longs()).asList());
        assertValues(MAX_VALUE - 42L, MAX_VALUE, ContiguousSet.create(Range.closed(MAX_VALUE - 42L, MAX_VALUE), DiscreteDomain.longs()).asList());
    }

    @Test
    public void testMinAverageValueBytes()
    {
        assertMinAverageValueBytes(0L, ImmutableList.of());
        assertMinAverageValueBytes(TIMESTAMP_VALUE_BYTES, ImmutableList.of(42L));
        assertMinAverageValueBytes(TIMESTAMP_VALUE_BYTES, ImmutableList.of(0L));
        assertMinAverageValueBytes(TIMESTAMP_VALUE_BYTES, ImmutableList.of(0L, 42L, 42L, 43L));
    }

    @Test
    public void testBloomFilter()
    {
        TimestampStatisticsBuilder statisticsBuilder = new TimestampStatisticsBuilder(new Utf8BloomFilterBuilder(3, 0.01));
        statisticsBuilder.addValue(314L);
        statisticsBuilder.addValue(1011L);
        statisticsBuilder.addValue(4242L);
        BloomFilter bloomFilter = statisticsBuilder.buildColumnStatistics().getBloomFilter();
        assertThat(bloomFilter).isNotNull();
        assertThat(bloomFilter.testLong(314L)).isTrue();
        assertThat(bloomFilter.testLong(1011L)).isTrue();
        assertThat(bloomFilter.testLong(4242L)).isTrue();
        assertThat(bloomFilter.testLong(100L)).isFalse();
    }
}
