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
package io.trino.plugin.raptor.legacy.storage.organization;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestTemporalFunction
{
    private static final DateTime DATE_TIME = new DateTime(1970, 1, 2, 0, 0, 0, UTC);

    @Test
    public void testDateBlock()
    {
        BlockBuilder blockBuilder = DATE.createBlockBuilder(null, 2);
        DATE.writeLong(blockBuilder, 13);
        DATE.writeLong(blockBuilder, 42);
        Block block = blockBuilder.build();

        assertThat(TemporalFunction.getDay(DATE, block, 0)).isEqualTo(13);
        assertThat(TemporalFunction.getDay(DATE, block, 1)).isEqualTo(42);
    }

    @Test
    public void testTimestampBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_MILLIS.createBlockBuilder(null, 4);

        // start and end of UTC day
        TIMESTAMP_MILLIS.writeLong(blockBuilder, DATE_TIME.getMillis() * MICROSECONDS_PER_MILLISECOND);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, (DATE_TIME.getMillis() + Duration.ofHours(23).toMillis()) * MICROSECONDS_PER_MILLISECOND);

        Block block = blockBuilder.build();

        assertThat(TemporalFunction.getDay(TIMESTAMP_MILLIS, block, 0)).isEqualTo(1);
        assertThat(TemporalFunction.getDay(TIMESTAMP_MILLIS, block, 1)).isEqualTo(1);
    }

    @Test
    public void testDateShardRange()
    {
        assertThat(TemporalFunction.getDayFromRange(dateRange(2, 2))).isEqualTo(2);
        assertThat(TemporalFunction.getDayFromRange(dateRange(13, 13))).isEqualTo(13);

        // date is determined from lowest shard
        assertThat(TemporalFunction.getDayFromRange(dateRange(2, 5))).isEqualTo(2);
    }

    @Test
    public void testTimestampShardRange()
    {
        // The time frame should be look like following:

        // time range covers full day of day 1
        assertThat(TemporalFunction.getDayFromRange(timeRange(DATE_TIME.getMillis(), Duration.ofDays(1)))).isEqualTo(1);
        // time range covers full day of day 1 and 2
        assertThat(TemporalFunction.getDayFromRange(timeRange(DATE_TIME.getMillis(), Duration.ofDays(2)))).isEqualTo(2);
        // time range covers 13 hours of day 1 and 11 hours of day 2
        assertThat(TemporalFunction.getDayFromRange(timeRange(DATE_TIME.getMillis() + Duration.ofHours(11).toMillis(), Duration.ofHours(24)))).isEqualTo(1);
        // time range covers 11 hours of day 0 and 13 hours of day 1
        assertThat(TemporalFunction.getDayFromRange(timeRange(DATE_TIME.getMillis() + Duration.ofHours(13).toMillis(), Duration.ofHours(24)))).isEqualTo(2);
    }

    private static ShardRange dateRange(int start, int end)
    {
        return ShardRange.of(new Tuple(DATE, start), new Tuple(DATE, end));
    }

    private static ShardRange timeRange(long start, Duration duration)
    {
        return ShardRange.of(
                new Tuple(TIMESTAMP_MILLIS, start),
                new Tuple(TIMESTAMP_MILLIS, start + duration.toMillis()));
    }
}
