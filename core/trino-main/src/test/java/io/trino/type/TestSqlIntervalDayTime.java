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
package io.trino.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.SECOND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlIntervalDayTime
{
    @Test
    public void testRenderMicroseconds()
    {
        // a microsecond value renders the requested number of fractional digits, up to six
        assertThat(new SqlIntervalDayTime(1_000_000, 0, 6)).hasToString("0 00:00:01.000000");
        assertThat(new SqlIntervalDayTime(1_500_000, 0, 3)).hasToString("0 00:00:01.500");
        assertThat(new SqlIntervalDayTime(1_500_000, 0, 0)).hasToString("0 00:00:01");
        assertThat(new SqlIntervalDayTime(-1_500_000, 0, 3)).hasToString("-0 00:00:01.500");
    }

    @Test
    public void testRenderPicoseconds()
    {
        // the picoseconds within the microsecond extend the fraction to twelve digits
        assertThat(new SqlIntervalDayTime(1_000_000, 123_456, 9)).hasToString("0 00:00:01.000000123");
        assertThat(new SqlIntervalDayTime(1_500_000, 789_000, 9)).hasToString("0 00:00:01.500000789");
        assertThat(new SqlIntervalDayTime(1_500_000, 789_012, 12)).hasToString("0 00:00:01.500000789012");
        // negative values borrow across the microsecond boundary
        assertThat(new SqlIntervalDayTime(-1_000_000, 200_000, 9)).hasToString("-0 00:00:00.999999800");
    }

    @Test
    public void testLongIntervalToString()
    {
        assertThat(new LongInterval(1_500_000, 789_012)).hasToString("0 00:00:01.500000789012");
    }

    @Test
    public void testLongIntervalDayTimeTypeBlockRoundTrip()
    {
        // the picosecond-backed form stores a (micros, picosOfMicro) pair in a Fixed12 block
        LongIntervalDayTimeType type = new LongIntervalDayTimeType(DAY, SECOND, 9, 9);
        BlockBuilder builder = type.createBlockBuilder(null, 2);
        LongInterval value = new LongInterval(1_500_000, 789_000);
        type.writeObject(builder, value);
        builder.appendNull();
        Block block = builder.build();

        assertThat(type.getObject(block, 0)).isEqualTo(value);
        assertThat(type.getObjectValue(block, 0)).hasToString("0 00:00:01.500000789");
        assertThat(type.getObjectValue(block, 1)).isNull();
    }
}
