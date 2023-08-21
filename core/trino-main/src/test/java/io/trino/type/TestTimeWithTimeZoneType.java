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
import io.trino.spi.type.SqlTimeWithTimeZone;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimeWithTimeZoneType
        extends AbstractTestType
{
    public TestTimeWithTimeZoneType()
    {
        super(TIME_TZ_MILLIS, SqlTimeWithTimeZone.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIME_TZ_MILLIS.createBlockBuilder(null, 15);
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(1_111_000_000L, 0));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(1_111_000_000L, 1));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(1_111_000_000L, 2));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 3));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 4));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 5));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 6));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 7));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(3_333_000_000L, 8));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(3_333_000_000L, 9));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(4_444_000_000L, 10));
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return packTimeWithTimeZone(unpackTimeNanos((Long) value) + 10, unpackOffsetMinutes((Long) value));
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEmpty();
    }
}
