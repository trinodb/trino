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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.TSRange;
import org.testng.annotations.Test;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.prestosql.spi.type.TSRangeType.TSRANGE_MILLIS;
import static java.lang.Math.abs;
import static org.testng.Assert.assertEquals;

public class TestTSRangeType
        extends AbstractTestType
{
    private static final int RANGE_ENTRIES = 2;
    private static final int RANGE_LENGTH = 2 * SIZE_OF_LONG;

    public TestTSRangeType()
    {
        super(TSRANGE_MILLIS, TSRange.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TSRANGE_MILLIS.createBlockBuilder(null, RANGE_ENTRIES);
        TSRANGE_MILLIS.writeSlice(blockBuilder, getSliceForRange("[5,6)"));
        TSRANGE_MILLIS.writeSlice(blockBuilder, getSliceForRange("[5,7)"));
        TSRANGE_MILLIS.writeSlice(blockBuilder, getSliceForRange("[5,8)"));
        TSRANGE_MILLIS.writeSlice(blockBuilder, getSliceForRange("[5,9)"));
        return blockBuilder.build();
    }

    private static Slice getSliceForRange(String rangeString)
    {
        return tSRangeToSlice(TSRange.fromString(rangeString));
    }

    public static Slice tSRangeToSlice(TSRange tsRange)
    {
        Slice slice = Slices.allocate(RANGE_LENGTH);
        slice.setLong(0, tsRange.isLowerClosed() ? tsRange.getLower() : -tsRange.getLower());
        slice.setLong(SIZE_OF_LONG, tsRange.isUpperClosed() ? tsRange.getUpper() : -tsRange.getUpper());
        return slice;
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        Slice slice = (Slice) value;
        final long lower = slice.getLong(0);
        final long upper = slice.getLong(SIZE_OF_LONG);
        return tSRangeToSlice(TSRange.createTSRange(abs(lower) + 1, abs(upper) + 1, lower >= 0, upper >= 0));
    }

    @Test
    public void testDisplayName()
    {
        assertEquals(TSRANGE_MILLIS.getDisplayName(), "tsrange(3)");
    }

    @Override
    protected Object getNonNullValue()
    {
        return getSliceForRange("[1,2)");
    }
}
