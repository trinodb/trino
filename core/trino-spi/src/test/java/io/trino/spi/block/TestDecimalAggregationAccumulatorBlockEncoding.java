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
package io.trino.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.Type;

import java.util.Random;

import static io.trino.spi.type.DecimalAggregationAccumulatorType.LONG_DECIMAL_WITH_OVERFLOW_AND_LONG;

public class TestDecimalAggregationAccumulatorBlockEncoding
        extends BaseBlockEncodingTest<Slice>
{
    private static final Type TYPE = LONG_DECIMAL_WITH_OVERFLOW_AND_LONG;

    @Override
    protected Type getType()
    {
        return TYPE;
    }

    @Override
    protected void write(BlockBuilder blockBuilder, Slice value)
    {
        TYPE.writeSlice(blockBuilder, value);
    }

    @Override
    protected Slice randomValue(Random random)
    {
        return Slices.wrappedLongArray(random.nextLong(), random.nextLong(), random.nextLong(), random.nextLong());
    }
}
