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

import io.trino.spi.type.Int128;

import java.util.ArrayList;
import java.util.List;

public class TestInt128ArrayBlockBuilder
        extends AbstractTestBlockBuilder<Int128>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new Int128ArrayBlockBuilder(null, 1);
    }

    @Override
    protected List<Int128> getTestValues()
    {
        return List.of(Int128.valueOf(90, 10), Int128.valueOf(91, 11), Int128.valueOf(92, 12), Int128.valueOf(93, 13), Int128.valueOf(94, 14));
    }

    @Override
    protected Int128 getUnusedTestValue()
    {
        return Int128.valueOf(-1, -2);
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Int128> values)
    {
        Int128ArrayBlockBuilder blockBuilder = new Int128ArrayBlockBuilder(null, 1);
        for (Int128 value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeInt128(value.getHigh(), value.getLow());
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Int128> blockToValues(ValueBlock valueBlock)
    {
        Int128ArrayBlock int128ArrayBlock = (Int128ArrayBlock) valueBlock;
        List<Int128> actualValues = new ArrayList<>(int128ArrayBlock.getPositionCount());
        for (int i = 0; i < int128ArrayBlock.getPositionCount(); i++) {
            if (int128ArrayBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(int128ArrayBlock.getInt128(i));
            }
        }
        return actualValues;
    }
}
