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

import java.util.ArrayList;
import java.util.List;

public class TestShortArrayBlockBuilder
        extends AbstractTestBlockBuilder<Short>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new ShortArrayBlockBuilder(null, 1);
    }

    @Override
    protected List<Short> getTestValues()
    {
        return List.of((short) 10, (short) 11, (short) 12, (short) 13, (short) 14);
    }

    @Override
    protected Short getUnusedTestValue()
    {
        return (short) -1;
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Short> values)
    {
        ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, 1);
        for (Short value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeShort(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Short> blockToValues(ValueBlock valueBlock)
    {
        ShortArrayBlock shortArrayBlock = (ShortArrayBlock) valueBlock;
        List<Short> actualValues = new ArrayList<>(shortArrayBlock.getPositionCount());
        for (int i = 0; i < shortArrayBlock.getPositionCount(); i++) {
            if (shortArrayBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(shortArrayBlock.getShort(i));
            }
        }
        return actualValues;
    }
}
