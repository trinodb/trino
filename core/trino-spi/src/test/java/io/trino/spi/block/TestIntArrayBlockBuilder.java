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

public class TestIntArrayBlockBuilder
        extends AbstractTestBlockBuilder<Integer>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new IntArrayBlockBuilder(null, 1);
    }

    @Override
    protected List<Integer> getTestValues()
    {
        return List.of(10, 11, 12, 13, 14);
    }

    @Override
    protected Integer getUnusedTestValue()
    {
        return -1;
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Integer> values)
    {
        IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, 1);
        for (Integer value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeInt(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Integer> blockToValues(ValueBlock valueBlock)
    {
        IntArrayBlock intArrayBlock = (IntArrayBlock) valueBlock;
        List<Integer> actualValues = new ArrayList<>(intArrayBlock.getPositionCount());
        for (int i = 0; i < intArrayBlock.getPositionCount(); i++) {
            if (intArrayBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(intArrayBlock.getInt(i));
            }
        }
        return actualValues;
    }
}
