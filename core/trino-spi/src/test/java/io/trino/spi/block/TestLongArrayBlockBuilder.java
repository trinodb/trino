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

public class TestLongArrayBlockBuilder
        extends AbstractTestBlockBuilder<Long>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new LongArrayBlockBuilder(null, 1);
    }

    @Override
    protected List<Long> getTestValues()
    {
        return List.of(10L, 11L, 12L, 13L, 14L);
    }

    @Override
    protected Long getUnusedTestValue()
    {
        return -1L;
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Long> values)
    {
        LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, 1);
        for (Long value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeLong(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Long> blockToValues(ValueBlock valueBlock)
    {
        LongArrayBlock longArrayBlock = (LongArrayBlock) valueBlock;
        List<Long> actualValues = new ArrayList<>(longArrayBlock.getPositionCount());
        for (int i = 0; i < longArrayBlock.getPositionCount(); i++) {
            if (longArrayBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(longArrayBlock.getLong(i));
            }
        }
        return actualValues;
    }
}
