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

public class TestFixed12BlockBuilder
        extends AbstractTestBlockBuilder<TestFixed12BlockBuilder.Fixed12>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new Fixed12BlockBuilder(null, 1);
    }

    @Override
    protected List<Fixed12> getTestValues()
    {
        return List.of(new Fixed12(90, 10), new Fixed12(91, 11), new Fixed12(92, 12), new Fixed12(93, 13), new Fixed12(94, 14));
    }

    @Override
    protected Fixed12 getUnusedTestValue()
    {
        return new Fixed12(-1, -2);
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Fixed12> values)
    {
        Fixed12BlockBuilder blockBuilder = new Fixed12BlockBuilder(null, 1);
        for (Fixed12 value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeFixed12(value.first(), value.second());
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Fixed12> blockToValues(ValueBlock valueBlock)
    {
        Fixed12Block fixed12ArrayBlock = (Fixed12Block) valueBlock;
        List<Fixed12> actualValues = new ArrayList<>(fixed12ArrayBlock.getPositionCount());
        for (int i = 0; i < fixed12ArrayBlock.getPositionCount(); i++) {
            if (fixed12ArrayBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(new Fixed12(fixed12ArrayBlock.getFixed12First(i), fixed12ArrayBlock.getFixed12Second(i)));
            }
        }
        return actualValues;
    }

    public record Fixed12(long first, int second) {}
}
