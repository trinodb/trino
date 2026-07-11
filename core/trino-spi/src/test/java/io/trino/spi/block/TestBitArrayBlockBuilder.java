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

import static org.assertj.core.api.Assertions.assertThat;

final class TestBitArrayBlockBuilder
        extends AbstractTestBlockBuilder<Boolean>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new BitArrayBlockBuilder(null, 1);
    }

    @Override
    protected List<Boolean> getTestValues()
    {
        return List.of(true, false, true, true, false);
    }

    @Override
    protected Boolean getUnusedTestValue()
    {
        return false;
    }

    @Override
    protected void verifyTestValues(List<Boolean> values)
    {
        assertThat(values)
                .hasSize(5)
                .doesNotContainNull()
                .contains(true, false);
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Boolean> values)
    {
        BitArrayBlockBuilder blockBuilder = new BitArrayBlockBuilder(null, 1);
        for (Boolean value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeBoolean(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Boolean> blockToValues(ValueBlock valueBlock)
    {
        BitArrayBlock bitArrayBlock = (BitArrayBlock) valueBlock;
        List<Boolean> actualValues = new ArrayList<>(bitArrayBlock.getPositionCount());
        for (int i = 0; i < bitArrayBlock.getPositionCount(); i++) {
            if (bitArrayBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(bitArrayBlock.getBoolean(i));
            }
        }
        return actualValues;
    }
}
