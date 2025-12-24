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

import io.trino.spi.variant.Variant;

import java.util.ArrayList;
import java.util.List;

final class TestVariantBlockBuilder
        extends AbstractTestBlockBuilder<Variant>
{
    @Override
    protected BlockBuilder createBlockBuilder()
    {
        return new VariantBlockBuilder(null, 1);
    }

    @Override
    protected List<Variant> getTestValues()
    {
        return List.of(Variant.ofBoolean(true), Variant.ofByte((byte) 90), Variant.ofInt(91), Variant.ofDouble(92.12), Variant.ofString("ninty three"));
    }

    @Override
    protected Variant getUnusedTestValue()
    {
        return Variant.ofString("unused value");
    }

    @Override
    protected ValueBlock blockFromValues(Iterable<Variant> values)
    {
        VariantBlockBuilder blockBuilder = new VariantBlockBuilder(null, 1);
        for (Variant value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                blockBuilder.writeEntry(value);
            }
        }
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected List<Variant> blockToValues(ValueBlock valueBlock)
    {
        VariantBlock variantBlock = (VariantBlock) valueBlock;
        List<Variant> actualValues = new ArrayList<>(variantBlock.getPositionCount());
        for (int i = 0; i < variantBlock.getPositionCount(); i++) {
            if (variantBlock.isNull(i)) {
                actualValues.add(null);
            }
            else {
                actualValues.add(variantBlock.getVariant(i));
            }
        }
        return actualValues;
    }
}
