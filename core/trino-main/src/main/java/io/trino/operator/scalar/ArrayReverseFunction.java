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
package io.trino.operator.scalar;

import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.function.ColumnarScalarImplementation;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;

@ScalarFunction(value = "reverse", neverFails = true)
@Description("Returns an array which has the reversed order of the given array.")
public final class ArrayReverseFunction
{
    private ArrayReverseFunction() {}

    @ColumnarScalarImplementation
    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block reverseColumnar(@SqlType("array(E)") Block arrayColumn)
    {
        ColumnarArray arrays = ColumnarArray.toColumnarArray(arrayColumn);
        int[] ids = new int[arrays.getElementsBlock().getPositionCount()];
        int nextId = 0;
        for (int position = 0; position < arrays.getPositionCount(); position++) {
            int offset = arrays.getOffset(position);
            int length = arrays.getLength(position);
            for (int element = length - 1; element >= 0; element--) {
                ids[nextId++] = offset + element;
            }
        }

        Block elements = DictionaryBlock.create(ids.length, arrays.getElementsBlock(), ids);
        return replaceElements(arrayColumn, arrays, elements);
    }

    private static Block replaceElements(Block arrayColumn, ColumnarArray arrays, Block elements)
    {
        if (arrayColumn instanceof ArrayBlock arrayBlock) {
            return arrayBlock.createProjection(elements);
        }

        int positionCount = arrays.getPositionCount();
        int[] offsets = new int[positionCount + 1];
        long[] valueIsValid = arrays.mayHaveNull() ? allocateWords(positionCount, false) : null;
        for (int position = 0; position < positionCount; position++) {
            offsets[position + 1] = offsets[position] + arrays.getLength(position);
            if (valueIsValid != null && !arrays.isNull(position)) {
                set(valueIsValid, 0, position);
            }
        }
        return ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(valueIsValid), offsets, elements);
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block reverse(@SqlType("array(E)") Block block)
    {
        int arrayLength = block.getPositionCount();

        if (arrayLength < 2) {
            return block;
        }

        int[] positions = new int[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            positions[i] = arrayLength - i - 1;
        }
        return block.copyPositions(positions, 0, arrayLength);
    }
}
