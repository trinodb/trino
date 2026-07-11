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
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.function.ColumnarScalar;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SelectedPositions;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.function.SelectedPositions.positionsRange;
import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;

@ScalarFunction("flatten")
@Description("Flattens the given array")
public final class ArrayFlattenFunction
{
    private ArrayFlattenFunction() {}

    @ColumnarScalar
    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block flattenColumnar(SelectedPositions selectedPositions, @SqlType("array(array(E))") Block arrayColumn)
    {
        if (selectedPositions.isList() && !(arrayColumn instanceof ArrayBlock arrays && arrays.getRawElementBlock() instanceof ValueBlock)) {
            // Encoded arrays are expanded to every array they reference, so only the selected arrays are copied first
            Block selectedArrays = arrayColumn.copyPositions(selectedPositions.positions(), selectedPositions.offset(), selectedPositions.size());
            return flattenColumnar(positionsRange(0, selectedPositions.size()), selectedArrays);
        }

        ColumnarArray outerArrays = ColumnarArray.toColumnarArray(arrayColumn);
        ColumnarArray innerArrays = ColumnarArray.toColumnarArray(outerArrays.getElementsBlock());

        int positionCount = outerArrays.getPositionCount();
        // Gathering the selected elements is proportional to their count, while flattening every array is proportional
        // to the positions, so gather when the selected arrays are expected to have fewer elements than there are positions
        long elementCount = elementOffset(outerArrays, innerArrays, positionCount) - elementOffset(outerArrays, innerArrays, 0);
        if (selectedPositions.isList() && selectedPositions.size() * elementCount < (long) positionCount * positionCount) {
            return flattenSelected(selectedPositions, outerArrays, innerArrays);
        }

        // Both levels are contiguous and null arrays have no entries, so the elements of an outer
        // array start where the elements of its first inner array start
        int[] offsets = new int[positionCount + 1];
        for (int position = 0; position <= positionCount; position++) {
            offsets[position] = elementOffset(outerArrays, innerArrays, position);
        }
        long[] valueIsValid = null;
        if (outerArrays.mayHaveNull()) {
            valueIsValid = allocateWords(positionCount, false);
            for (int position = 0; position < positionCount; position++) {
                if (!outerArrays.isNull(position)) {
                    set(valueIsValid, 0, position);
                }
            }
        }
        return selectedPositions.select(ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(valueIsValid), offsets, innerArrays.getElementsBlock()));
    }

    private static Block flattenSelected(SelectedPositions selectedPositions, ColumnarArray outerArrays, ColumnarArray innerArrays)
    {
        int size = selectedPositions.size();
        long selectedElementCount = 0;
        for (int index = 0; index < size; index++) {
            int position = selectedPositions.position(index);
            selectedElementCount += elementOffset(outerArrays, innerArrays, position + 1) - elementOffset(outerArrays, innerArrays, position);
        }

        int[] offsets = new int[size + 1];
        int[] ids = new int[toIntExact(selectedElementCount)];
        long[] valueIsValid = outerArrays.mayHaveNull() ? allocateWords(size, false) : null;
        int nextId = 0;
        for (int index = 0; index < size; index++) {
            int position = selectedPositions.position(index);
            if (valueIsValid != null && !outerArrays.isNull(position)) {
                set(valueIsValid, 0, index);
            }
            int end = elementOffset(outerArrays, innerArrays, position + 1);
            for (int element = elementOffset(outerArrays, innerArrays, position); element < end; element++) {
                ids[nextId++] = element;
            }
            offsets[index + 1] = nextId;
        }
        Block elements = DictionaryBlock.create(ids.length, innerArrays.getElementsBlock(), ids);
        return ArrayBlock.fromElementBlock(size, Optional.ofNullable(valueIsValid), offsets, elements);
    }

    private static int elementOffset(ColumnarArray outerArrays, ColumnarArray innerArrays, int position)
    {
        return innerArrays.getOffset(outerArrays.getOffset(position));
    }

    @TypeParameter("E")
    @SqlType("array(E)")
    public static Block flatten(
            @TypeParameter("E") Type type,
            @TypeParameter("array(E)") Type arrayType,
            @SqlType("array(array(E))") Block array)
    {
        int expectedEntries = 0;
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!array.isNull(i)) {
                Block subArray = (Block) arrayType.getObject(array, i);
                expectedEntries = addExact(expectedEntries, subArray.getPositionCount());
            }
        }
        if (expectedEntries == 0) {
            return type.createBlockBuilder(null, 0).build();
        }

        BlockBuilder builder = type.createBlockBuilder(null, expectedEntries, toIntExact(array.getSizeInBytes() / expectedEntries));
        for (int i = 0; i < array.getPositionCount(); i++) {
            if (!array.isNull(i)) {
                Block subArray = (Block) arrayType.getObject(array, i);
                builder.appendBlockRange(subArray, 0, subArray.getPositionCount());
            }
        }
        return builder.build();
    }
}
