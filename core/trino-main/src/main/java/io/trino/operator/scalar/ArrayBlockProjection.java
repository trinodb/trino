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
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.function.SelectedPositions;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.type.BigintType.BIGINT;

final class ArrayBlockProjection
{
    private ArrayBlockProjection() {}

    public static Block project(SelectedPositions selectedPositions, Block arrayColumn, Block firstArgument, Block secondArgument, RegionFunction regionFunction)
    {
        // Read the arrays under a dictionary or a run, so only the selected arrays are resolved
        ColumnarArray arrays = ColumnarArray.toColumnarArray(arrayColumn.getUnderlyingValueBlock());
        int positionCount = selectedPositions.size();
        int[] offsets = new int[positionCount + 1];
        int[] sourceOffsets = new int[positionCount];
        long[] valueIsValid = arrays.mayHaveNull() || firstArgument.mayHaveNull() || (secondArgument != null && secondArgument.mayHaveNull())
                ? allocateWords(positionCount, false)
                : null;

        // Read constant arguments once, so only the arrays are read at each position
        long firstConstant = 0;
        Block firstColumn = firstArgument;
        if (firstArgument instanceof RunLengthEncodedBlock run && !run.isNull(0)) {
            firstConstant = BIGINT.getLong(run, 0);
            firstColumn = null;
        }
        long secondConstant = 0;
        Block secondColumn = secondArgument;
        if (secondArgument instanceof RunLengthEncodedBlock run && !run.isNull(0)) {
            secondConstant = BIGINT.getLong(run, 0);
            secondColumn = null;
        }

        for (int index = 0; index < positionCount; index++) {
            int position = selectedPositions.position(index);
            int arrayPosition = arrayColumn.getUnderlyingValuePosition(position);
            if (arrays.isNull(arrayPosition) || (firstColumn != null && firstColumn.isNull(position)) || (secondColumn != null && secondColumn.isNull(position))) {
                offsets[index + 1] = offsets[index];
                continue;
            }

            long first = firstColumn == null ? firstConstant : BIGINT.getLong(firstColumn, position);
            long second = secondColumn == null ? secondConstant : BIGINT.getLong(secondColumn, position);
            Region region = regionFunction.apply(arrays.getLength(arrayPosition), first, second);
            sourceOffsets[index] = arrays.getOffset(arrayPosition) + region.offset();
            offsets[index + 1] = offsets[index] + region.length();
            if (valueIsValid != null) {
                set(valueIsValid, 0, index);
            }
        }

        int[] ids = new int[offsets[positionCount]];
        for (int index = 0; index < positionCount; index++) {
            int outputOffset = offsets[index];
            int length = offsets[index + 1] - outputOffset;
            for (int element = 0; element < length; element++) {
                ids[outputOffset + element] = sourceOffsets[index] + element;
            }
        }

        Block elements = DictionaryBlock.create(ids.length, arrays.getElementsBlock(), ids);
        return ArrayBlock.fromElementBlock(positionCount, Optional.ofNullable(valueIsValid), offsets, elements);
    }

    @FunctionalInterface
    interface RegionFunction
    {
        Region apply(int arrayLength, long firstArgument, long secondArgument);
    }

    record Region(int offset, int length) {}
}
