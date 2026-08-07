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

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;
import static io.trino.spi.type.BigintType.BIGINT;

final class ArrayBlockProjection
{
    private ArrayBlockProjection() {}

    public static Block project(Block arrayColumn, Block firstArgument, Block secondArgument, RegionFunction regionFunction)
    {
        ColumnarArray arrays = ColumnarArray.toColumnarArray(arrayColumn);
        int positionCount = arrays.getPositionCount();
        int[] offsets = new int[positionCount + 1];
        int[] sourceOffsets = new int[positionCount];
        long[] valueIsValid = arrays.mayHaveNull() || firstArgument.mayHaveNull() || (secondArgument != null && secondArgument.mayHaveNull())
                ? allocateWords(positionCount, false)
                : null;

        for (int position = 0; position < positionCount; position++) {
            if (arrays.isNull(position) || firstArgument.isNull(position) || (secondArgument != null && secondArgument.isNull(position))) {
                offsets[position + 1] = offsets[position];
                continue;
            }

            long first = BIGINT.getLong(firstArgument, position);
            long second = secondArgument == null ? 0 : BIGINT.getLong(secondArgument, position);
            Region region = regionFunction.apply(arrays.getLength(position), first, second);
            sourceOffsets[position] = arrays.getOffset(position) + region.offset();
            offsets[position + 1] = offsets[position] + region.length();
            if (valueIsValid != null) {
                set(valueIsValid, 0, position);
            }
        }

        int[] ids = new int[offsets[positionCount]];
        for (int position = 0; position < positionCount; position++) {
            int outputOffset = offsets[position];
            int length = offsets[position + 1] - outputOffset;
            for (int element = 0; element < length; element++) {
                ids[outputOffset + element] = sourceOffsets[position] + element;
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
