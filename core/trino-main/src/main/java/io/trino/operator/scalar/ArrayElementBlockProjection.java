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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.DictionaryBlock;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

final class ArrayElementBlockProjection
{
    private ArrayElementBlockProjection() {}

    public static Block project(Block arrayColumn, Block indexes, Selection selection)
    {
        ColumnarArray arrays = ColumnarArray.toColumnarArray(arrayColumn);
        Block elements = arrays.getElementsBlock();
        int[] ids = new int[arrays.getPositionCount()];
        boolean requiresSyntheticNull = false;

        for (int position = 0; position < ids.length; position++) {
            if (arrays.isNull(position) || (indexes != null && indexes.isNull(position))) {
                ids[position] = -1;
                requiresSyntheticNull = true;
                continue;
            }

            int length = arrays.getLength(position);
            long index = switch (selection) {
                case FIRST -> 1;
                case LAST -> -1;
                case INDEX, SUBSCRIPT -> BIGINT.getLong(indexes, position);
            };
            int elementPosition = elementPosition(length, index, selection);
            if (elementPosition < 0) {
                ids[position] = -1;
                requiresSyntheticNull = true;
            }
            else {
                ids[position] = arrays.getOffset(position) + elementPosition;
            }
        }

        if (!requiresSyntheticNull) {
            return DictionaryBlock.create(ids.length, elements, ids);
        }

        int nullIndex = findNull(elements);
        Block dictionary = elements;
        if (nullIndex < 0) {
            nullIndex = elements.getPositionCount();
            dictionary = elements.copyWithAppendedNull();
        }
        for (int position = 0; position < ids.length; position++) {
            if (ids[position] < 0) {
                ids[position] = nullIndex;
            }
        }
        return DictionaryBlock.create(ids.length, dictionary, ids);
    }

    private static int findNull(Block block)
    {
        if (!block.mayHaveNull()) {
            return -1;
        }
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                return position;
            }
        }
        return -1;
    }

    private static int elementPosition(int length, long index, Selection selection)
    {
        if (index == 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SQL array indices start at 1");
        }
        if (selection == Selection.SUBSCRIPT) {
            if (index < 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Array subscript is negative: " + index);
            }
            if (index > length) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, format("Array subscript must be less than or equal to array length: %s > %s", index, length));
            }
            return toIntExact(index - 1);
        }
        if (Math.abs(index) > length) {
            return -1;
        }
        if (index > 0) {
            return toIntExact(index - 1);
        }
        return toIntExact(length + index);
    }

    enum Selection
    {
        FIRST,
        LAST,
        INDEX,
        SUBSCRIPT,
    }
}
