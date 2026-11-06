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
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Bitmap;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.SelectedPositions;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

final class ArrayElementBlockProjection
{
    private ArrayElementBlockProjection() {}

    public static Block project(SelectedPositions selectedPositions, Block arrayColumn, Block indexes, Selection selection)
    {
        // Read the arrays under a dictionary or a run, so only the selected arrays are resolved
        ColumnarArray arrays = ColumnarArray.toColumnarArray(arrayColumn.getUnderlyingValueBlock());
        Block elements = arrays.getElementsBlock();
        int[] ids = new int[selectedPositions.size()];
        int missingElementCount = 0;

        // Read a constant index once, so only the arrays are read at each position
        long constantIndex = 0;
        Block indexColumn = indexes;
        LongArrayBlock indexValues = null;
        if (indexes instanceof RunLengthEncodedBlock run && !run.isNull(0)) {
            constantIndex = BIGINT.getLong(run, 0);
            indexColumn = null;
        }
        else if (indexes != null) {
            indexValues = (LongArrayBlock) indexes.getUnderlyingValueBlock();
        }

        for (int selectedIndex = 0; selectedIndex < ids.length; selectedIndex++) {
            int position = selectedPositions.position(selectedIndex);
            int arrayPosition = arrayColumn.getUnderlyingValuePosition(position);
            if (arrays.isNull(arrayPosition) || (indexColumn != null && indexColumn.isNull(position))) {
                ids[selectedIndex] = -1;
                missingElementCount++;
                continue;
            }

            int length = arrays.getLength(arrayPosition);
            // The switch stays in the loop: hoisting the first and last indexes out of it measured 14% to 20% slower
            long index = switch (selection) {
                case FIRST -> 1;
                case LAST -> -1;
                case INDEX, SUBSCRIPT -> indexColumn == null ? constantIndex : indexValues.getLong(indexColumn.getUnderlyingValuePosition(position));
            };
            int elementPosition = elementPosition(length, index, selection);
            if (elementPosition < 0) {
                ids[selectedIndex] = -1;
                missingElementCount++;
            }
            else {
                ids[selectedIndex] = arrays.getOffset(arrayPosition) + elementPosition;
            }
        }

        return selectElements(elements, ids, missingElementCount);
    }

    /// Returns the elements at the ids, where a negative id selects a null. The ids are modified.
    static Block selectElements(Block elements, int[] ids, int missingElementCount)
    {
        if (elements instanceof DictionaryBlock dictionaryElements) {
            // Select the values under the dictionary directly, so the missing elements are resolved against the
            // values instead of copies of the dictionary, and the result needs no second ids array
            int[] rawIds = dictionaryElements.getRawIds();
            int rawIdsOffset = dictionaryElements.getRawIdsOffset();
            for (int index = 0; index < ids.length; index++) {
                if (ids[index] >= 0) {
                    ids[index] = rawIds[rawIdsOffset + ids[index]];
                }
            }
            elements = dictionaryElements.getDictionary();
        }

        if (missingElementCount == 0) {
            return DictionaryBlock.create(ids.length, elements, ids);
        }

        // Point the missing elements at a null element, when there is one, so nothing is copied
        int nullElement = firstNullElement(elements);
        if (nullElement >= 0) {
            for (int index = 0; index < ids.length; index++) {
                if (ids[index] < 0) {
                    ids[index] = nullElement;
                }
            }
            return DictionaryBlock.create(ids.length, elements, ids);
        }

        // Point the missing elements at a null appended to the range of the selected elements. Appending a null to
        // nested and variable width elements copies only their offsets, while copying an element copies its values,
        // so copy the selected elements only when their range is much larger. Appending a null to fixed width
        // elements copies their values, so the range of those elements must be closer to their count.
        int selectedElementCount = ids.length - missingElementCount;
        boolean appendCopiesValues = !(elements instanceof ArrayBlock || elements instanceof MapBlock || elements instanceof VariableWidthBlock);
        long rangeLimit = (long) selectedElementCount * (appendCopiesValues ? 16 : 64);
        int start = 0;
        int length = elements.getPositionCount();
        // The range is only needed when all the elements are too many to append a null to them
        if (length >= rangeLimit) {
            int minElement = Integer.MAX_VALUE;
            int maxElement = -1;
            for (int id : ids) {
                if (id >= 0) {
                    minElement = Math.min(minElement, id);
                    maxElement = Math.max(maxElement, id);
                }
            }
            if (selectedElementCount > 0 && maxElement - minElement >= rangeLimit) {
                return copySelectedElements(elements, ids, selectedElementCount);
            }
            start = selectedElementCount == 0 ? 0 : minElement;
            length = selectedElementCount == 0 ? 0 : maxElement - minElement + 1;
        }
        for (int index = 0; index < ids.length; index++) {
            ids[index] = ids[index] < 0 ? length : ids[index] - start;
        }
        Block range = length == elements.getPositionCount() ? elements : elements.getRegion(start, length);
        return DictionaryBlock.create(ids.length, range.copyWithAppendedNull(), ids);
    }

    private static int firstNullElement(Block elements)
    {
        if (!(elements instanceof ValueBlock valueBlock)) {
            return -1;
        }
        Optional<Bitmap> validity = valueBlock.getValidityBitmap();
        if (validity.isEmpty()) {
            return -1;
        }
        Bitmap bitmap = validity.get();
        long[] words = bitmap.getRawWords();
        int bitOffset = bitmap.getRawBitOffset();
        int bitCount = bitmap.getBitCount();
        for (int position = 0; position < bitCount; position += Long.SIZE) {
            int remaining = Math.min(Long.SIZE, bitCount - position);
            // The bits past the remaining ones are clear, so the first unset bit is only a null when it is among them
            int firstUnset = Long.numberOfTrailingZeros(~Bitmap.getBits(words, bitOffset, position, remaining));
            if (firstUnset < remaining) {
                return position + firstUnset;
            }
        }
        return -1;
    }

    private static Block copySelectedElements(Block elements, int[] ids, int selectedElementCount)
    {
        int[] selectedElements = new int[selectedElementCount];
        int nextElement = 0;
        for (int index = 0; index < ids.length; index++) {
            if (ids[index] >= 0) {
                selectedElements[nextElement] = ids[index];
                ids[index] = nextElement;
                nextElement++;
            }
            else {
                ids[index] = selectedElementCount;
            }
        }
        Block dictionary = elements.copyPositions(selectedElements, 0, selectedElementCount).copyWithAppendedNull();
        return DictionaryBlock.create(ids.length, dictionary, ids);
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
