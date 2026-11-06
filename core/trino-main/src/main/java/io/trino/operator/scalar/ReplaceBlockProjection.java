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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.SelectedPositions;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.type.VarcharType.VARCHAR;

final class ReplaceBlockProjection
{
    private ReplaceBlockProjection() {}

    /// Replaces the search string in each selected value. The replacements block is null when the search string is
    /// removed, which differs from a block of null replacements, as a null replacement makes the result null.
    public static Block project(SelectedPositions selectedPositions, Block column, Block searches, Block replacements)
    {
        // Read the values under a dictionary or a run, so only the selected values are resolved
        VariableWidthBlock values = (VariableWidthBlock) column.getUnderlyingValueBlock();
        int[] offsets = values.getRawOffsets();
        int arrayBase = values.getRawArrayBase();
        long[] valueIsValid = values.getRawValueIsValid();
        Slice slice = values.getRawSlice();
        byte[] bytes = slice.byteArray();
        int bytesOffset = slice.byteArrayOffset();

        // Read a constant search and replacement once, so only the values are read at each position
        Slice constantSearch = searches instanceof RunLengthEncodedBlock run && !run.isNull(0) ? VARCHAR.getSlice(run, 0) : null;
        Slice constantReplacement = replacements == null ? Slices.EMPTY_SLICE : null;
        if (replacements instanceof RunLengthEncodedBlock run && !run.isNull(0)) {
            constantReplacement = VARCHAR.getSlice(run, 0);
        }

        // A value without the search string is its own replacement, so the first match of each value is found
        // first, and the input is returned without a copy when no selected value changes
        int positionCount = selectedPositions.size();
        int[] valuePositions = new int[positionCount];
        int[] firstMatches = new int[positionCount];
        long[] resultIsValid = valueIsValid == null ? null : allocateWords(positionCount, true);
        boolean changed = false;
        int inputLength = 0;
        for (int index = 0; index < positionCount; index++) {
            int position = selectedPositions.position(index);
            int valuePosition = arrayBase + column.getUnderlyingValuePosition(position);
            valuePositions[index] = valuePosition;
            firstMatches[index] = -1;
            if (valueIsValid != null && !isSet(valueIsValid, 0, valuePosition)) {
                clear(resultIsValid, 0, index);
                continue;
            }
            if ((constantSearch == null && searches.isNull(position)) || (constantReplacement == null && replacements.isNull(position))) {
                // A null argument changes a value to null
                if (resultIsValid == null) {
                    resultIsValid = allocateWords(positionCount, true);
                }
                clear(resultIsValid, 0, index);
                changed = true;
                continue;
            }

            int valueStart = offsets[valuePosition];
            int valueLength = offsets[valuePosition + 1] - valueStart;
            inputLength += valueLength;
            Slice search = constantSearch != null ? constantSearch : VARCHAR.getSlice(searches, position);
            int firstMatch = search.length() == 0 ? 0 : Slices.wrappedBuffer(bytes, bytesOffset + valueStart, valueLength).indexOf(search);
            firstMatches[index] = firstMatch;
            changed |= firstMatch >= 0;
        }

        if (!changed) {
            return selectedPositions.select(column);
        }

        // The values are replaced into a single buffer, continuing the search after the first match of each value
        byte[] result = new byte[inputLength];
        int[] resultOffsets = new int[positionCount + 1];
        int size = 0;
        for (int index = 0; index < positionCount; index++) {
            if (resultIsValid != null && !isSet(resultIsValid, 0, index)) {
                resultOffsets[index + 1] = size;
                continue;
            }
            int position = selectedPositions.position(index);
            int valueStart = bytesOffset + offsets[valuePositions[index]];
            int valueLength = offsets[valuePositions[index] + 1] - offsets[valuePositions[index]];
            int match = firstMatches[index];
            if (match < 0) {
                result = ensureCapacity(result, size + valueLength);
                System.arraycopy(bytes, valueStart, result, size, valueLength);
                size += valueLength;
                resultOffsets[index + 1] = size;
                continue;
            }

            Slice value = Slices.wrappedBuffer(bytes, valueStart, valueLength);
            Slice search = constantSearch != null ? constantSearch : VARCHAR.getSlice(searches, position);
            Slice replacement = constantReplacement != null ? constantReplacement : VARCHAR.getSlice(replacements, position);
            if (search.length() == 0) {
                // An empty search string inserts the replacement around every code point
                Slice replacedValue = StringFunctions.replace(value, search, replacement);
                result = ensureCapacity(result, size + replacedValue.length());
                replacedValue.getBytes(0, result, size, replacedValue.length());
                size += replacedValue.length();
                resultOffsets[index + 1] = size;
                continue;
            }

            int start = 0;
            while (match >= 0) {
                result = ensureCapacity(result, size + (match - start) + replacement.length());
                System.arraycopy(bytes, valueStart + start, result, size, match - start);
                size += match - start;
                replacement.getBytes(0, result, size, replacement.length());
                size += replacement.length();
                start = match + search.length();
                match = start < valueLength ? value.indexOf(search, start) : -1;
            }
            result = ensureCapacity(result, size + (valueLength - start));
            System.arraycopy(bytes, valueStart + start, result, size, valueLength - start);
            size += valueLength - start;
            resultOffsets[index + 1] = size;
        }

        if (resultIsValid != null && !hasUnsetBit(resultIsValid, 0, positionCount)) {
            resultIsValid = null;
        }
        return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(result, 0, size), resultOffsets, Optional.ofNullable(resultIsValid));
    }

    private static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (capacity <= buffer.length) {
            return buffer;
        }
        return Arrays.copyOf(buffer, Math.max(capacity, buffer.length + (buffer.length >> 1)));
    }
}
