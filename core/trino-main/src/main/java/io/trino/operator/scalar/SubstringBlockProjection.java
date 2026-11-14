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
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.SelectedPositions;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.type.BigintType.BIGINT;

final class SubstringBlockProjection
{
    private SubstringBlockProjection() {}

    /// Selects the substring of each selected value. The length is null when the substring extends to the end of the value.
    public static Block project(SelectedPositions selectedPositions, Block column, Block start, Block length)
    {
        // Read the values under a dictionary or a run, so only the selected values are resolved
        VariableWidthBlock values = (VariableWidthBlock) column.getUnderlyingValueBlock();
        int[] offsets = values.getRawOffsets();
        int arrayBase = values.getRawArrayBase();
        long[] valueIsValid = values.getRawValueIsValid();
        Slice slice = values.getRawSlice();
        byte[] bytes = slice.byteArray();
        int bytesOffset = slice.byteArrayOffset();

        // A substring is a part of its value, so only the start of each substring in the bytes of the values is
        // recorded first, and the substrings are copied to a single buffer after their total length is known
        int positionCount = selectedPositions.size();
        int[] substringStarts = new int[positionCount];
        int[] resultOffsets = new int[positionCount + 1];
        long[] resultIsValid = null;
        if (valueIsValid != null || start.mayHaveNull() || (length != null && length.mayHaveNull())) {
            resultIsValid = allocateWords(positionCount, true);
        }
        for (int index = 0; index < positionCount; index++) {
            int position = selectedPositions.position(index);
            int valuePosition = arrayBase + column.getUnderlyingValuePosition(position);
            if ((valueIsValid != null && !isSet(valueIsValid, 0, valuePosition)) || start.isNull(position) || (length != null && length.isNull(position))) {
                clear(resultIsValid, 0, index);
                resultOffsets[index + 1] = resultOffsets[index];
                continue;
            }

            int valueStart = offsets[valuePosition];
            Slice value = Slices.wrappedBuffer(bytes, bytesOffset + valueStart, offsets[valuePosition + 1] - valueStart);
            Slice substring = length == null
                    ? StringFunctions.substring(value, BIGINT.getLong(start, position))
                    : StringFunctions.substring(value, BIGINT.getLong(start, position), BIGINT.getLong(length, position));
            substringStarts[index] = substring.byteArrayOffset();
            resultOffsets[index + 1] = resultOffsets[index] + substring.length();
        }

        byte[] result = new byte[resultOffsets[positionCount]];
        for (int index = 0; index < positionCount; index++) {
            int substringLength = resultOffsets[index + 1] - resultOffsets[index];
            if (substringLength > 0) {
                System.arraycopy(bytes, substringStarts[index], result, resultOffsets[index], substringLength);
            }
        }

        if (resultIsValid != null && !hasUnsetBit(resultIsValid, 0, positionCount)) {
            resultIsValid = null;
        }
        return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(result), resultOffsets, Optional.ofNullable(resultIsValid));
    }
}
