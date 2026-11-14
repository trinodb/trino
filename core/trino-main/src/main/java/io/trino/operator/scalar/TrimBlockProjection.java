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
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.function.SelectedPositions;

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;

final class TrimBlockProjection
{
    private TrimBlockProjection() {}

    public static Block project(SelectedPositions selectedPositions, Block column, Trim trim)
    {
        // Read the values under a dictionary or a run, so only the selected values are resolved
        VariableWidthBlock values = (VariableWidthBlock) column.getUnderlyingValueBlock();
        int[] offsets = values.getRawOffsets();
        int arrayBase = values.getRawArrayBase();
        long[] valueIsValid = values.getRawValueIsValid();
        Slice slice = values.getRawSlice();
        byte[] bytes = slice.byteArray();
        int bytesOffset = slice.byteArrayOffset();

        // A trimmed value is a part of its value, so only the start of each trimmed value in the bytes of the values
        // is recorded first, and the trimmed values are copied to a single buffer after their total length is known
        int positionCount = selectedPositions.size();
        int[] trimmedStarts = new int[positionCount];
        int[] resultOffsets = new int[positionCount + 1];
        long[] resultIsValid = valueIsValid == null ? null : allocateWords(positionCount, false);
        boolean trimmed = false;
        for (int index = 0; index < positionCount; index++) {
            int valuePosition = arrayBase + column.getUnderlyingValuePosition(selectedPositions.position(index));
            if (valueIsValid != null) {
                if (!isSet(valueIsValid, 0, valuePosition)) {
                    resultOffsets[index + 1] = resultOffsets[index];
                    continue;
                }
                set(resultIsValid, 0, index);
            }

            int valueStart = offsets[valuePosition];
            int valueLength = offsets[valuePosition + 1] - valueStart;
            Slice value = Slices.wrappedBuffer(bytes, bytesOffset + valueStart, valueLength);
            // The switch stays in the loop, as the trim is a constant of the query
            Slice trimmedValue = switch (trim) {
                case BOTH -> SliceUtf8.trim(value);
                case LEFT -> SliceUtf8.leftTrim(value);
                case RIGHT -> SliceUtf8.rightTrim(value);
            };
            trimmed |= trimmedValue.length() != valueLength;
            trimmedStarts[index] = trimmedValue.byteArrayOffset();
            resultOffsets[index + 1] = resultOffsets[index] + trimmedValue.length();
        }

        // No value has whitespace to trim, so the input is returned without a copy
        if (!trimmed) {
            return selectedPositions.select(column);
        }

        byte[] result = new byte[resultOffsets[positionCount]];
        for (int index = 0; index < positionCount; index++) {
            int length = resultOffsets[index + 1] - resultOffsets[index];
            if (length > 0) {
                System.arraycopy(bytes, trimmedStarts[index], result, resultOffsets[index], length);
            }
        }

        if (resultIsValid != null && !hasUnsetBit(resultIsValid, 0, positionCount)) {
            resultIsValid = null;
        }
        return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(result), resultOffsets, Optional.ofNullable(resultIsValid));
    }

    enum Trim
    {
        BOTH,
        LEFT,
        RIGHT,
    }
}
