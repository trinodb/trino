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

import java.util.Arrays;
import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.set;

final class CaseConversionBlockProjection
{
    private CaseConversionBlockProjection() {}

    public static Block toLowerCase(SelectedPositions selectedPositions, Block column)
    {
        return convert(selectedPositions, column, true);
    }

    public static Block toUpperCase(SelectedPositions selectedPositions, Block column)
    {
        return convert(selectedPositions, column, false);
    }

    private static Block convert(SelectedPositions selectedPositions, Block column, boolean lowerCase)
    {
        // Read the values under a dictionary or a run, so only the selected values are resolved
        VariableWidthBlock values = (VariableWidthBlock) column.getUnderlyingValueBlock();
        int[] offsets = values.getRawOffsets();
        int arrayBase = values.getRawArrayBase();
        long[] valueIsValid = values.getRawValueIsValid();
        Slice slice = values.getRawSlice();
        byte[] bytes = slice.byteArray();
        int bytesOffset = slice.byteArrayOffset();
        boolean contiguous = column == values && !selectedPositions.isList();

        // Find the length of the results, whether any value has a letter to convert, and which values are not ASCII.
        // A value is not scanned past its first byte that is not ASCII, as it is converted a code point at a time.
        int positionCount = selectedPositions.size();
        int[] valuePositions = new int[positionCount];
        int[] resultOffsets = new int[positionCount + 1];
        boolean[] nonAsciiValues = new boolean[positionCount];
        int convertedLetter = lowerCase ? 'A' : 'a';
        int letters = 0;
        boolean nonAscii = false;
        for (int index = 0; index < positionCount; index++) {
            int valuePosition = arrayBase + column.getUnderlyingValuePosition(selectedPositions.position(index));
            valuePositions[index] = valuePosition;
            int start = offsets[valuePosition];
            int end = offsets[valuePosition + 1];
            resultOffsets[index + 1] = resultOffsets[index] + (end - start);
            for (int offset = bytesOffset + start; offset < bytesOffset + end; offset++) {
                int value = bytes[offset];
                if (value < 0) {
                    nonAsciiValues[index] = true;
                    nonAscii = true;
                    break;
                }
                letters |= ((value - convertedLetter) & 0xFF) < 26 ? 1 : 0;
            }
        }

        // The values are their own conversion, so the input is returned without a copy
        if (letters == 0 && !nonAscii) {
            return selectedPositions.select(column);
        }

        long[] resultIsValid = null;
        if (valueIsValid != null) {
            resultIsValid = allocateWords(positionCount, false);
            for (int index = 0; index < positionCount; index++) {
                if (isSet(valueIsValid, 0, valuePositions[index])) {
                    set(resultIsValid, 0, index);
                }
            }
            if (!hasUnsetBit(resultIsValid, 0, positionCount)) {
                resultIsValid = null;
            }
        }

        byte[] result = new byte[resultOffsets[positionCount]];
        if (!nonAscii) {
            // ASCII letters convert in place, so the values are copied and then converted in a single pass
            if (contiguous) {
                System.arraycopy(bytes, bytesOffset + offsets[valuePositions[0]], result, 0, result.length);
            }
            else {
                for (int index = 0; index < positionCount; index++) {
                    int start = offsets[valuePositions[index]];
                    System.arraycopy(bytes, bytesOffset + start, result, resultOffsets[index], resultOffsets[index + 1] - resultOffsets[index]);
                }
            }
            convertLetters(result, 0, result.length, convertedLetter);
            return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(result), resultOffsets, Optional.ofNullable(resultIsValid));
        }

        // The values that are not ASCII are converted a code point at a time, which can change their length
        int size = 0;
        for (int index = 0; index < positionCount; index++) {
            if (resultIsValid == null || isSet(resultIsValid, 0, index)) {
                int start = bytesOffset + offsets[valuePositions[index]];
                int length = offsets[valuePositions[index] + 1] - offsets[valuePositions[index]];
                if (nonAsciiValues[index]) {
                    Slice converted = lowerCase ? SliceUtf8.toLowerCase(bytes, start, length) : SliceUtf8.toUpperCase(bytes, start, length);
                    result = ensureCapacity(result, size + converted.length());
                    converted.getBytes(0, result, size, converted.length());
                    size += converted.length();
                }
                else {
                    result = ensureCapacity(result, size + length);
                    System.arraycopy(bytes, start, result, size, length);
                    if (letters != 0) {
                        convertLetters(result, size, size + length, convertedLetter);
                    }
                    size += length;
                }
            }
            resultOffsets[index + 1] = size;
        }
        return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(result, 0, size), resultOffsets, Optional.ofNullable(resultIsValid));
    }

    private static void convertLetters(byte[] bytes, int start, int end, int convertedLetter)
    {
        for (int offset = start; offset < end; offset++) {
            int value = bytes[offset];
            if (((value - convertedLetter) & 0xFF) < 26) {
                bytes[offset] = (byte) (value ^ 0x20);
            }
        }
    }

    private static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (capacity <= buffer.length) {
            return buffer;
        }
        return Arrays.copyOf(buffer, Math.max(capacity, buffer.length + (buffer.length >> 1)));
    }
}
