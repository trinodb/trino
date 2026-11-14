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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.SelectedPositions;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static java.lang.Math.toIntExact;

final class ConcatBlockProjection
{
    private ConcatBlockProjection() {}

    public static Block project(SelectedPositions selectedPositions, SourcePage arguments, int maxOutputLength)
    {
        // Each argument is read a column at a time, so the loops over a flat, dictionary, or constant argument stay simple
        int positionCount = selectedPositions.size();
        int arity = arguments.getChannelCount();
        long[] lengths = new long[positionCount];
        long[] valueIsValid = null;
        for (int argument = 0; argument < arity; argument++) {
            Block column = arguments.getBlock(argument);
            if (column instanceof RunLengthEncodedBlock run) {
                if (run.isNull(0)) {
                    return RunLengthEncodedBlock.create(run.getValue(), positionCount);
                }
                int length = ((VariableWidthBlock) run.getValue()).getSliceLength(0);
                for (int index = 0; index < positionCount; index++) {
                    lengths[index] += length;
                }
                continue;
            }

            VariableWidthBlock values = (VariableWidthBlock) column.getUnderlyingValueBlock();
            int[] offsets = values.getRawOffsets();
            int arrayBase = values.getRawArrayBase();
            DictionaryBlock dictionary = column instanceof DictionaryBlock dictionaryBlock ? dictionaryBlock : null;
            int[] ids = dictionary == null ? null : dictionary.getRawIds();
            int idsOffset = dictionary == null ? 0 : dictionary.getRawIdsOffset();
            for (int index = 0; index < positionCount; index++) {
                int position = selectedPositions.position(index);
                int valuePosition = arrayBase + (ids == null ? position : ids[idsOffset + position]);
                lengths[index] += offsets[valuePosition + 1] - offsets[valuePosition];
            }

            long[] argumentIsValid = values.getRawValueIsValid();
            if (argumentIsValid != null) {
                if (valueIsValid == null) {
                    valueIsValid = allocateWords(positionCount, true);
                }
                for (int index = 0; index < positionCount; index++) {
                    int position = selectedPositions.position(index);
                    if (!isSet(argumentIsValid, arrayBase, ids == null ? position : ids[idsOffset + position])) {
                        clear(valueIsValid, 0, index);
                    }
                }
            }
        }

        // A null argument makes the result null before its length is validated
        int[] resultOffsets = new int[positionCount + 1];
        long totalLength = 0;
        for (int index = 0; index < positionCount; index++) {
            if (valueIsValid != null && !isSet(valueIsValid, 0, index)) {
                lengths[index] = 0;
            }
            else if (lengths[index] > maxOutputLength) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Concatenated string is too large");
            }
            totalLength += lengths[index];
            resultOffsets[index + 1] = toIntExact(totalLength);
        }

        byte[] bytes = new byte[resultOffsets[positionCount]];
        int[] outputOffsets = Arrays.copyOf(resultOffsets, positionCount);
        for (int argument = 0; argument < arity; argument++) {
            Block column = arguments.getBlock(argument);
            if (column instanceof RunLengthEncodedBlock run) {
                Slice value = ((VariableWidthBlock) run.getValue()).getSlice(0);
                for (int index = 0; index < positionCount; index++) {
                    if (lengths[index] != 0) {
                        value.getBytes(0, bytes, outputOffsets[index], value.length());
                        outputOffsets[index] += value.length();
                    }
                }
                continue;
            }

            VariableWidthBlock values = (VariableWidthBlock) column.getUnderlyingValueBlock();
            int[] offsets = values.getRawOffsets();
            int arrayBase = values.getRawArrayBase();
            byte[] valueBytes = values.getRawSlice().byteArray();
            int valueBytesOffset = values.getRawSlice().byteArrayOffset();
            DictionaryBlock dictionary = column instanceof DictionaryBlock dictionaryBlock ? dictionaryBlock : null;
            int[] ids = dictionary == null ? null : dictionary.getRawIds();
            int idsOffset = dictionary == null ? 0 : dictionary.getRawIdsOffset();
            for (int index = 0; index < positionCount; index++) {
                if (lengths[index] == 0) {
                    continue;
                }
                int position = selectedPositions.position(index);
                int valuePosition = arrayBase + (ids == null ? position : ids[idsOffset + position]);
                int start = offsets[valuePosition];
                int length = offsets[valuePosition + 1] - start;
                System.arraycopy(valueBytes, valueBytesOffset + start, bytes, outputOffsets[index], length);
                outputOffsets[index] += length;
            }
        }

        if (valueIsValid != null && !hasUnsetBit(valueIsValid, 0, positionCount)) {
            valueIsValid = null;
        }
        return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(bytes), resultOffsets, Optional.ofNullable(valueIsValid));
    }
}
