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

import java.util.Optional;

import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.clear;
import static io.trino.spi.block.Bitmap.hasUnsetBit;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

final class SplitPartBlockProjection
{
    private SplitPartBlockProjection() {}

    public static Block project(SelectedPositions selectedPositions, Block column, Block delimiters, Block indexes)
    {
        // Read the values under a dictionary or a run, so only the selected values are resolved
        VariableWidthBlock values = (VariableWidthBlock) column.getUnderlyingValueBlock();
        int[] offsets = values.getRawOffsets();
        int arrayBase = values.getRawArrayBase();
        long[] valueIsValid = values.getRawValueIsValid();
        Slice slice = values.getRawSlice();
        byte[] bytes = slice.byteArray();
        int bytesOffset = slice.byteArrayOffset();

        // Read a constant delimiter and index once, so only the values are read at each position
        Slice constantDelimiter = delimiters instanceof RunLengthEncodedBlock run && !run.isNull(0) ? VARCHAR.getSlice(run, 0) : null;
        boolean indexIsConstant = indexes instanceof RunLengthEncodedBlock run && !run.isNull(0);
        long constantIndex = indexIsConstant ? BIGINT.getLong(indexes, 0) : 0;

        // A part is a part of its value, so only the start of each part in the bytes of the values is recorded first,
        // and the parts are copied to a single buffer after their total length is known
        int positionCount = selectedPositions.size();
        int[] partStarts = new int[positionCount];
        int[] resultOffsets = new int[positionCount + 1];
        long[] resultIsValid = allocateWords(positionCount, true);
        for (int index = 0; index < positionCount; index++) {
            int position = selectedPositions.position(index);
            int valuePosition = arrayBase + column.getUnderlyingValuePosition(position);
            resultOffsets[index + 1] = resultOffsets[index];
            if ((valueIsValid != null && !isSet(valueIsValid, 0, valuePosition)) || (constantDelimiter == null && delimiters.isNull(position)) || (!indexIsConstant && indexes.isNull(position))) {
                clear(resultIsValid, 0, index);
                continue;
            }

            int valueStart = offsets[valuePosition];
            Slice value = Slices.wrappedBuffer(bytes, bytesOffset + valueStart, offsets[valuePosition + 1] - valueStart);
            Slice part = StringFunctions.splitPart(
                    value,
                    constantDelimiter != null ? constantDelimiter : VARCHAR.getSlice(delimiters, position),
                    indexIsConstant ? constantIndex : BIGINT.getLong(indexes, position));
            if (part == null) {
                clear(resultIsValid, 0, index);
                continue;
            }
            partStarts[index] = part.byteArrayOffset();
            resultOffsets[index + 1] += part.length();
        }

        byte[] result = new byte[resultOffsets[positionCount]];
        for (int index = 0; index < positionCount; index++) {
            int length = resultOffsets[index + 1] - resultOffsets[index];
            if (length > 0) {
                System.arraycopy(bytes, partStarts[index], result, resultOffsets[index], length);
            }
        }

        if (!hasUnsetBit(resultIsValid, 0, positionCount)) {
            resultIsValid = null;
        }
        return new VariableWidthBlock(positionCount, Slices.wrappedBuffer(result), resultOffsets, Optional.ofNullable(resultIsValid));
    }
}
