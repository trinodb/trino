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
package io.trino.parquet.writer.valuewriter;

import io.airlift.slice.Slice;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.block.VariableWidthBlock;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Arrays;

import static com.google.common.base.Verify.verify;
import static org.apache.parquet.schema.PrimitiveComparator.UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR;

public class BinaryValueWriter
        extends PrimitiveValueWriter
{
    // bounds of the values written to the current page, applied to the statistics once when the page is reset
    private byte[] minimum;
    private byte[] maximum;

    public BinaryValueWriter(ValuesWriter valuesWriter, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        verify(getStatistics().comparator() == UNSIGNED_LEXICOGRAPHICAL_BINARY_COMPARATOR, "Unexpected statistics order for %s", parquetType);
    }

    @Override
    public Statistics<?> resetAndGetStatistics()
    {
        if (minimum != null) {
            getStatistics().updateStats(Binary.fromConstantByteArray(minimum));
            getStatistics().updateStats(Binary.fromConstantByteArray(maximum));
            minimum = null;
            maximum = null;
        }
        return super.resetAndGetStatistics();
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;

        Slice rawSlice = variableWidthBlock.getRawSlice();
        byte[] rawByteArray = rawSlice.byteArray();
        int rawByteArrayOffset = rawSlice.byteArrayOffset();

        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                int sliceOffset = variableWidthBlock.getRawSliceOffset(i);
                int sliceLength = variableWidthBlock.getSliceLength(i);
                valuesWriter.writeBytes(rawSlice, sliceOffset, sliceLength);
                updateStatistics(rawByteArray, rawByteArrayOffset + sliceOffset, sliceLength);
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Slice slice = ((VariableWidthBlock) block).getSlice(0);
        for (int i = 0; i < count; i++) {
            valuesWriter.writeBytes(slice);
        }
        updateStatistics(slice.byteArray(), slice.byteArrayOffset(), slice.length());
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        VariableWidthBlock variableWidthBlock = (VariableWidthBlock) block;

        Slice rawSlice = variableWidthBlock.getRawSlice();
        byte[] rawByteArray = rawSlice.byteArray();
        int rawByteArrayOffset = rawSlice.byteArrayOffset();

        boolean mayHaveNull = block.mayHaveNull();
        for (int index = 0; index < length; index++) {
            int position = positions[offset + index];
            if (!mayHaveNull || !block.isNull(position)) {
                int sliceOffset = variableWidthBlock.getRawSliceOffset(position);
                int sliceLength = variableWidthBlock.getSliceLength(position);
                valuesWriter.writeBytes(rawSlice, sliceOffset, sliceLength);
                updateStatistics(rawByteArray, rawByteArrayOffset + sliceOffset, sliceLength);
            }
        }
    }

    private void updateStatistics(byte[] bytes, int offset, int length)
    {
        boolean belowMinimum = minimum == null || Arrays.compareUnsigned(bytes, offset, offset + length, minimum, 0, minimum.length) < 0;
        if (!belowMinimum && Arrays.compareUnsigned(bytes, offset, offset + length, maximum, 0, maximum.length) <= 0) {
            return;
        }
        // the bounds outlive the block, so they get their own copy rather than a view of the block's bytes
        byte[] bound = Arrays.copyOfRange(bytes, offset, offset + length);
        if (belowMinimum) {
            minimum = bound;
        }
        if (maximum == null || !belowMinimum) {
            maximum = bound;
        }
    }
}
