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
package io.trino.plugin.varada.storage.read.fill;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.juffer.ByteBufferInputStream;
import io.trino.plugin.varada.storage.common.StorageConstants;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

public class VarcharArrayBlockFiller
        extends ArraySliceFiller
{
    public VarcharArrayBlockFiller(StorageEngineConstants storageEngineConstants)
    {
        super(storageEngineConstants, BlockFillerType.ARRAY_VARCHAR, VarcharType.VARCHAR);
    }

    @Override
    protected Block getBlock(Slice slice, int offset, int length)
    {
        int nativeSlicePosition = offset;
        int outputPosition = 0;
        int numberOfElements = slice.getInt(nativeSlicePosition);
        nativeSlicePosition += Integer.BYTES;

        int[] outputOffsets = new int[numberOfElements + 1]; // +1 contains the total size
        boolean[] valueIsNull = new boolean[numberOfElements];
        int[] nativePositions = new int[numberOfElements];
        int[] valueLengths = new int[numberOfElements];

        for (int i = 0; i < numberOfElements; i++) {
            outputOffsets[i] = outputPosition;
            byte nullByteSignal = slice.getByte(nativeSlicePosition);
            nativeSlicePosition += Byte.BYTES;
            nativePositions[i] = nativeSlicePosition;
            if (nullByteSignal == StorageConstants.NULL_MARKER_VALUE) {
                valueLengths[i] = 0;
                valueIsNull[i] = true;
            }
            else {
                valueIsNull[i] = false;
                valueLengths[i] = slice.getInt(nativeSlicePosition);
                nativePositions[i] += Integer.BYTES;
                nativeSlicePosition += Integer.BYTES + valueLengths[i]; //update the nativeSlicePosition to look in the slice
            }
            outputPosition += valueLengths[i];
        }
        int totalSize = outputPosition;
        outputOffsets[numberOfElements] = totalSize;
        byte[] values = new byte[totalSize];
        Slice outputSlice = Slices.wrappedBuffer(values);

        for (int i = 0; i < numberOfElements; i++) {
            if (!valueIsNull[i]) {
                byte[] bytes = slice.getBytes(nativePositions[i], valueLengths[i]);
                outputSlice.setBytes(outputOffsets[i], bytes);
            }
        }
        return new VariableWidthBlock(numberOfElements, outputSlice, outputOffsets, Optional.of(valueIsNull));
    }

    @Override
    protected Block getBlock(ByteBufferInputStream byteBufferInputStream, int offset, int length)
    {
        int outputPosition = 0;
        int originalPosition = byteBufferInputStream.position();
        byteBufferInputStream.position(offset);
        int numberOfElements = byteBufferInputStream.readInt();

        int[] outputOffsets = new int[numberOfElements + 1]; // +1 contains the total size
        boolean[] valueIsNull = new boolean[numberOfElements];
        int[] nativePositions = new int[numberOfElements];
        int[] valueLengths = new int[numberOfElements];

        for (int i = 0; i < numberOfElements; i++) {
            outputOffsets[i] = outputPosition;
            byte nullByteSignal = byteBufferInputStream.readByte();
            if (nullByteSignal == StorageConstants.NULL_MARKER_VALUE) {
                valueLengths[i] = 0;
                valueIsNull[i] = true;
                nativePositions[i] = byteBufferInputStream.position();
            }
            else {
                valueIsNull[i] = false;
                valueLengths[i] = byteBufferInputStream.readInt();
                nativePositions[i] = byteBufferInputStream.position();
                byteBufferInputStream.skip(valueLengths[i]); // we mark the position of the value to be copied to slice below in the second loop, now we skip it to read the next one
            }
            outputPosition += valueLengths[i];
        }
        int totalSize = outputPosition;
        outputOffsets[numberOfElements] = totalSize;
        byte[] values = new byte[totalSize];
        Slice outputSlice = Slices.wrappedBuffer(values);

        for (int i = 0; i < numberOfElements; i++) {
            if (!valueIsNull[i]) {
                byte[] bytes = byteBufferInputStream.readBytes(nativePositions[i], valueLengths[i]);
                outputSlice.setBytes(outputOffsets[i], bytes);
            }
        }

        byteBufferInputStream.position(originalPosition);
        return new VariableWidthBlock(numberOfElements, outputSlice, outputOffsets, Optional.of(valueIsNull));
    }
}
