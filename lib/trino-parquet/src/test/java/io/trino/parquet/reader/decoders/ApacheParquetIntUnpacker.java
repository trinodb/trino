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
package io.trino.parquet.reader.decoders;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;

class ApacheParquetIntUnpacker
{
    private final BytePacker delegate;
    private final int bitWidth;

    public ApacheParquetIntUnpacker(int bitWidth)
    {
        this.bitWidth = bitWidth;
        this.delegate = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    }

    public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
    {
        int byteWidth = bitWidth * 8 / Byte.SIZE;
        Slice sliceBuffer = Slices.wrappedBuffer(new byte[byteWidth]);
        for (int i = outputOffset; i < outputOffset + length; i += 8) {
            input.readBytes(sliceBuffer, 0, byteWidth);
            int[] outputBuffer = new int[8];
            delegate.unpack8Values(sliceBuffer.toByteBuffer(), 0, outputBuffer, 0);
            System.arraycopy(outputBuffer, 0, output, i, 8);
        }
    }
}
