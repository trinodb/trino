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

import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;

import static com.google.common.base.Preconditions.checkArgument;

class ApacheParquetIntUnpacker
{
    private final BytePacker delegate;
    private final int bitWidth;
    private final int[] outputBuffer = new int[32];

    public ApacheParquetIntUnpacker(int bitWidth)
    {
        checkArgument(bitWidth >= 0 && bitWidth <= Integer.SIZE, "bitWidth %s should be in the range 0-32", bitWidth);
        this.bitWidth = bitWidth;
        this.delegate = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    }

    public void unpack(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
    {
        int byteWidth = bitWidth * 8 / Byte.SIZE;
        byte[] buffer = new byte[byteWidth];
        for (int i = outputOffset; i < outputOffset + length; i += 8) {
            input.readBytes(buffer, 0, byteWidth);
            delegate.unpack8Values(buffer, 0, output, i);
        }
    }

    public void unpackDelta(int[] output, int outputOffset, SimpleSliceInputStream input, int length)
    {
        int byteWidth = bitWidth * 32 / Byte.SIZE;
        byte[] buffer = new byte[byteWidth];
        for (int i = outputOffset; i < outputOffset + length; i += 32) {
            input.readBytes(buffer, 0, byteWidth);
            delegate.unpack32Values(buffer, 0, outputBuffer, 0);
            for (int j = 0; j < 32; j++) {
                output[i + j] += output[i + j - 1] + outputBuffer[j];
            }
        }
    }
}
