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
import io.trino.parquet.reader.flat.NullsDecoder;

/**
 * Decoder for RLE encoded values of BOOLEAN primitive type
 * <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3">
 * Run Length Encoding / Bit-Packing Hybrid (RLE)
 * </a>
 */
public final class RleBitPackingHybridBooleanDecoder
        implements ValueDecoder<byte[]>
{
    private final NullsDecoder decoder = new NullsDecoder();

    @Override
    public void init(SimpleSliceInputStream input)
    {
        // First int is size in bytes which is not needed here
        input.skip(Integer.BYTES);
        this.decoder.init(input.asSlice());
    }

    @Override
    public void read(byte[] values, int offset, int length)
    {
        boolean[] buffer = new boolean[length];
        decoder.readNext(buffer, 0, length);
        for (int i = 0; i < length; i++) {
            // NullsDecoder returns false for 1 (non-null) and true for 0 (null)
            values[offset + i] = buffer[i] ? (byte) 0 : (byte) 1;
        }
    }

    @Override
    public void skip(int n)
    {
        decoder.skip(n);
    }
}
