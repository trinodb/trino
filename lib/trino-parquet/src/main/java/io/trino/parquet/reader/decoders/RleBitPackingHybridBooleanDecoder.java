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
import io.trino.parquet.reader.flat.FlatDefinitionLevelDecoder;

import static io.trino.parquet.reader.flat.NullsDecoders.createNullsDecoder;
import static io.trino.spi.block.Bitmap.isSet;
import static io.trino.spi.block.Bitmap.wordsForBits;
import static java.util.Arrays.fill;

/**
 * Decoder for RLE encoded values of BOOLEAN primitive type
 * <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3">
 * Run Length Encoding / Bit-Packing Hybrid (RLE)
 * </a>
 */
public final class RleBitPackingHybridBooleanDecoder
        implements ValueDecoder<byte[]>
{
    private final FlatDefinitionLevelDecoder decoder;

    public RleBitPackingHybridBooleanDecoder(boolean vectorizedDecodingEnabled)
    {
        this.decoder = createNullsDecoder(vectorizedDecodingEnabled);
    }

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
        long[] buffer = new long[wordsForBits(length)];
        int trueCount = decoder.readNext(buffer, 0, length);
        if (trueCount == length) {
            fill(values, offset, offset + length, (byte) 1);
            return;
        }
        if (trueCount == 0) {
            fill(values, offset, offset + length, (byte) 0);
            return;
        }
        for (int i = 0; i < length; i++) {
            values[offset + i] = isSet(buffer, 0, i) ? (byte) 1 : 0;
        }
    }

    @Override
    public void skip(int n)
    {
        decoder.skip(n);
    }
}
