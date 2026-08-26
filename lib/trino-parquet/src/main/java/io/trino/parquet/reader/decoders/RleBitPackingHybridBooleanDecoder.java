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
import io.trino.parquet.reader.flat.BitBuffer;
import io.trino.parquet.reader.flat.FlatDefinitionLevelDecoder;

import static io.trino.parquet.reader.flat.NullsDecoders.createNullsDecoder;
import static io.trino.spi.block.Bitmap.clearBits;
import static io.trino.spi.block.Bitmap.setBits;

/**
 * Decoder for RLE encoded values of BOOLEAN primitive type
 * <a href="https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3">
 * Run Length Encoding / Bit-Packing Hybrid (RLE)
 * </a>
 */
public final class RleBitPackingHybridBooleanDecoder
        implements ValueDecoder<BitBuffer>
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
    public void read(BitBuffer values, int offset, int length)
    {
        clearBits(values.getValues(), 0, offset, length);
        int trueCount = decoder.readNext(values.getValues(), offset, length);
        if (trueCount == length) {
            setBits(values.getValues(), 0, offset, length);
        }
    }

    @Override
    public void skip(int n)
    {
        decoder.skip(n);
    }
}
