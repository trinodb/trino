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
package io.trino.lance.file.v2.encoding;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.lance.file.v2.reader.BinaryBuffer;
import io.trino.lance.file.v2.reader.BufferAdapter;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.lance.file.v2.reader.BinaryBufferAdapter.VARIABLE_BINARY_BUFFER_ADAPTER;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FsstEncoding
        implements LanceEncoding
{
    public static final int FSST_MAX_SYMBOLS = 256;
    public static final int FSST_MAX_SYMBOL_SIZE = 8;
    public static final int FSST_SYMBOL_TABLE_SIZE = Long.BYTES + FSST_MAX_SYMBOLS * FSST_MAX_SYMBOL_SIZE + FSST_MAX_SYMBOLS;
    public static final long FSST_SYMBOL_TABLE_MAGIC = 0x46535354L << 32;
    public static final short FSST_ESC = 255;

    private final long header;
    private final int numSymbols;
    private final Slice symbols;
    private final short[] lengths;
    private final LanceEncoding valueEncoding;

    public FsstEncoding(LanceEncoding valueEncoding, Slice symbolTableSlice)
    {
        checkArgument(symbolTableSlice.length() == FSST_SYMBOL_TABLE_SIZE, format("FSST symbol table must have %d bytes", FSST_SYMBOL_TABLE_SIZE));
        checkArgument((symbolTableSlice.getLong(0) & FSST_SYMBOL_TABLE_MAGIC) == FSST_SYMBOL_TABLE_MAGIC, "Invalid header in FSST symbol table");
        this.header = symbolTableSlice.getLong(0);
        this.numSymbols = toIntExact(header & 0xFF);
        this.symbols = symbolTableSlice.slice(Long.BYTES, FSST_MAX_SYMBOL_SIZE * numSymbols);
        this.lengths = new short[FSST_MAX_SYMBOLS];
        for (int i = 0; i < numSymbols; i++) {
            this.lengths[i] = symbolTableSlice.getUnsignedByte(Long.BYTES + numSymbols * FSST_MAX_SYMBOL_SIZE + i);
        }
        this.valueEncoding = requireNonNull(valueEncoding, "valueEncoding is null");
    }

    @Override
    public BufferAdapter getBufferAdapter()
    {
        return VARIABLE_BINARY_BUFFER_ADAPTER;
    }

    @Override
    public MiniBlockDecoder getMiniBlockDecoder()
    {
        return new FsstMiniBlockDecoder(valueEncoding.getMiniBlockDecoder());
    }

    public static FsstEncoding fromProto(build.buf.gen.lance.encodings21.Fsst proto)
    {
        LanceEncoding valueEncoding = LanceEncoding.fromProto(proto.getValues());
        return new FsstEncoding(valueEncoding, Slices.wrappedBuffer(proto.getSymbolTable().toByteArray()));
    }

    public class FsstMiniBlockDecoder
            implements MiniBlockDecoder<BinaryBuffer>
    {
        private final MiniBlockDecoder<BinaryBuffer> valueDecoder;

        public FsstMiniBlockDecoder(MiniBlockDecoder<BinaryBuffer> valueDecoder)
        {
            this.valueDecoder = valueDecoder;
        }

        @Override
        public void init(List<Slice> slices, int numValues)
        {
            checkArgument(slices.size() == 1, "FSST encoded block has exactly one buffer");
            valueDecoder.init(slices, numValues);
        }

        @Override
        public void read(int sourceIndex, BinaryBuffer destination, int destinationIndex, int length)
        {
            // TODO: performance optimizations with vectorization, loop unrolling and reduced memory copy
            BinaryBuffer inputs = VARIABLE_BINARY_BUFFER_ADAPTER.createBuffer(length);
            valueDecoder.read(sourceIndex, inputs, destinationIndex, length);
            for (int i = 0; i < length; i++) {
                destination.add(decompress(inputs.get(i)), destinationIndex + i);
            }
        }

        public Slice decompress(Slice input)
        {
            if ((header & (1 << 24)) == 0) {
                return input.copy();
            }

            ByteArrayList output = new ByteArrayList(FSST_MAX_SYMBOL_SIZE * input.length());
            int count = 0;
            for (int i = 0; i < input.length(); i++) {
                short code = input.getUnsignedByte(i);
                if (code < FSST_ESC) {
                    int size = lengths[code];
                    output.addElements(count, symbols.getBytes(code * FSST_MAX_SYMBOL_SIZE, size));
                    count += size;
                }
                else {
                    i++;
                    output.add(input.getByte(i));
                    count++;
                }
            }
            return Slices.wrappedBuffer(output.toByteArray());
        }
    }
}
