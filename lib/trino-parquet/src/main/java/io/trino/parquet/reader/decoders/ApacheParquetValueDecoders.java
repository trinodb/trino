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
import io.trino.plugin.base.type.DecodedTimestamp;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static io.trino.parquet.ParquetReaderUtils.castToByte;
import static io.trino.parquet.ParquetTimestampUtils.decodeInt96Timestamp;
import static io.trino.parquet.reader.flat.Int96ColumnAdapter.Int96Buffer;
import static java.util.Objects.requireNonNull;

/**
 * This is a set of proxy value decoders that use a delegated value reader from apache lib.
 */
public class ApacheParquetValueDecoders
{
    private ApacheParquetValueDecoders() {}

    public static final class BooleanApacheParquetValueDecoder
            implements ValueDecoder<byte[]>
    {
        private final ValuesReader delegate;

        public BooleanApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            byte[] buffer = input.readBytes();
            try {
                // Deprecated PLAIN boolean decoder from Apache lib is the only one that actually
                // uses the valueCount argument to allocate memory so we simulate it here.
                int valueCount = buffer.length * Byte.SIZE;
                delegate.initFromPage(valueCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, 0, buffer.length)));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void read(byte[] values, int offset, int length)
        {
            for (int i = offset; i < offset + length; i++) {
                values[i] = castToByte(delegate.readBoolean());
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class UuidApacheParquetValueDecoder
            implements ValueDecoder<long[]>
    {
        private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

        private final ValuesReader delegate;

        public UuidApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(long[] values, int offset, int length)
        {
            int endOffset = (offset + length) * 2;
            for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
                byte[] data = delegate.readBytes().getBytes();
                values[currentOutputOffset] = (long) LONG_ARRAY_HANDLE.get(data, 0);
                values[currentOutputOffset + 1] = (long) LONG_ARRAY_HANDLE.get(data, Long.BYTES);
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    public static final class Int96ApacheParquetValueDecoder
            implements ValueDecoder<Int96Buffer>
    {
        private final ValuesReader delegate;

        public Int96ApacheParquetValueDecoder(ValuesReader delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            initialize(input, delegate);
        }

        @Override
        public void read(Int96Buffer values, int offset, int length)
        {
            int endOffset = offset + length;
            for (int i = offset; i < endOffset; i++) {
                DecodedTimestamp decodedTimestamp = decodeInt96Timestamp(delegate.readBytes());
                values.longs[i] = decodedTimestamp.epochSeconds();
                values.ints[i] = decodedTimestamp.nanosOfSecond();
            }
        }

        @Override
        public void skip(int n)
        {
            delegate.skip(n);
        }
    }

    private static void initialize(SimpleSliceInputStream input, ValuesReader reader)
    {
        byte[] buffer = input.readBytes();
        try {
            reader.initFromPage(0, ByteBufferInputStream.wrap(ByteBuffer.wrap(buffer, 0, buffer.length)));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
