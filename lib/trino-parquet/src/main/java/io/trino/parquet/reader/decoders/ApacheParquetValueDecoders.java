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
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import static io.trino.parquet.ParquetReaderUtils.castToByte;
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
}
