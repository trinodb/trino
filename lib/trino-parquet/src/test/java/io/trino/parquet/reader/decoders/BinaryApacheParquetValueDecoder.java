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
import io.trino.parquet.reader.flat.BinaryBuffer;
import org.apache.parquet.column.values.ValuesReader;

import static java.util.Objects.requireNonNull;

final class BinaryApacheParquetValueDecoder
        implements ValueDecoder<BinaryBuffer>
{
    private final ValuesReader delegate;

    public BinaryApacheParquetValueDecoder(ValuesReader delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void init(SimpleSliceInputStream input)
    {
        AbstractValueDecodersTest.initialize(input, delegate);
    }

    @Override
    public void read(BinaryBuffer values, int offsetsIndex, int length)
    {
        for (int i = 0; i < length; i++) {
            byte[] value = delegate.readBytes().getBytes();
            values.add(value, i + offsetsIndex);
        }
    }

    @Override
    public void skip(int n)
    {
        delegate.skip(n);
    }
}
