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

import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.reader.SimpleSliceInputStream;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;

public interface ValueDecoder<T>
{
    void init(SimpleSliceInputStream input);

    void read(T values, int offset, int length);

    void skip(int n);

    class EmptyValueDecoder<T>
            implements ValueDecoder<T>
    {
        @Override
        public void init(SimpleSliceInputStream input) {}

        @Override
        public void read(T values, int offset, int length) {}

        @Override
        public void skip(int n) {}
    }

    interface ValueDecodersProvider<T>
    {
        ValueDecoder<T> create(ParquetEncoding encoding);
    }

    interface LevelsDecoderProvider
    {
        ValueDecoder<int[]> create(int maxLevel);
    }

    static ValueDecoder<int[]> createLevelsDecoder(int maxLevel)
    {
        if (maxLevel == 0) {
            return new ValueDecoder.EmptyValueDecoder<>();
        }
        return new RleBitPackingHybridDecoder(getWidthFromMaxInt(maxLevel));
    }
}
