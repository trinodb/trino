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
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.dictionary.Dictionary;
import io.trino.parquet.reader.SimpleSliceInputStream;

import javax.annotation.Nullable;

import static io.trino.parquet.reader.decoders.ValueDecoders.getLongDecoder;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.util.Objects.requireNonNull;

/**
 * {@link io.trino.parquet.reader.decoders.ValueDecoder} implementations which build on top of implementations from {@link io.trino.parquet.reader.decoders.ValueDecoders}.
 * These decoders apply transformations to the output of an underlying primitive parquet type decoder to convert it into values
 * which can be used by {@link io.trino.parquet.reader.flat.ColumnAdapter} to create Trino blocks.
 */
public class TransformingValueDecoders
{
    private TransformingValueDecoders() {}

    public static ValueDecoder<long[]> getTimeMicrosDecoder(ParquetEncoding encoding, PrimitiveField field, @Nullable Dictionary dictionary)
    {
        return new InlineTransformDecoder<>(
                getLongDecoder(encoding, field, dictionary),
                (values, offset, length) -> {
                    for (int i = offset; i < offset + length; i++) {
                        values[i] = values[i] * PICOSECONDS_PER_MICROSECOND;
                    }
                });
    }

    private static class InlineTransformDecoder<T>
            implements ValueDecoder<T>
    {
        private final ValueDecoder<T> valueDecoder;
        private final TypeTransform<T> typeTransform;

        private InlineTransformDecoder(ValueDecoder<T> valueDecoder, TypeTransform<T> typeTransform)
        {
            this.valueDecoder = requireNonNull(valueDecoder, "valueDecoder is null");
            this.typeTransform = requireNonNull(typeTransform, "typeTransform is null");
        }

        @Override
        public void init(SimpleSliceInputStream input)
        {
            valueDecoder.init(input);
        }

        @Override
        public void read(T values, int offset, int length)
        {
            valueDecoder.read(values, offset, length);
            typeTransform.process(values, offset, length);
        }

        @Override
        public void skip(int n)
        {
            valueDecoder.skip(n);
        }
    }

    private interface TypeTransform<T>
    {
        void process(T values, int offset, int length);
    }
}
