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
import io.trino.spi.type.DecimalType;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

import java.math.BigInteger;
import java.util.OptionalInt;
import java.util.Random;
import java.util.stream.IntStream;

import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static io.trino.parquet.ParquetTypeUtils.paddingBigInteger;
import static io.trino.parquet.reader.TestData.longToBytes;
import static io.trino.parquet.reader.TestData.maxPrecision;
import static io.trino.parquet.reader.TestData.unscaledRandomShortDecimalSupplier;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.LongDecimalApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.ShortDecimalApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ValueDecoders.getFixedWidthShortDecimalDecoder;
import static io.trino.parquet.reader.flat.Int128ColumnAdapter.INT128_ADAPTER;
import static io.trino.parquet.reader.flat.LongColumnAdapter.LONG_ADAPTER;
import static io.trino.testing.DataProviders.concat;
import static java.lang.Math.min;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestFixedWidthByteArrayValueDecoders
        extends AbstractValueDecodersTest
{
    @Override
    protected Object[][] tests()
    {
        return concat(
                generateShortDecimalTests(PLAIN),
                generateShortDecimalTests(RLE_DICTIONARY),
                generateLongDecimalTests(PLAIN),
                generateLongDecimalTests(RLE_DICTIONARY));
    }

    private static Object[][] generateShortDecimalTests(ParquetEncoding encoding)
    {
        return IntStream.range(1, 17)
                .mapToObj(typeLength -> {
                    int precision = maxPrecision(min(typeLength, 8));
                    return new Object[] {
                            createShortDecimalTestType(typeLength, precision),
                            encoding,
                            createShortDecimalInputDataProvider(typeLength, precision)};
                })
                .toArray(Object[][]::new);
    }

    private static Object[][] generateLongDecimalTests(ParquetEncoding encoding)
    {
        return IntStream.range(9, 17)
                .mapToObj(typeLength -> new Object[] {
                        createLongDecimalTestType(typeLength),
                        encoding,
                        createLongDecimalInputDataProvider(typeLength)})
                .toArray(Object[][]::new);
    }

    private static TestType<long[]> createShortDecimalTestType(int typeLength, int precision)
    {
        DecimalType decimalType = DecimalType.createDecimalType(precision, 2);
        PrimitiveField primitiveField = createField(FIXED_LEN_BYTE_ARRAY, OptionalInt.of(typeLength), decimalType);
        return new TestType<>(
                primitiveField,
                (encoding, field) -> getFixedWidthShortDecimalDecoder(encoding, field, decimalType),
                valuesReader -> new ShortDecimalApacheParquetValueDecoder(valuesReader, decimalType, primitiveField.getDescriptor()),
                LONG_ADAPTER,
                (actual, expected) -> assertThat(actual).isEqualTo(expected));
    }

    private static TestType<long[]> createLongDecimalTestType(int typeLength)
    {
        int precision = maxPrecision(typeLength);
        DecimalType decimalType = DecimalType.createDecimalType(precision, 2);
        return new TestType<>(
                createField(FIXED_LEN_BYTE_ARRAY, OptionalInt.of(typeLength), decimalType),
                ValueDecoders::getFixedWidthLongDecimalDecoder,
                LongDecimalApacheParquetValueDecoder::new,
                INT128_ADAPTER,
                (actual, expected) -> assertThat(actual).isEqualTo(expected));
    }

    private static InputDataProvider createShortDecimalInputDataProvider(int typeLength, int precision)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                long[] values = unscaledRandomShortDecimalSupplier(min(typeLength * Byte.SIZE, 64), precision).apply(dataSize);
                byte[][] bytes = new byte[dataSize][];
                for (int i = 0; i < dataSize; i++) {
                    bytes[i] = longToBytes(values[i], typeLength);
                }
                return writeBytes(valuesWriter, bytes);
            }

            @Override
            public String toString()
            {
                return "fixed_len_byte_array(" + typeLength + ")";
            }
        };
    }

    private static InputDataProvider createLongDecimalInputDataProvider(int typeLength)
    {
        return new InputDataProvider() {
            @Override
            public DataBuffer write(ValuesWriter valuesWriter, int dataSize)
            {
                Random random = new Random(dataSize);
                byte[][] bytes = new byte[dataSize][];
                for (int i = 0; i < dataSize; i++) {
                    bytes[i] = paddingBigInteger(
                            new BigInteger(Math.min(typeLength * Byte.SIZE - 1, 126), random),
                            typeLength);
                }
                return writeBytes(valuesWriter, bytes);
            }

            @Override
            public String toString()
            {
                return "fixed_len_byte_array(" + typeLength + ")";
            }
        };
    }

    private static DataBuffer writeBytes(ValuesWriter valuesWriter, byte[][] input)
    {
        for (byte[] value : input) {
            valuesWriter.writeBytes(Binary.fromConstantByteArray(value));
        }

        return getWrittenBuffer(valuesWriter);
    }
}
