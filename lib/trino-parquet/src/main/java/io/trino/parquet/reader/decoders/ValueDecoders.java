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

import io.trino.parquet.DictionaryPage;
import io.trino.parquet.ParquetEncoding;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.SimpleSliceInputStream;
import io.trino.parquet.reader.flat.BinaryBuffer;
import io.trino.parquet.reader.flat.ColumnAdapter;
import io.trino.parquet.reader.flat.DictionaryDecoder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.values.ValuesReader;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ValuesType.VALUES;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.BinaryApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.BooleanApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.BoundedVarcharApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.ByteApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.CharApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.Int96ApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.IntApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.IntToLongApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.LongApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.LongDecimalApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.ShortApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.ShortDecimalApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.UuidApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.BinaryPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.BoundedVarcharPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.CharPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.BooleanPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntToBytePlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntToLongPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntToShortPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.LongDecimalPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.LongPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.ShortDecimalFixedLengthByteArrayDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.UuidPlainValueDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getBinaryLongDecimalDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getBinaryShortDecimalDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt64ToByteDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt64ToIntDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt64ToShortDecoder;
import static io.trino.parquet.reader.flat.Int96ColumnAdapter.Int96Buffer;

/**
 * This class provides static API for creating value decoders for given fields and encodings.
 * If no suitable decoder is found the Apache Parquet fallback is used.
 * Not all types are supported since this class is at this point used only by flat readers
 * <p>
 * This class is to replace most of the logic contained in ParquetEncoding enum
 */
public final class ValueDecoders
{
    private ValueDecoders() {}

    public static ValueDecoder<long[]> getDoubleDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        if (PLAIN.equals(encoding)) {
            return new LongPlainValueDecoder();
        }
        throw wrongEncoding(encoding, field);
    }

    public static ValueDecoder<int[]> getRealDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        if (PLAIN.equals(encoding)) {
            return new IntPlainValueDecoder();
        }
        throw wrongEncoding(encoding, field);
    }

    public static ValueDecoder<long[]> getShortDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        checkArgument(field.getType() instanceof DecimalType, "Trino type %s is not a decimal", field.getType());
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case INT64 -> getLongDecoder(encoding, field);
            case INT32 -> getIntToLongDecoder(encoding, field);
            case FIXED_LEN_BYTE_ARRAY -> getFixedWidthShortDecimalDecoder(encoding, field, (DecimalType) field.getType());
            case BINARY -> getBinaryShortDecimalDecoder(encoding, field);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<long[]> getLongDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case FIXED_LEN_BYTE_ARRAY -> getFixedWidthLongDecimalDecoder(encoding, field);
            case BINARY -> getBinaryLongDecimalDecoder(encoding, field);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<long[]> getUuidDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new UuidPlainValueDecoder();
            case DELTA_BYTE_ARRAY ->
                    new UuidApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<long[]> getLongDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new LongPlainValueDecoder();
            case DELTA_BINARY_PACKED, RLE, BIT_PACKED ->
                    new LongApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<long[]> getIntToLongDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        // We need to produce LongArrayBlock from the decoded integers for INT32 backed decimals and bigints
        return switch (encoding) {
            case PLAIN -> new IntToLongPlainValueDecoder();
            case DELTA_BINARY_PACKED, RLE, BIT_PACKED ->
                    new IntToLongApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<int[]> getIntDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case INT64 -> getInt64ToIntDecoder(encoding, field);
            case INT32 -> getInt32Decoder(encoding, field);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<short[]> getShortDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case INT64 -> getInt64ToShortDecoder(encoding, field);
            case INT32 -> getInt32ToShortDecoder(encoding, field);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<byte[]> getByteDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (field.getDescriptor().getPrimitiveType().getPrimitiveTypeName()) {
            case INT64 -> getInt64ToByteDecoder(encoding, field);
            case INT32 -> getInt32ToByteDecoder(encoding, field);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<byte[]> getBooleanDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new BooleanPlainValueDecoder();
            case RLE, BIT_PACKED -> new BooleanApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<Int96Buffer> getInt96Decoder(ParquetEncoding encoding, PrimitiveField field)
    {
        if (PLAIN.equals(encoding)) {
            return new Int96ApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
        }
        throw wrongEncoding(encoding, field);
    }

    public static ValueDecoder<long[]> getFixedWidthShortDecimalDecoder(ParquetEncoding encoding, PrimitiveField field, DecimalType decimalType)
    {
        return switch (encoding) {
            case PLAIN -> new ShortDecimalFixedLengthByteArrayDecoder(decimalType, field.getDescriptor());
            case DELTA_BYTE_ARRAY -> new ShortDecimalApacheParquetValueDecoder(
                    getApacheParquetReader(encoding, field),
                    decimalType,
                    field.getDescriptor());
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<long[]> getFixedWidthLongDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new LongDecimalPlainValueDecoder(field.getDescriptor().getPrimitiveType().getTypeLength());
            case DELTA_BYTE_ARRAY ->
                    new LongDecimalApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<BinaryBuffer> getBoundedVarcharBinaryDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        Type trinoType = field.getType();
        checkArgument(
                trinoType instanceof VarcharType varcharType && !varcharType.isUnbounded(),
                "Trino type %s is not a bounded varchar",
                trinoType);
        return switch (encoding) {
            case PLAIN -> new BoundedVarcharPlainValueDecoder((VarcharType) trinoType);
            case DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY ->
                    new BoundedVarcharApacheParquetValueDecoder(getApacheParquetReader(encoding, field), (VarcharType) trinoType);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<BinaryBuffer> getCharBinaryDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        Type trinoType = field.getType();
        checkArgument(
                trinoType instanceof CharType,
                "Trino type %s is not a char",
                trinoType);
        return switch (encoding) {
            case PLAIN -> new CharPlainValueDecoder((CharType) trinoType);
            case DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY ->
                    new CharApacheParquetValueDecoder(getApacheParquetReader(encoding, field), (CharType) trinoType);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<BinaryBuffer> getBinaryDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new BinaryPlainValueDecoder();
            case DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY ->
                    new BinaryApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static <T> DictionaryDecoder<T> getDictionaryDecoder(
            DictionaryPage dictionaryPage,
            ColumnAdapter<T> columnAdapter,
            ValueDecoder<T> plainValuesDecoder,
            boolean isNonNull)
    {
        int size = dictionaryPage.getDictionarySize();
        // Extra value is added to the end of the dictionary for nullable columns because
        // parquet dictionary page does not include null but Trino DictionaryBlock's dictionary does
        T dictionary = columnAdapter.createBuffer(size + (isNonNull ? 0 : 1));
        plainValuesDecoder.init(new SimpleSliceInputStream(dictionaryPage.getSlice()));
        plainValuesDecoder.read(dictionary, 0, size);
        return new DictionaryDecoder<>(dictionary, columnAdapter, size, isNonNull);
    }

    private static ValueDecoder<int[]> getInt32Decoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new IntPlainValueDecoder();
            case DELTA_BINARY_PACKED, RLE, BIT_PACKED ->
                    new IntApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    private static ValueDecoder<short[]> getInt32ToShortDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new IntToShortPlainValueDecoder();
            case DELTA_BINARY_PACKED, RLE, BIT_PACKED ->
                    new ShortApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    private static ValueDecoder<byte[]> getInt32ToByteDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new IntToBytePlainValueDecoder();
            case DELTA_BINARY_PACKED, RLE, BIT_PACKED ->
                    new ByteApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    private static ValuesReader getApacheParquetReader(ParquetEncoding encoding, PrimitiveField field)
    {
        return encoding.getValuesReader(field.getDescriptor(), VALUES);
    }

    private static IllegalArgumentException wrongEncoding(ParquetEncoding encoding, PrimitiveField field)
    {
        return new IllegalArgumentException("Wrong encoding " + encoding + " for column type " + field.getDescriptor().getPrimitiveType().getPrimitiveTypeName());
    }
}
