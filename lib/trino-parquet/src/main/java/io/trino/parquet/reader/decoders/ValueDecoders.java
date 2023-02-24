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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.PrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ValuesType.VALUES;
import static io.trino.parquet.reader.decoders.ApacheParquetValueDecoders.BooleanApacheParquetValueDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedByteDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedIntDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedLongDecoder;
import static io.trino.parquet.reader.decoders.DeltaBinaryPackedDecoders.DeltaBinaryPackedShortDecoder;
import static io.trino.parquet.reader.decoders.DeltaByteArrayDecoders.BinaryDeltaByteArrayDecoder;
import static io.trino.parquet.reader.decoders.DeltaByteArrayDecoders.BoundedVarcharDeltaByteArrayDecoder;
import static io.trino.parquet.reader.decoders.DeltaByteArrayDecoders.CharDeltaByteArrayDecoder;
import static io.trino.parquet.reader.decoders.DeltaLengthByteArrayDecoders.BinaryDeltaLengthDecoder;
import static io.trino.parquet.reader.decoders.DeltaLengthByteArrayDecoders.BoundedVarcharDeltaLengthDecoder;
import static io.trino.parquet.reader.decoders.DeltaLengthByteArrayDecoders.CharDeltaLengthDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.BinaryPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.BoundedVarcharPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainByteArrayDecoders.CharPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.BooleanPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.FixedLengthPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.Int96PlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntToBytePlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.IntToShortPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.LongDecimalPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.LongPlainValueDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.ShortDecimalFixedLengthByteArrayDecoder;
import static io.trino.parquet.reader.decoders.PlainValueDecoders.UuidPlainValueDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getBinaryLongDecimalDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getBinaryShortDecimalDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getDeltaFixedWidthLongDecimalDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getDeltaFixedWidthShortDecimalDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getDeltaUuidDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt32ToLongDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt64ToByteDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt64ToIntDecoder;
import static io.trino.parquet.reader.decoders.TransformingValueDecoders.getInt64ToShortDecoder;
import static io.trino.parquet.reader.flat.Int96ColumnAdapter.Int96Buffer;
import static org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;

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
        PrimitiveType primitiveType = field.getDescriptor().getPrimitiveType();
        checkArgument(
                primitiveType.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation,
                "Column %s is not annotated as a decimal",
                field);
        return switch (primitiveType.getPrimitiveTypeName()) {
            case INT64 -> getLongDecoder(encoding, field);
            case INT32 -> getInt32ToLongDecoder(encoding, field);
            case FIXED_LEN_BYTE_ARRAY -> getFixedWidthShortDecimalDecoder(encoding, field);
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
            case DELTA_BYTE_ARRAY -> getDeltaUuidDecoder(encoding);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<long[]> getLongDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new LongPlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedLongDecoder();
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
            case RLE -> new RleBitPackingHybridBooleanDecoder();
            // BIT_PACKED is a deprecated encoding which should not be used anymore as per
            // https://github.com/apache/parquet-format/blob/master/Encodings.md#bit-packed-deprecated-bit_packed--4
            // An unoptimized decoder for this encoding is provided here for compatibility with old files or non-compliant writers
            case BIT_PACKED -> new BooleanApacheParquetValueDecoder(getApacheParquetReader(encoding, field));
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<Int96Buffer> getInt96Decoder(ParquetEncoding encoding, PrimitiveField field)
    {
        if (PLAIN.equals(encoding)) {
            // INT96 type has been deprecated as per https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
            // However, this encoding is still commonly encountered in parquet files.
            return new Int96PlainValueDecoder();
        }
        throw wrongEncoding(encoding, field);
    }

    public static ValueDecoder<long[]> getFixedWidthShortDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new ShortDecimalFixedLengthByteArrayDecoder(field.getDescriptor());
            case DELTA_BYTE_ARRAY -> getDeltaFixedWidthShortDecimalDecoder(encoding, field);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<long[]> getFixedWidthLongDecimalDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new LongDecimalPlainValueDecoder(field.getDescriptor().getPrimitiveType().getTypeLength());
            case DELTA_BYTE_ARRAY -> getDeltaFixedWidthLongDecimalDecoder(encoding, field);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<BinaryBuffer> getFixedWidthBinaryDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new FixedLengthPlainValueDecoder(field.getDescriptor().getPrimitiveType().getTypeLength());
            case DELTA_BYTE_ARRAY -> new BinaryDeltaByteArrayDecoder();
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
            case DELTA_LENGTH_BYTE_ARRAY -> new BoundedVarcharDeltaLengthDecoder((VarcharType) trinoType);
            case DELTA_BYTE_ARRAY -> new BoundedVarcharDeltaByteArrayDecoder((VarcharType) trinoType);
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
            case DELTA_LENGTH_BYTE_ARRAY -> new CharDeltaLengthDecoder((CharType) trinoType);
            case DELTA_BYTE_ARRAY -> new CharDeltaByteArrayDecoder((CharType) trinoType);
            default -> throw wrongEncoding(encoding, field);
        };
    }

    public static ValueDecoder<BinaryBuffer> getBinaryDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new BinaryPlainValueDecoder();
            case DELTA_LENGTH_BYTE_ARRAY -> new BinaryDeltaLengthDecoder();
            case DELTA_BYTE_ARRAY -> new BinaryDeltaByteArrayDecoder();
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

    public static ValueDecoder<int[]> getInt32Decoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new IntPlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedIntDecoder();
            default -> throw wrongEncoding(encoding, field);
        };
    }

    private static ValueDecoder<short[]> getInt32ToShortDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new IntToShortPlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedShortDecoder();
            default -> throw wrongEncoding(encoding, field);
        };
    }

    private static ValueDecoder<byte[]> getInt32ToByteDecoder(ParquetEncoding encoding, PrimitiveField field)
    {
        return switch (encoding) {
            case PLAIN -> new IntToBytePlainValueDecoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedByteDecoder();
            default -> throw wrongEncoding(encoding, field);
        };
    }

    private static ValuesReader getApacheParquetReader(ParquetEncoding encoding, PrimitiveField field)
    {
        return encoding.getValuesReader(field.getDescriptor(), VALUES);
    }

    private static IllegalArgumentException wrongEncoding(ParquetEncoding encoding, PrimitiveField field)
    {
        return new IllegalArgumentException("Wrong encoding " + encoding + " for column " + field.getDescriptor());
    }
}
