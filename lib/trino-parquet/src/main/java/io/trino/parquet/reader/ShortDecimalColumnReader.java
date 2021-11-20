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
package io.trino.parquet.reader;

import io.trino.parquet.RichColumnDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.io.ParquetDecodingException;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalConversions.shortToLongCast;
import static io.trino.spi.type.DecimalConversions.shortToShortCast;
import static io.trino.spi.type.Decimals.isLongDecimal;
import static io.trino.spi.type.Decimals.isShortDecimal;
import static io.trino.spi.type.Decimals.longTenToNth;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ShortDecimalColumnReader
        extends PrimitiveColumnReader
{
    private final DecimalType parquetDecimalType;
    private final int typeLength;

    ShortDecimalColumnReader(RichColumnDescriptor descriptor, DecimalType parquetDecimalType)
    {
        super(descriptor);
        this.parquetDecimalType = requireNonNull(parquetDecimalType, "parquetDecimalType is null");
        typeLength = descriptor.getPrimitiveType().getTypeLength();
        checkArgument(typeLength <= 16, "Type length %s should be <= 16 for short decimal column %s", typeLength, descriptor);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type trinoType)
    {
        if (!((trinoType instanceof DecimalType) || isIntegerType(trinoType))) {
            throw new ParquetDecodingException(format("Unsupported Trino column type (%s) for Parquet column (%s)", trinoType, columnDescriptor));
        }

        long value;

        // When decimals are encoded with primitive types Parquet stores unscaled values
        if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT32) {
            value = valuesReader.readInteger();
        }
        else if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT64) {
            value = valuesReader.readLong();
        }
        else {
            byte[] bytes = valuesReader.readBytes().getBytes();
            if (typeLength <= Long.BYTES) {
                value = getShortDecimalValue(bytes);
            }
            else {
                int startOffset = bytes.length - Long.BYTES;
                checkBytesFitInShortDecimal(bytes, startOffset, trinoType);
                value = getShortDecimalValue(bytes, startOffset, Long.BYTES);
            }
        }

        if (trinoType instanceof DecimalType) {
            DecimalType trinoDecimalType = (DecimalType) trinoType;

            if (isShortDecimal(trinoDecimalType)) {
                long rescale = longTenToNth(Math.abs(trinoDecimalType.getScale() - parquetDecimalType.getScale()));
                long convertedValue = shortToShortCast(
                        value,
                        parquetDecimalType.getPrecision(),
                        parquetDecimalType.getScale(),
                        trinoDecimalType.getPrecision(),
                        trinoDecimalType.getScale(),
                        rescale,
                        rescale / 2);

                trinoType.writeLong(blockBuilder, convertedValue);
            }
            else if (isLongDecimal(trinoDecimalType)) {
                trinoType.writeObject(blockBuilder, shortToLongCast(
                        value,
                        parquetDecimalType.getPrecision(),
                        parquetDecimalType.getScale(),
                        trinoDecimalType.getPrecision(),
                        trinoDecimalType.getScale()));
            }
        }
        else {
            if (parquetDecimalType.getScale() != 0) {
                throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", trinoType, columnDescriptor));
            }

            if (!isInValidNumberRange(trinoType, value)) {
                throw new TrinoException(NOT_SUPPORTED, format("Could not coerce from %s to %s: %s", parquetDecimalType, trinoType, value));
            }
            trinoType.writeLong(blockBuilder, value);
        }
    }

    protected boolean isIntegerType(Type type)
    {
        return type.equals(TINYINT) || type.equals(SMALLINT) || type.equals(INTEGER) || type.equals(BIGINT);
    }

    protected boolean isInValidNumberRange(Type type, long value)
    {
        if (type.equals(TINYINT)) {
            return Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE;
        }
        if (type.equals(SMALLINT)) {
            return Short.MIN_VALUE <= value && value <= Short.MAX_VALUE;
        }
        if (type.equals(INTEGER)) {
            return Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE;
        }
        if (type.equals(BIGINT)) {
            return true;
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private void checkBytesFitInShortDecimal(byte[] bytes, int endOffset, Type trinoType)
    {
        // Equivalent to expectedValue = bytes[endOffset] < 0 ? -1 : 0
        byte expectedValue = (byte) (bytes[endOffset] >> 7);
        for (int i = 0; i < endOffset; i++) {
            if (bytes[i] != expectedValue) {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Could not read fixed_len_byte_array(%d) value %s into %s",
                        typeLength,
                        new BigDecimal(new BigInteger(bytes), parquetDecimalType.getScale()),
                        trinoType));
            }
        }
    }
}
