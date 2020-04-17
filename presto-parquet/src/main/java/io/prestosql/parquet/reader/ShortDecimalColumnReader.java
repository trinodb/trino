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
package io.prestosql.parquet.reader;

import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import org.apache.parquet.io.ParquetDecodingException;

import static io.prestosql.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DecimalConversions.shortToLongCast;
import static io.prestosql.spi.type.DecimalConversions.shortToShortCast;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.Decimals.longTenToNth;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ShortDecimalColumnReader
        extends PrimitiveColumnReader
{
    private final DecimalType parquetDecimalType;

    ShortDecimalColumnReader(RichColumnDescriptor descriptor, DecimalType parquetDecimalType)
    {
        super(descriptor);
        this.parquetDecimalType = requireNonNull(parquetDecimalType, "parquetDecimalType is null");
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type prestoType)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (!((prestoType instanceof DecimalType) || isIntegerType(prestoType))) {
                throw new ParquetDecodingException(format("Unsupported Presto column type (%s) for Parquet column (%s)", prestoType, columnDescriptor));
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
                value = getShortDecimalValue(valuesReader.readBytes().getBytes());
            }

            if (prestoType instanceof DecimalType) {
                DecimalType prestoDecimalType = (DecimalType) prestoType;

                if (isShortDecimal(prestoDecimalType)) {
                    long rescale = longTenToNth(Math.abs(prestoDecimalType.getScale() - parquetDecimalType.getScale()));
                    long convertedValue = shortToShortCast(
                            value,
                            parquetDecimalType.getPrecision(),
                            parquetDecimalType.getScale(),
                            prestoDecimalType.getPrecision(),
                            prestoDecimalType.getScale(),
                            rescale,
                            rescale / 2);

                    prestoType.writeLong(blockBuilder, convertedValue);
                }
                else if (isLongDecimal(prestoDecimalType)) {
                    prestoType.writeSlice(blockBuilder, shortToLongCast(
                            value,
                            parquetDecimalType.getPrecision(),
                            parquetDecimalType.getScale(),
                            prestoDecimalType.getPrecision(),
                            prestoDecimalType.getScale()));
                }
            }
            else {
                if (parquetDecimalType.getScale() != 0) {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported Presto column type (%s) for Parquet column (%s)", prestoType, columnDescriptor));
                }

                if (!isInValidNumberRange(prestoType, value)) {
                    throw new PrestoException(NOT_SUPPORTED, format("Could not coerce from %s to %s: %s", parquetDecimalType, prestoType, value));
                }
                prestoType.writeLong(blockBuilder, value);
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
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

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT32) {
                valuesReader.readInteger();
            }
            else if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT64) {
                valuesReader.readLong();
            }
            else {
                valuesReader.readBytes();
            }
        }
    }
}
