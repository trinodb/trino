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
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import org.apache.parquet.io.ParquetDecodingException;

import static io.prestosql.parquet.ParquetTypeUtils.getShortDecimalValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
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
            long decimalValue;
            // When decimals are encoded with primitive types Parquet stores unscaled values
            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT32) {
                decimalValue = valuesReader.readInteger();
            }
            else if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT64) {
                decimalValue = valuesReader.readLong();
            }
            else {
                decimalValue = getShortDecimalValue(valuesReader.readBytes().getBytes());
            }

            if (isShortDecimal(prestoType)) {
                validateDecimal((DecimalType) prestoType);
                // TODO: validate that value fits Presto decimal precision
                prestoType.writeLong(blockBuilder, decimalValue);
            }
            else if (isLongDecimal(prestoType)) {
                validateDecimal((DecimalType) prestoType);
                // TODO: validate that value fits Presto decimal precision
                prestoType.writeSlice(blockBuilder, unscaledDecimal(decimalValue));
            }
            else if (prestoType.equals(TINYINT) || prestoType.equals(SMALLINT) || prestoType.equals(INTEGER) || prestoType.equals(BIGINT)) {
                if (parquetDecimalType.getScale() != 0) {
                    throw new ParquetDecodingException(format(
                            "Parquet decimal column type with non-zero scale (%s) cannot be converted to Presto %s column type",
                            parquetDecimalType.getScale(),
                            prestoType));
                }
                prestoType.writeLong(blockBuilder, decimalValue);
            }
            else {
                throw new ParquetDecodingException(format("Unsupported Presto column type (%s) for Parquet column (%s)", prestoType, columnDescriptor));
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    private void validateDecimal(DecimalType prestoType)
    {
        if (prestoType.getScale() != parquetDecimalType.getScale()) {
            throw new ParquetDecodingException(format(
                    "Presto decimal column type has different scale (%s) than Parquet decimal column (%s)",
                    prestoType.getScale(),
                    parquetDecimalType.getScale()));
        }
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
