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
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class ShortDecimalColumnReader
        extends PrimitiveColumnReader
{
    ShortDecimalColumnReader(RichColumnDescriptor descriptor, DecimalType sourceType, Type targetType)
    {
        super(descriptor, sourceType, targetType);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder)
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            long decimalValue;

            if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT32) {
                decimalValue = valuesReader.readInteger();
            }
            else if (columnDescriptor.getPrimitiveType().getPrimitiveTypeName() == INT64) {
                decimalValue = valuesReader.readLong();
            }
            else {
                decimalValue = getShortDecimalValue(valuesReader.readBytes().getBytes());
            }

            if (isShortDecimal(sourceType)) {
                sourceType.writeLong(blockBuilder, decimalValue);
            }
            else if (isLongDecimal(sourceType)) {
                sourceType.writeSlice(blockBuilder, unscaledDecimal(decimalValue));
            }
            else {
                throw new ParquetDecodingException(format("Unsupported Presto column type (%s) for Parquet column (%s)", sourceType, columnDescriptor));
            }
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
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
