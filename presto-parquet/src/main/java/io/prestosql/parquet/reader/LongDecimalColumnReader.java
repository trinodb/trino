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
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import java.math.BigInteger;

import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LongDecimalColumnReader
        extends PrimitiveColumnReader
{
    private final DecimalType parquetDecimalType;

    LongDecimalColumnReader(RichColumnDescriptor descriptor, DecimalType parquetDecimalType)
    {
        super(descriptor);
        this.parquetDecimalType = requireNonNull(parquetDecimalType, "parquetDecimalType is null");
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type prestoType)
    {
        if (!isLongDecimal(prestoType)) {
            throw new ParquetDecodingException(format("Unsupported Presto column type (%s) for Parquet column (%s)", prestoType, columnDescriptor));
        }
        DecimalType prestoDecimalType = (DecimalType) prestoType;
        if (prestoDecimalType.getScale() != parquetDecimalType.getScale()) {
            throw new ParquetDecodingException(format(
                    "Presto decimal column type has different scale (%s) than Parquet decimal column (%s)",
                    prestoDecimalType.getScale(),
                    parquetDecimalType.getScale()));
        }

        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            Binary value = valuesReader.readBytes();
            prestoType.writeSlice(blockBuilder, Decimals.encodeUnscaledValue(new BigInteger(value.getBytes())));
        }
        else if (isValueNull()) {
            blockBuilder.appendNull();
        }
    }

    @Override
    protected void skipValue()
    {
        if (definitionLevel == columnDescriptor.getMaxDefinitionLevel()) {
            valuesReader.readBytes();
        }
    }
}
