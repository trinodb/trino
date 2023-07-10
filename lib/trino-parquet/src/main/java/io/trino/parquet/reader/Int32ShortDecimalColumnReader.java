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

import io.trino.parquet.PrimitiveField;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Decimals.overflows;
import static java.lang.String.format;

public class Int32ShortDecimalColumnReader
        extends PrimitiveColumnReader
{
    public Int32ShortDecimalColumnReader(PrimitiveField field)
    {
        super(field);
    }

    @Override
    protected void readValue(BlockBuilder blockBuilder, Type trinoType)
    {
        if (trinoType instanceof DecimalType trinoDecimalType && trinoDecimalType.isShort()) {
            long value = valuesReader.readInteger();
            if (overflows(value, trinoDecimalType.getPrecision())) {
                throw new TrinoException(INVALID_CAST_ARGUMENT, format("Cannot read parquet INT32 value '%s' as DECIMAL(%s, %s)", value, trinoDecimalType.getPrecision(), trinoDecimalType.getScale()));
            }

            trinoType.writeLong(blockBuilder, value);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format("Unsupported Trino column type (%s) for Parquet column (%s)", trinoType, field.getDescriptor()));
        }
    }
}
