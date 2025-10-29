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
package io.trino.parquet.writer.valuewriter;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.parquet.ParquetTypeUtils.paddingBigInteger;
import static java.util.Objects.requireNonNull;

public class FixedLenByteArrayLongDecimalValueWriter
        extends PrimitiveValueWriter
{
    private final DecimalType decimalType;

    public FixedLenByteArrayLongDecimalValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        this.decimalType = (DecimalType) requireNonNull(type, "type is null");
        checkArgument(!this.decimalType.isShort(), "type is not a long decimal");
        checkArgument(
                parquetType.getTypeLength() > 0 && parquetType.getTypeLength() <= Int128.SIZE,
                "Type length %s must be in range 1-%s",
                parquetType.getTypeLength(),
                Int128.SIZE);
    }

    @Override
    protected void writeValueBlock(ValueBlock block)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                byte[] bytes = readBytes(block, i);
                valuesWriter.writeBytes(Slices.wrappedBuffer(bytes));
                statistics.updateStats(Binary.fromConstantByteArray(bytes));
            }
        }
    }

    @Override
    protected void writeRepeated(ValueBlock block, int count)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        byte[] bytes = readBytes(block, 0);
        Slice slice = Slices.wrappedBuffer(bytes);
        for (int i = 0; i < count; i++) {
            valuesWriter.writeBytes(slice);
        }
        statistics.updateStats(Binary.fromConstantByteArray(bytes));
    }

    @Override
    protected void writePositions(ValueBlock block, int[] positions, int offset, int length)
    {
        ValuesWriter valuesWriter = getValuesWriter();
        Statistics<?> statistics = getStatistics();
        boolean mayHaveNull = block.mayHaveNull();
        for (int index = 0; index < length; index++) {
            int position = positions[offset + index];
            if (!mayHaveNull || !block.isNull(position)) {
                byte[] bytes = readBytes(block, position);
                valuesWriter.writeBytes(Slices.wrappedBuffer(bytes));
                statistics.updateStats(Binary.fromConstantByteArray(bytes));
            }
        }
    }

    private byte[] readBytes(ValueBlock block, int position)
    {
        Int128 decimal = (Int128) decimalType.getObject(block, position);
        BigInteger bigInteger = decimal.toBigInteger();
        return paddingBigInteger(bigInteger, getTypeLength());
    }
}
