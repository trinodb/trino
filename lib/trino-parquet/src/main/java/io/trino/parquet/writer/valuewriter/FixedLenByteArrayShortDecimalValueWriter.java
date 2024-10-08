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

import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FixedLenByteArrayShortDecimalValueWriter
        extends PrimitiveValueWriter
{
    private final DecimalType decimalType;

    public FixedLenByteArrayShortDecimalValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        super(parquetType, valuesWriter);
        this.decimalType = (DecimalType) requireNonNull(type, "type is null");
        checkArgument(this.decimalType.isShort(), "type is not a short decimal");
        checkArgument(
                parquetType.getTypeLength() > 0 && parquetType.getTypeLength() <= Long.BYTES,
                "Type length %s must be in range 1-%s",
                parquetType.getTypeLength(),
                Long.BYTES);
    }

    @Override
    public void write(Block block)
    {
        ValuesWriter valuesWriter = requireNonNull(getValuesWriter(), "valuesWriter is null");
        Statistics<?> statistics = requireNonNull(getStatistics(), "statistics is null");
        boolean mayHaveNull = block.mayHaveNull();
        byte[] buffer = new byte[getTypeLength()];
        Binary reusedBinary = Binary.fromReusedByteArray(buffer);
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!mayHaveNull || !block.isNull(i)) {
                long value = decimalType.getLong(block, i);
                storeLongIntoBuffer(value, buffer);
                valuesWriter.writeBytes(reusedBinary);
                statistics.updateStats(reusedBinary);
            }
        }
    }

    private static void storeLongIntoBuffer(long unscaledValue, byte[] buffer)
    {
        switch (buffer.length) {
            case 1:
                buffer[0] = (byte) unscaledValue;
                break;
            case 2:
                buffer[0] = (byte) (unscaledValue >> 8);
                buffer[1] = (byte) unscaledValue;
                break;
            case 3:
                buffer[0] = (byte) (unscaledValue >> 16);
                buffer[1] = (byte) (unscaledValue >> 8);
                buffer[2] = (byte) unscaledValue;
                break;
            case 4:
                buffer[0] = (byte) (unscaledValue >> 24);
                buffer[1] = (byte) (unscaledValue >> 16);
                buffer[2] = (byte) (unscaledValue >> 8);
                buffer[3] = (byte) unscaledValue;
                break;
            case 5:
                buffer[0] = (byte) (unscaledValue >> 32);
                buffer[1] = (byte) (unscaledValue >> 24);
                buffer[2] = (byte) (unscaledValue >> 16);
                buffer[3] = (byte) (unscaledValue >> 8);
                buffer[4] = (byte) unscaledValue;
                break;
            case 6:
                buffer[0] = (byte) (unscaledValue >> 40);
                buffer[1] = (byte) (unscaledValue >> 32);
                buffer[2] = (byte) (unscaledValue >> 24);
                buffer[3] = (byte) (unscaledValue >> 16);
                buffer[4] = (byte) (unscaledValue >> 8);
                buffer[5] = (byte) unscaledValue;
                break;
            case 7:
                buffer[0] = (byte) (unscaledValue >> 48);
                buffer[1] = (byte) (unscaledValue >> 40);
                buffer[2] = (byte) (unscaledValue >> 32);
                buffer[3] = (byte) (unscaledValue >> 24);
                buffer[4] = (byte) (unscaledValue >> 16);
                buffer[5] = (byte) (unscaledValue >> 8);
                buffer[6] = (byte) unscaledValue;
                break;
            case 8:
                buffer[0] = (byte) (unscaledValue >> 56);
                buffer[1] = (byte) (unscaledValue >> 48);
                buffer[2] = (byte) (unscaledValue >> 40);
                buffer[3] = (byte) (unscaledValue >> 32);
                buffer[4] = (byte) (unscaledValue >> 24);
                buffer[5] = (byte) (unscaledValue >> 16);
                buffer[6] = (byte) (unscaledValue >> 8);
                buffer[7] = (byte) unscaledValue;
                break;
            default:
                throw new IllegalArgumentException("Invalid number of bytes: " + buffer.length);
        }
    }
}
