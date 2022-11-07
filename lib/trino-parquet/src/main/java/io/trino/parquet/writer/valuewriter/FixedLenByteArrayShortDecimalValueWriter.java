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
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (!block.isNull(i)) {
                long value = decimalType.getLong(block, i);
                Binary binary = Binary.fromConstantByteArray(paddingLong(value));
                getValueWriter().writeBytes(binary);
                getStatistics().updateStats(binary);
            }
        }
    }

    private byte[] paddingLong(long unscaledValue)
    {
        int numBytes = getTypeLength();
        byte[] result = new byte[numBytes];
        switch (numBytes) {
            case 1:
                result[0] = (byte) unscaledValue;
                break;
            case 2:
                result[0] = (byte) (unscaledValue >> 8);
                result[1] = (byte) unscaledValue;
                break;
            case 3:
                result[0] = (byte) (unscaledValue >> 16);
                result[1] = (byte) (unscaledValue >> 8);
                result[2] = (byte) unscaledValue;
                break;
            case 4:
                result[0] = (byte) (unscaledValue >> 24);
                result[1] = (byte) (unscaledValue >> 16);
                result[2] = (byte) (unscaledValue >> 8);
                result[3] = (byte) unscaledValue;
                break;
            case 5:
                result[0] = (byte) (unscaledValue >> 32);
                result[1] = (byte) (unscaledValue >> 24);
                result[2] = (byte) (unscaledValue >> 16);
                result[3] = (byte) (unscaledValue >> 8);
                result[4] = (byte) unscaledValue;
                break;
            case 6:
                result[0] = (byte) (unscaledValue >> 40);
                result[1] = (byte) (unscaledValue >> 32);
                result[2] = (byte) (unscaledValue >> 24);
                result[3] = (byte) (unscaledValue >> 16);
                result[4] = (byte) (unscaledValue >> 8);
                result[5] = (byte) unscaledValue;
                break;
            case 7:
                result[0] = (byte) (unscaledValue >> 48);
                result[1] = (byte) (unscaledValue >> 40);
                result[2] = (byte) (unscaledValue >> 32);
                result[3] = (byte) (unscaledValue >> 24);
                result[4] = (byte) (unscaledValue >> 16);
                result[5] = (byte) (unscaledValue >> 8);
                result[6] = (byte) unscaledValue;
                break;
            case 8:
                result[0] = (byte) (unscaledValue >> 56);
                result[1] = (byte) (unscaledValue >> 48);
                result[2] = (byte) (unscaledValue >> 40);
                result[3] = (byte) (unscaledValue >> 32);
                result[4] = (byte) (unscaledValue >> 24);
                result[5] = (byte) (unscaledValue >> 16);
                result[6] = (byte) (unscaledValue >> 8);
                result[7] = (byte) unscaledValue;
                break;
            default:
                throw new IllegalArgumentException("Invalid number of bytes: " + numBytes);
        }
        return result;
    }
}
