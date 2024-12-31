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
package io.trino.plugin.paimon;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TypeUtils;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static org.apache.paimon.shade.guava30.com.google.common.base.Verify.verify;

/**
 * TrinoRow {@link InternalRow}.
 */
public class PaimonRow
        implements InternalRow, Serializable
{
    private final RowKind rowKind;
    private final Page singlePage;

    public PaimonRow(Page singlePage, RowKind rowKind)
    {
        verify(singlePage.getPositionCount() == 1, "singlePage must have only one row");
        this.singlePage = singlePage;
        this.rowKind = rowKind;
    }

    @Override
    public int getFieldCount()
    {
        return singlePage.getChannelCount();
    }

    @Override
    public RowKind getRowKind()
    {
        return rowKind;
    }

    @Override
    public void setRowKind(RowKind rowKind)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNullAt(int i)
    {
        return singlePage.getBlock(i).isNull(0);
    }

    @Override
    public boolean getBoolean(int i)
    {
        return (boolean) TypeUtils.readNativeValue(BOOLEAN, singlePage.getBlock(i), 0);
    }

    @Override
    public byte getByte(int i)
    {
        Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, singlePage.getBlock(i), 0);
        return slice.getByte(0);
    }

    @Override
    public short getShort(int i)
    {
        long value = (long) TypeUtils.readNativeValue(SMALLINT, singlePage.getBlock(i), 0);
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Value out of range for short: " + value);
        }
        return (short) value;
    }

    @Override
    public int getInt(int i)
    {
        long value = (long) TypeUtils.readNativeValue(INTEGER, singlePage.getBlock(i), 0);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Value out of range for int: " + value);
        }
        return (int) value;
    }

    @Override
    public long getLong(int i)
    {
        return (long) TypeUtils.readNativeValue(BIGINT, singlePage.getBlock(i), 0);
    }

    @Override
    public float getFloat(int i)
    {
        return intBitsToFloat(
                toIntExact((long) TypeUtils.readNativeValue(REAL, singlePage.getBlock(i), 0)));
    }

    @Override
    public double getDouble(int i)
    {
        return (double) TypeUtils.readNativeValue(DOUBLE, singlePage.getBlock(i), 0);
    }

    @Override
    public BinaryString getString(int i)
    {
        return BinaryString.fromBytes(getBinary(i));
    }

    @Override
    public Decimal getDecimal(int i, int decimalPrecision, int decimalScale)
    {
        Object value =
                TypeUtils.readNativeValue(
                        DecimalType.createDecimalType(decimalPrecision, decimalScale),
                        singlePage.getBlock(i),
                        0);
        if (decimalPrecision <= MAX_SHORT_PRECISION) {
            return Decimal.fromUnscaledLong((Long) value, decimalPrecision, decimalScale);
        }
        else {
            long high = ((Int128) value).getHigh();
            long low = ((Int128) value).getLow();
            BigInteger bigIntegerValue =
                    BigInteger.valueOf(high).shiftLeft(64).add(BigInteger.valueOf(low));
            BigDecimal bigDecimalValue = new BigDecimal(bigIntegerValue, decimalScale);
            return Decimal.fromBigDecimal(bigDecimalValue, decimalPrecision, decimalScale);
        }
    }

    @Override
    public Timestamp getTimestamp(int i, int timestampPrecision)
    {
        long value = (long) TypeUtils.readNativeValue(TIMESTAMP_MICROS, singlePage.getBlock(i), 0);
        return Timestamp.fromMicros(value);
    }

    @Override
    public byte[] getBinary(int i)
    {
        Slice slice = (Slice) TypeUtils.readNativeValue(VARBINARY, singlePage.getBlock(i), 0);
        return slice.getBytes();
    }

    @Override
    public InternalArray getArray(int i)
    {
        // todo
        //            singlePage.getBlock(i).getChildren()
        return null;
    }

    @Override
    public InternalMap getMap(int i)
    {
        // todo
        return null;
    }

    @Override
    public InternalRow getRow(int i, int i1)
    {
        // todo
        return null;
    }
}
