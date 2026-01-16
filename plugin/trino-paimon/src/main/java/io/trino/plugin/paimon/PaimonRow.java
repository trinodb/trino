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
import io.trino.spi.TrinoException;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalMap;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Decimals.MAX_SHORT_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PaimonRow
        implements InternalRow, Serializable
{
    private final RowType rowType;
    private final RowKind rowKind;
    private final Page singlePage;
    private final Type[] cacheTypes;

    public PaimonRow(RowType rowType, Page singlePage, RowKind rowKind)
    {
        verify(singlePage.getPositionCount() == 1, "singlePage must have only one row");
        this.rowType = requireNonNull(rowType, "rowType is null");
        this.singlePage = requireNonNull(singlePage, "singlePage is null");
        this.rowKind = requireNonNull(rowKind, "rowKind is null");
        cacheTypes = new Type[singlePage.getChannelCount()];
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
        return (boolean) readNativeValue(BOOLEAN, singlePage.getBlock(i), 0);
    }

    @Override
    public byte getByte(int i)
    {
        return ((ByteArrayBlock) singlePage.getBlock(i)).getByte(0);
    }

    @Override
    public short getShort(int i)
    {
        long value = (long) readNativeValue(SMALLINT, singlePage.getBlock(i), 0);
        checkArgument(value >= Short.MIN_VALUE && value <= Short.MAX_VALUE, "Value out of range for short: %s", value);
        return (short) value;
    }

    @Override
    public int getInt(int i)
    {
        if (rowType.getTypeAt(i).getTypeRoot() == DataTypeRoot.TIME_WITHOUT_TIME_ZONE) {
            if (cacheTypes[i] == null) {
                cacheTypes[i] = TimeType.createTimeType(((org.apache.paimon.types.TimeType) rowType.getTypeAt(i)).getPrecision());
            }
            SqlTime sqlTime = (SqlTime) cacheTypes[i].getObjectValue(singlePage.getBlock(i), 0);
            return (int) (sqlTime.getPicos() / 1_000_000);
        }

        long value = (long) readNativeValue(INTEGER, singlePage.getBlock(i), 0);
        checkArgument(value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE, "Value out of range for int: %s", value);
        return (int) value;
    }

    @Override
    public long getLong(int i)
    {
        return (long) readNativeValue(BIGINT, singlePage.getBlock(i), 0);
    }

    @Override
    public float getFloat(int i)
    {
        return intBitsToFloat(
                toIntExact((long) readNativeValue(REAL, singlePage.getBlock(i), 0)));
    }

    @Override
    public double getDouble(int i)
    {
        return (double) readNativeValue(DOUBLE, singlePage.getBlock(i), 0);
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
                readNativeValue(
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
        if (rowType.getTypeAt(i).getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            SqlTimestampWithTimeZone sqlTimestampWithTimeZone = (SqlTimestampWithTimeZone) TimestampWithTimeZoneType.createTimestampWithTimeZoneType(timestampPrecision).getObjectValue(singlePage.getBlock(i), 0);
            return Timestamp.fromEpochMillis(sqlTimestampWithTimeZone.getEpochMillis(), sqlTimestampWithTimeZone.getPicosOfMilli() * 1000);
        }
        else {
            SqlTimestamp sqlTimestamp = (SqlTimestamp) TimestampType.createTimestampType(timestampPrecision).getObjectValue(singlePage.getBlock(i), 0);
            return Timestamp.fromLocalDateTime(sqlTimestamp.toLocalDateTime());
        }
    }

    @Override
    public byte[] getBinary(int i)
    {
        Slice slice = (Slice) readNativeValue(VARBINARY, singlePage.getBlock(i), 0);
        return slice.getBytes();
    }

    @Override
    public Variant getVariant(int i)
    {
        throw new TrinoException(NOT_SUPPORTED, "variant type is not supported.");
    }

    @Override
    public InternalArray getArray(int i)
    {
        // todo
        throw new TrinoException(NOT_SUPPORTED, "array type is not yet supported.");
    }

    @Override
    public InternalMap getMap(int i)
    {
        // todo
        throw new TrinoException(NOT_SUPPORTED, "map type is not yet supported.");
    }

    @Override
    public InternalRow getRow(int i, int i1)
    {
        // todo
        throw new TrinoException(NOT_SUPPORTED, "row type is not yet supported.");
    }
}
