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
package io.trino.plugin.iceberg.delete;

import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.StructLike;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;

public final class TrinoRow
        implements StructLike
{
    private final Object[] values;

    public TrinoRow(ConnectorSession session, Type[] types, SourcePage page, int position)
    {
        checkArgument(types.length == page.getChannelCount(), "mismatched types for page");
        values = new Object[types.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = getObjectValue(session, page.getBlock(i), position, types[i]);
        }
    }

    @Override
    public int size()
    {
        return values.length;
    }

    @Override
    public <T> T get(int i, Class<T> clazz)
    {
        return clazz.cast(values[i]);
    }

    @Override
    public <T> void set(int i, T t)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        return "TrinoRow" + Arrays.toString(values);
    }

    static Object getObjectValue(ConnectorSession session, Block block, int position, Type type)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(TINYINT)) {
            return (int) TINYINT.getByte(block, position);
        }
        if (type.equals(SMALLINT)) {
            return (int) SMALLINT.getShort(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        if (type.equals(DATE)) {
            return DATE.getInt(block, position);
        }
        if (type.equals(BOOLEAN)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (type instanceof DecimalType decimalType) {
            return readBigDecimal(decimalType, block, position);
        }
        if (type.equals(REAL)) {
            return REAL.getFloat(block, position);
        }
        if (type.equals(DOUBLE)) {
            return DOUBLE.getDouble(block, position);
        }
        if (type.equals(TIME_MICROS)) {
            return TIME_MICROS.getLong(block, position) / PICOSECONDS_PER_MICROSECOND;
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            return TIMESTAMP_MICROS.getLong(block, position);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros(getTimestampTz(block, position));
        }
        if (type instanceof VarbinaryType varbinaryType) {
            return varbinaryType.getSlice(block, position).toByteBuffer();
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(UUID)) {
            return trinoUuidToJavaUuid(UUID.getSlice(block, position));
        }
        if (type instanceof RowType rowType) {
            List<Object> values = (List<Object>) rowType.getObjectValue(session, block, position);
            return rowData(values.toArray());
        }
        throw new UnsupportedOperationException("Type not supported as equality delete column: " + type.getDisplayName());
    }

    public static StructLike rowData(Object... values)
    {
        return new StructLike() {
            @Override
            public int size()
            {
                return values.length;
            }

            @Override
            public <T> T get(int i, Class<T> aClass)
            {
                return aClass.cast(values[i]);
            }

            @Override
            public <T> void set(int i, T t)
            {
                throw new UnsupportedOperationException("Testing StructLike does not support setting values.");
            }
        };
    }
}
