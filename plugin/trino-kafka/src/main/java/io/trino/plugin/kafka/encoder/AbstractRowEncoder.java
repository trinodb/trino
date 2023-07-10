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
package io.trino.plugin.kafka.encoder;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractRowEncoder
        implements RowEncoder
{
    protected final ConnectorSession session;
    protected final List<EncoderColumnHandle> columnHandles;

    /**
     * The current column index for appending values to the row encoder.
     * Gets incremented by appendColumnValue and set back to zero when the encoder is reset.
     */
    protected int currentColumnIndex;

    protected AbstractRowEncoder(ConnectorSession session, List<EncoderColumnHandle> columnHandles)
    {
        this.session = requireNonNull(session, "session is null");
        requireNonNull(columnHandles, "columnHandles is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.currentColumnIndex = 0;
    }

    @Override
    public void appendColumnValue(Block block, int position)
    {
        checkArgument(currentColumnIndex < columnHandles.size(), format("currentColumnIndex '%d' is greater than number of columns '%d'", currentColumnIndex, columnHandles.size()));
        Type type = columnHandles.get(currentColumnIndex).getType();
        if (block.isNull(position)) {
            appendNullValue();
        }
        else if (type == BOOLEAN) {
            appendBoolean(BOOLEAN.getBoolean(block, position));
        }
        else if (type == BIGINT) {
            appendLong(BIGINT.getLong(block, position));
        }
        else if (type == INTEGER) {
            appendInt(INTEGER.getInt(block, position));
        }
        else if (type == SMALLINT) {
            appendShort(SMALLINT.getShort(block, position));
        }
        else if (type == TINYINT) {
            appendByte(TINYINT.getByte(block, position));
        }
        else if (type == DOUBLE) {
            appendDouble(DOUBLE.getDouble(block, position));
        }
        else if (type == REAL) {
            appendFloat(REAL.getFloat(block, position));
        }
        else if (type instanceof VarcharType varcharType) {
            appendString(varcharType.getSlice(block, position).toStringUtf8());
        }
        else if (type instanceof VarbinaryType varbinaryType) {
            appendByteBuffer(varbinaryType.getSlice(block, position).toByteBuffer());
        }
        else if (type == DATE) {
            appendSqlDate((SqlDate) DATE.getObjectValue(session, block, position));
        }
        else if (type instanceof TimeType) {
            appendSqlTime((SqlTime) type.getObjectValue(session, block, position));
        }
        else if (type instanceof TimeWithTimeZoneType) {
            appendSqlTimeWithTimeZone((SqlTimeWithTimeZone) type.getObjectValue(session, block, position));
        }
        else if (type instanceof TimestampType) {
            appendSqlTimestamp((SqlTimestamp) type.getObjectValue(session, block, position));
        }
        else if (type instanceof TimestampWithTimeZoneType) {
            appendSqlTimestampWithTimeZone((SqlTimestampWithTimeZone) type.getObjectValue(session, block, position));
        }
        else if (type instanceof ArrayType) {
            appendArray((List<Object>) type.getObjectValue(session, block, position));
        }
        else if (type instanceof MapType) {
            appendMap((Map<Object, Object>) type.getObjectValue(session, block, position));
        }
        else if (type instanceof RowType) {
            appendRow((List<Object>) type.getObjectValue(session, block, position));
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", type, columnHandles.get(currentColumnIndex).getName()));
        }
        currentColumnIndex++;
    }

    // these append value methods should be overridden for each row encoder
    // only the methods with types supported by the data format should be overridden
    protected void appendNullValue()
    {
        throw new UnsupportedOperationException(format("Column '%s' does not support 'null' value", columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendLong(long value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", long.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendInt(int value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", int.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendShort(short value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", short.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendByte(byte value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", byte.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendDouble(double value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", double.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendFloat(float value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", float.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendBoolean(boolean value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", boolean.class.getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendString(String value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendByteBuffer(ByteBuffer value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlDate(SqlDate value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTime(SqlTime value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTimeWithTimeZone(SqlTimeWithTimeZone value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTimestamp(SqlTimestamp value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendSqlTimestampWithTimeZone(SqlTimestampWithTimeZone value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendArray(List<Object> value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendMap(Map<Object, Object> value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void appendRow(List<Object> value)
    {
        throw new UnsupportedOperationException(format("Unsupported type '%s' for column '%s'", value.getClass().getName(), columnHandles.get(currentColumnIndex).getName()));
    }

    protected void resetColumnIndex()
    {
        currentColumnIndex = 0;
    }

    @Override
    public void close()
    {
        // do nothing
    }
}
