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
package io.trino.plugin.accumulo.model;

import com.google.common.primitives.Primitives;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.plugin.accumulo.Types;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Time;
import java.sql.Timestamp;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class Field
{
    private final Object value;
    private final Type type;
    private final boolean indexed;

    public Field(Object nativeValue, Type type)
    {
        this(nativeValue, type, false);
    }

    public Field(Object nativeValue, Type type, boolean indexed)
    {
        this.value = convert(nativeValue, type);
        this.type = requireNonNull(type, "type is null");
        this.indexed = indexed;
    }

    public Type getType()
    {
        return type;
    }

    public Block getArray()
    {
        return (Block) value;
    }

    public Long getLong()
    {
        return (Long) value;
    }

    public Boolean getBoolean()
    {
        return (Boolean) value;
    }

    public Byte getByte()
    {
        return (Byte) value;
    }

    public long getDate()
    {
        return (Long) value;
    }

    public Double getDouble()
    {
        return (Double) value;
    }

    public Float getFloat()
    {
        return (Float) value;
    }

    public Integer getInt()
    {
        return (Integer) value;
    }

    public SqlMap getMap()
    {
        return (SqlMap) value;
    }

    public Object getObject()
    {
        return value;
    }

    public Short getShort()
    {
        return (Short) value;
    }

    public Timestamp getTimestamp()
    {
        return (Timestamp) value;
    }

    public Time getTime()
    {
        return (Time) value;
    }

    public byte[] getVarbinary()
    {
        return (byte[]) value;
    }

    public String getVarchar()
    {
        return (String) value;
    }

    public boolean isIndexed()
    {
        return indexed;
    }

    public boolean isNull()
    {
        return value == null;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("value", value)
                .add("type", type)
                .toString();
    }

    /**
     * Convert Trino native value (stack representation) of given type to Accumulo equivalent.
     *
     * @param value Object to convert
     * @param type Destination Trino type
     */
    private static Object convert(Object value, Type type)
    {
        if (value == null) {
            return null;
        }

        checkArgument(Primitives.wrap(type.getJavaType()).isInstance(value), "Invalid representation for %s: %s [%s]", type, value, value.getClass().getName());

        // Array? Better be a block!
        if (Types.isArrayType(type)) {
            // Block
            return value;
        }

        // Map? Better be a block!
        if (Types.isMapType(type)) {
            // Block
            return value;
        }

        // And now for the plain types
        if (type.equals(BIGINT)) {
            // long
            return value;
        }

        if (type.equals(INTEGER)) {
            return toIntExact((long) value);
        }

        if (type.equals(BOOLEAN)) {
            // boolean
            return value;
        }

        if (type.equals(DATE)) {
            // long
            return value;
        }

        if (type.equals(DOUBLE)) {
            // double
            return value;
        }

        if (type.equals(REAL)) {
            return intBitsToFloat(toIntExact((long) value));
        }

        if (type.equals(SMALLINT)) {
            return Shorts.checkedCast((long) value);
        }

        if (type.equals(TIME_MILLIS)) {
            return new Time((long) value);
        }

        if (type.equals(TIMESTAMP_MILLIS)) {
            return new Timestamp(floorDiv((Long) value, MICROSECONDS_PER_MILLISECOND));
        }

        if (type.equals(TINYINT)) {
            return SignedBytes.checkedCast((long) value);
        }

        if (type.equals(VARBINARY)) {
            return ((Slice) value).getBytes();
        }

        if (type instanceof VarcharType) {
            return ((Slice) value).toStringUtf8();
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported Trino type: " + type);
    }
}
