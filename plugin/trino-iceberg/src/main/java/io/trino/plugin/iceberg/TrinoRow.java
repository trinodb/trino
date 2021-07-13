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
package io.trino.plugin.iceberg;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.iceberg.StructLike;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.util.Timestamps.getTimestampTz;
import static io.trino.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class TrinoRow
        implements StructLike
{
    private final List<Type> types;
    private final Block[] blocks;
    private final int position;

    public TrinoRow(List<Type> types, Block[] blocks, int position)
    {
        this.types = requireNonNull(types, "types list is null");
        this.blocks = requireNonNull(blocks, "blocks array is null");
        checkArgument(position >= 0, "page position must be positive: %s", position);
        this.position = position;
    }

    public int getPosition()
    {
        return position;
    }

    @Override
    public int size()
    {
        return blocks.length;
    }

    @Override
    public <T> T get(int i, Class<T> aClass)
    {
        Block block = blocks[i].getLoadedBlock();
        Type type = types.get(i);
        T value;
        // TODO: can refactor with IcebergPageSink.getIcebergValue
        if (block.isNull(position)) {
            value = null;
        }
        else if (type instanceof BigintType) {
            value = aClass.cast(type.getLong(block, position));
        }
        else if (type instanceof IntegerType || type instanceof SmallintType || type instanceof TinyintType || type instanceof DateType) {
            value = aClass.cast(toIntExact(type.getLong(block, position)));
        }
        else if (type instanceof BooleanType) {
            value = aClass.cast(type.getBoolean(block, position));
        }
        else if (type instanceof DecimalType) {
            value = aClass.cast(readBigDecimal((DecimalType) type, block, position));
        }
        else if (type instanceof RealType) {
            value = aClass.cast(intBitsToFloat(toIntExact(type.getLong(block, position))));
        }
        else if (type instanceof DoubleType) {
            value = aClass.cast(type.getDouble(block, position));
        }
        else if (type.equals(TIME_MICROS)) {
            value = aClass.cast(type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND);
        }
        else if (type.equals(TIMESTAMP_MICROS)) {
            value = aClass.cast(type.getLong(block, position));
        }
        else if (type.equals(TIMESTAMP_TZ_MICROS)) {
            value = aClass.cast(timestampTzToMicros(getTimestampTz(block, position)));
        }
        else if (type instanceof VarbinaryType) {
            value = aClass.cast(type.getSlice(block, position).getBytes());
        }
        else if (type instanceof VarcharType) {
            value = aClass.cast(type.getSlice(block, position).toStringUtf8());
        }
        else {
            // will most likely throw unsupported exception
            value = block.getObject(position, aClass);
        }

        return value;
    }

    @Override
    public <T> void set(int i, T t)
    {
        throw new TrinoException(NOT_SUPPORTED, "writing to TrinoRow is not supported");
    }
}
