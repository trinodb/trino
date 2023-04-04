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
package io.trino.plugin.deltalake.util;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.trino.spi.Page;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTimeZone;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.io.BaseEncoding.base16;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static io.trino.plugin.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveWriteUtils.getHiveDecimal;
import static io.trino.plugin.hive.util.HiveWriteUtils.getHiveTimestamp;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

public final class DeltaLakeWriteUtils
{
    private DeltaLakeWriteUtils() {}

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            Object value = getField(DateTimeZone.UTC, partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
            if (value == null) {
                partitionValues.add(HIVE_DEFAULT_DYNAMIC_PARTITION);
            }
            else {
                String valueString = value.toString();
                if (!CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(valueString)) {
                    throw new TrinoException(HIVE_INVALID_PARTITION_VALUE,
                            "Hive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: " +
                                    base16().withSeparator(" ", 2).encode(valueString.getBytes(UTF_8)));
                }
                partitionValues.add(valueString);
            }
        }
        return partitionValues.build();
    }

    public static Object getField(DateTimeZone localZone, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        if (BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        if (INTEGER.equals(type)) {
            return toIntExact(type.getLong(block, position));
        }
        if (SMALLINT.equals(type)) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        if (TINYINT.equals(type)) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        if (REAL.equals(type)) {
            return intBitsToFloat((int) type.getLong(block, position));
        }
        if (DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return new Text(type.getSlice(block, position).getBytes());
        }
        if (type instanceof CharType charType) {
            return new Text(padSpaces(type.getSlice(block, position), charType).toStringUtf8());
        }
        if (VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        if (DATE.equals(type)) {
            return Date.ofEpochDay(toIntExact(type.getLong(block, position)));
        }
        if (type instanceof TimestampType) {
            return getHiveTimestamp(localZone, (TimestampType) type, block, position);
        }
        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType && timestampWithTimeZoneType.getPrecision() == 3) {
            return getTimestampTZ(timestampWithTimeZoneType, block, position);
        }
        if (type instanceof DecimalType decimalType) {
            return getHiveDecimal(decimalType, block, position);
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                list.add(getField(localZone, elementType, arrayBlock, i));
            }
            return unmodifiableList(list);
        }
        if (type instanceof MapType) {
            Type keyType = ((MapType) type).getKeyType();
            Type valueType = ((MapType) type).getValueType();
            Block mapBlock = block.getObject(position, Block.class);

            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                map.put(
                        getField(localZone, keyType, mapBlock, i),
                        getField(localZone, valueType, mapBlock, i + 1));
            }
            return unmodifiableMap(map);
        }
        if (type instanceof RowType) {
            List<Type> fieldTypes = type.getTypeParameters();
            Block rowBlock = block.getObject(position, Block.class);
            checkCondition(
                    fieldTypes.size() == rowBlock.getPositionCount(),
                    StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "Expected row value field count does not match type field count");
            List<Object> row = new ArrayList<>(rowBlock.getPositionCount());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                row.add(getField(localZone, fieldTypes.get(i), rowBlock, i));
            }
            return unmodifiableList(row);
        }
        throw new TrinoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    private static TimestampTZ getTimestampTZ(TimestampWithTimeZoneType type, Block block, int position)
    {
        long value = block.getLong(position, 0);
        long millisUtc = unpackMillisUtc(value);
        if (type.isShort()) {
            Instant instant = Instant.ofEpochMilli(millisUtc);
            return new TimestampTZ((long) Math.floor(millisUtc / 1000), instant.getNano(), unpackZoneKey(value).getZoneId());
        }
        else {
            int picosOfMilli = block.getInt(position, SIZE_OF_LONG);
            Instant instant = Instant.ofEpochMilli(millisUtc).plusNanos(picosOfMilli * 1000);
            return new TimestampTZ(instant.getEpochSecond(), instant.getNano(), unpackZoneKey(value).getZoneId());
        }
    }
}
