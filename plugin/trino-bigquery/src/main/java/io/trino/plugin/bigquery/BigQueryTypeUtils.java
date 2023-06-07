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
package io.trino.plugin.bigquery;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.bigquery.BigQueryType.timestampToStringConverter;
import static io.trino.plugin.bigquery.BigQueryType.toZonedDateTime;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.readBigDecimal;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.ZoneOffset.UTC;
import static java.util.Collections.unmodifiableMap;

public final class BigQueryTypeUtils
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSS");

    private BigQueryTypeUtils() {}

    @Nullable
    public static Object readNativeValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        // TODO https://github.com/trinodb/trino/issues/13741 Add support for time, geography, map type
        if (type.equals(BOOLEAN)) {
            return BOOLEAN.getBoolean(block, position);
        }
        if (type.equals(TINYINT)) {
            return TINYINT.getByte(block, position);
        }
        if (type.equals(SMALLINT)) {
            return SMALLINT.getShort(block, position);
        }
        if (type.equals(INTEGER)) {
            return INTEGER.getInt(block, position);
        }
        if (type.equals(BIGINT)) {
            return BIGINT.getLong(block, position);
        }
        if (type.equals(DOUBLE)) {
            return DOUBLE.getDouble(block, position);
        }
        if (type instanceof DecimalType) {
            return readBigDecimal((DecimalType) type, block, position).toString();
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(VARBINARY)) {
            return Base64.getEncoder().encodeToString(VARBINARY.getSlice(block, position).getBytes());
        }
        if (type.equals(DATE)) {
            int days = DATE.getInt(block, position);
            return DATE_FORMATTER.format(LocalDate.ofEpochDay(days));
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            long epochMicros = TIMESTAMP_MICROS.getLong(block, position);
            long epochSeconds = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanoAdjustment = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            return DATETIME_FORMATTER.format(toZonedDateTime(epochSeconds, nanoAdjustment, UTC));
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(block, position);
            return timestampToStringConverter(timestamp);
        }
        if (type instanceof ArrayType arrayType) {
            Block arrayBlock = block.getObject(position, Block.class);
            ImmutableList.Builder<Object> list = ImmutableList.builderWithExpectedSize(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = readNativeValue(arrayType.getElementType(), arrayBlock, i);
                if (element == null) {
                    throw new TrinoException(NOT_SUPPORTED, "BigQuery does not support null elements in arrays");
                }
                list.add(element);
            }
            return list.build();
        }
        if (type instanceof RowType rowType) {
            Block rowBlock = block.getObject(position, Block.class);

            List<Type> fieldTypes = rowType.getTypeParameters();
            if (fieldTypes.size() != rowBlock.getPositionCount()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            }

            Map<String, Object> rowValue = new HashMap<>();
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                String fieldName = rowType.getFields().get(i).getName().orElseThrow(() -> new IllegalArgumentException("Field name must exist in BigQuery"));
                Object fieldValue = readNativeValue(fieldTypes.get(i), rowBlock, i);
                rowValue.put(fieldName, fieldValue);
            }
            return unmodifiableMap(rowValue);
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
    }
}
