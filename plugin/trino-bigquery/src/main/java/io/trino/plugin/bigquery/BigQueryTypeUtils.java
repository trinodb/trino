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

import com.google.protobuf.ByteString;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static io.trino.plugin.bigquery.BigQueryTypeManager.datetimeToStringConverter;
import static io.trino.plugin.bigquery.BigQueryTypeManager.timestampToStringConverter;
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
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public final class BigQueryTypeUtils
{
    private static final long MIN_SUPPORTED_DATE = LocalDate.parse("0001-01-01").toEpochDay();
    private static final long MAX_SUPPORTED_DATE = LocalDate.parse("9999-12-31").toEpochDay();

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

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
            return type.getLong(block, position);
        }
        if (type.equals(SMALLINT)) {
            return SMALLINT.getLong(block, position);
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
        if (type instanceof DecimalType decimalType) {
            return readBigDecimal(decimalType, block, position).toString();
        }
        if (type instanceof VarcharType varcharType) {
            return varcharType.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(VARBINARY)) {
            return ByteString.copyFrom(VARBINARY.getSlice(block, position).getBytes());
        }
        if (type.equals(DATE)) {
            int days = DATE.getInt(block, position);
            if (days < MIN_SUPPORTED_DATE || days > MAX_SUPPORTED_DATE) {
                throw new TrinoException(NOT_SUPPORTED, "BigQuery supports dates between 0001-01-01 and 9999-12-31 but got " + LocalDate.ofEpochDay(days));
            }
            return DATE_FORMATTER.format(LocalDate.ofEpochDay(days));
        }
        if (type.equals(TIMESTAMP_MICROS)) {
            long epochMicros = TIMESTAMP_MICROS.getLong(block, position);
            return datetimeToStringConverter(epochMicros);
        }
        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) TIMESTAMP_TZ_MICROS.getObject(block, position);
            return timestampToStringConverter(timestamp);
        }
        if (type instanceof ArrayType arrayType) {
            Block arrayBlock = arrayType.getObject(block, position);
            JSONArray list = new JSONArray();
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = readNativeValue(arrayType.getElementType(), arrayBlock, i);
                if (element == null) {
                    throw new TrinoException(NOT_SUPPORTED, "BigQuery does not support null elements in arrays");
                }
                list.put(element);
            }
            return list;
        }
        if (type instanceof RowType rowType) {
            SqlRow sqlRow = rowType.getObject(block, position);

            List<Type> fieldTypes = rowType.getTypeParameters();
            if (fieldTypes.size() != sqlRow.getFieldCount()) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");
            }

            int rawIndex = sqlRow.getRawIndex();
            JSONObject rowValue = new JSONObject();
            for (int fieldIndex = 0; fieldIndex < sqlRow.getFieldCount(); fieldIndex++) {
                String fieldName = rowType.getFields().get(fieldIndex).getName().orElseThrow(() -> new IllegalArgumentException("Field name must exist in BigQuery"));
                Object fieldValue = readNativeValue(fieldTypes.get(fieldIndex), sqlRow.getRawFieldBlock(fieldIndex), rawIndex);
                rowValue.put(fieldName, fieldValue);
            }
            return rowValue;
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
    }
}
