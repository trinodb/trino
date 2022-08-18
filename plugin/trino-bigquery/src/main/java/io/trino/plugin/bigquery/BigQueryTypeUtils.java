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

import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import javax.annotation.Nullable;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Base64;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static java.lang.Math.toIntExact;

public final class BigQueryTypeUtils
{
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    private BigQueryTypeUtils() {}

    @Nullable
    public static Object readNativeValue(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        // TODO https://github.com/trinodb/trino/issues/13741 Add support for decimal, time, timestamp, timestamp with time zone, geography, array, map, row type
        if (type.equals(BOOLEAN)) {
            return type.getBoolean(block, position);
        }
        if (type.equals(TINYINT)) {
            return SignedBytes.checkedCast(type.getLong(block, position));
        }
        if (type.equals(SMALLINT)) {
            return Shorts.checkedCast(type.getLong(block, position));
        }
        if (type.equals(INTEGER)) {
            return toIntExact(type.getLong(block, position));
        }
        if (type.equals(BIGINT)) {
            return type.getLong(block, position);
        }
        if (type.equals(DOUBLE)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (type.equals(VARBINARY)) {
            return Base64.getEncoder().encodeToString(type.getSlice(block, position).getBytes());
        }
        if (type.equals(DATE)) {
            long days = type.getLong(block, position);
            return DATE_FORMATTER.format(LocalDate.ofEpochDay(days));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
    }
}
