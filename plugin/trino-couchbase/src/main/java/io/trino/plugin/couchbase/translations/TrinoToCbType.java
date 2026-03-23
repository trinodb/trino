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
package io.trino.plugin.couchbase.translations;

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import jakarta.annotation.Nullable;

import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;

public final class TrinoToCbType
{
    private TrinoToCbType()
    {
    }

    private static final Set<Type> PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            REAL,
            DOUBLE,
            DATE,
            TIME_MILLIS,
            TIMESTAMP_MILLIS,
            TIMESTAMP_TZ_MILLIS);

    @Nullable
    public static Object serialize(Type type, Object value)
    {
        if (value instanceof Optional optional) {
            value = optional.orElse(null);
        }
        if (value == null) {
            return null;
        }
        if (type == DateType.DATE || type.equals(BigintType.BIGINT) || type.equals(DoubleType.DOUBLE)) {
            return value;
        }
        else if (type instanceof VarcharType) {
            Slice slice = (Slice) value;
            return slice.toStringUtf8();
        }
        else {
            throw new RuntimeException("Unsupported domain value type: " + type);
        }
    }

    public static boolean isPushdownSupportedType(Type type)
    {
        return type instanceof CharType
                || type instanceof VarcharType
                || type instanceof DecimalType
                || PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }
}
