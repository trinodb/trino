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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;

public final class TypeUtils
{
    private static final Set<Type> PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            DATE,
            TIME_MILLIS,
            TIMESTAMP_MILLIS,
            TIMESTAMP_TZ_MILLIS);

    private TypeUtils() {}

    public static boolean isJsonType(Type type)
    {
        return type.getBaseName().equals(JSON);
    }

    public static boolean isArrayType(Type type)
    {
        return type instanceof ArrayType;
    }

    public static boolean isMapType(Type type)
    {
        return type instanceof MapType;
    }

    public static boolean isRowType(Type type)
    {
        return type instanceof RowType;
    }

    public static boolean isPushdownSupportedType(Type type)
    {
        return type instanceof VarcharType || type instanceof ObjectIdType || PUSHDOWN_SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }
}
