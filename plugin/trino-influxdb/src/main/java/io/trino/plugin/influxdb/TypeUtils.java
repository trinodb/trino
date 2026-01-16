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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Set;

public final class TypeUtils
{
    public static final String BOOLEAN = "boolean";
    public static final String STRING = "string";
    public static final String INTEGER = "integer";
    public static final String FLOAT = "float";
    public static final String TIMESTAMP = "timestamp";

    private static final Set<Type> supportedTypes = ImmutableSet.of(
            BooleanType.BOOLEAN,
            VarcharType.VARCHAR,
            BigintType.BIGINT,
            DoubleType.DOUBLE);

    private TypeUtils() {}

    public static Type toTrinoType(String influxType)
    {
        return switch (influxType) {
            case TIMESTAMP -> TimestampType.TIMESTAMP_NANOS;
            case BOOLEAN -> BooleanType.BOOLEAN;
            case STRING -> VarcharType.VARCHAR;
            case INTEGER -> BigintType.BIGINT;
            case FLOAT -> DoubleType.DOUBLE;
            default -> throw new RuntimeException("type is not supported : " + influxType);
        };
    }

    public static boolean isPushdownSupportedType(Type type)
    {
        return type instanceof TimestampType
                || supportedTypes.contains(type);
    }
}
