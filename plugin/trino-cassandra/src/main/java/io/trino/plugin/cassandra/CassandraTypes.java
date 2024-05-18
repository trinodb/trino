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
package io.trino.plugin.cassandra;

import io.trino.plugin.cassandra.CassandraType.Kind;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;

import static io.trino.plugin.cassandra.CassandraType.primitiveType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public final class CassandraTypes
{
    private CassandraTypes() {}

    public static final CassandraType ASCII = primitiveType(Kind.ASCII, createUnboundedVarcharType());
    public static final CassandraType BIGINT = primitiveType(Kind.BIGINT, BigintType.BIGINT);
    public static final CassandraType BLOB = primitiveType(Kind.BLOB, VarbinaryType.VARBINARY);
    public static final CassandraType BOOLEAN = primitiveType(Kind.BOOLEAN, BooleanType.BOOLEAN);
    public static final CassandraType COUNTER = primitiveType(Kind.COUNTER, BigintType.BIGINT);
    public static final CassandraType CUSTOM = primitiveType(Kind.CUSTOM, VarbinaryType.VARBINARY);
    public static final CassandraType DATE = primitiveType(Kind.DATE, DateType.DATE);
    public static final CassandraType DECIMAL = primitiveType(Kind.DECIMAL, DoubleType.DOUBLE);
    public static final CassandraType DOUBLE = primitiveType(Kind.DOUBLE, DoubleType.DOUBLE);
    public static final CassandraType FLOAT = primitiveType(Kind.FLOAT, RealType.REAL);
    public static final CassandraType INT = primitiveType(Kind.INT, IntegerType.INTEGER);
    public static final CassandraType LIST = primitiveType(Kind.LIST, createUnboundedVarcharType());
    public static final CassandraType MAP = primitiveType(Kind.MAP, createUnboundedVarcharType());
    public static final CassandraType SET = primitiveType(Kind.SET, createUnboundedVarcharType());
    public static final CassandraType SMALLINT = primitiveType(Kind.SMALLINT, SmallintType.SMALLINT);
    public static final CassandraType TEXT = primitiveType(Kind.TEXT, createUnboundedVarcharType());
    public static final CassandraType TIME = primitiveType(Kind.TIME, TimeType.TIME_NANOS);
    public static final CassandraType TIMESTAMP = primitiveType(Kind.TIMESTAMP, TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
    public static final CassandraType TIMEUUID = primitiveType(Kind.TIMEUUID, UuidType.UUID);
    public static final CassandraType TINYINT = primitiveType(Kind.TINYINT, TinyintType.TINYINT);
    public static final CassandraType UUID = primitiveType(Kind.UUID, UuidType.UUID);
    public static final CassandraType VARCHAR = primitiveType(Kind.VARCHAR, createUnboundedVarcharType());
    public static final CassandraType VARINT = primitiveType(Kind.VARINT, createUnboundedVarcharType());
}
