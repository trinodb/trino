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
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.VarbinaryType;

import java.util.UUID;

import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;

public final class CassandraTypes
{
    private CassandraTypes() {}

    private static final int UUID_STRING_MAX_LENGTH = 36;
    // IPv4: 255.255.255.255 - 15 characters
    // IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF - 39 characters
    // IPv4 embedded into IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:255.255.255.255 - 45 characters
    private static final int IP_ADDRESS_STRING_MAX_LENGTH = 45;

    public static final CassandraType ASCII = new CassandraType(Kind.ASCII, createUnboundedVarcharType());
    public static final CassandraType BIGINT = new CassandraType(Kind.BIGINT, BigintType.BIGINT);
    public static final CassandraType BLOB = new CassandraType(Kind.BLOB, VarbinaryType.VARBINARY);
    public static final CassandraType BOOLEAN = new CassandraType(Kind.BOOLEAN, BooleanType.BOOLEAN);
    public static final CassandraType COUNTER = new CassandraType(Kind.COUNTER, BigintType.BIGINT);
    public static final CassandraType CUSTOM = new CassandraType(Kind.CUSTOM, VarbinaryType.VARBINARY);
    public static final CassandraType DATE = new CassandraType(Kind.DATE, DateType.DATE);
    public static final CassandraType DECIMAL = new CassandraType(Kind.DECIMAL, DoubleType.DOUBLE);
    public static final CassandraType DOUBLE = new CassandraType(Kind.DOUBLE, DoubleType.DOUBLE);
    public static final CassandraType FLOAT = new CassandraType(Kind.FLOAT, RealType.REAL);
    public static final CassandraType INET = new CassandraType(Kind.INET, createVarcharType(IP_ADDRESS_STRING_MAX_LENGTH));
    public static final CassandraType INT = new CassandraType(Kind.INT, IntegerType.INTEGER);
    public static final CassandraType LIST = new CassandraType(Kind.LIST, createUnboundedVarcharType());
    public static final CassandraType MAP = new CassandraType(Kind.MAP, createUnboundedVarcharType());
    public static final CassandraType SET = new CassandraType(Kind.SET, createUnboundedVarcharType());
    public static final CassandraType SMALLINT = new CassandraType(Kind.SMALLINT, SmallintType.SMALLINT);
    public static final CassandraType TEXT = new CassandraType(Kind.TEXT, createUnboundedVarcharType());
    public static final CassandraType TIMESTAMP = new CassandraType(Kind.TIMESTAMP, TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
    public static final CassandraType TIMEUUID = new CassandraType(Kind.TIMEUUID, createVarcharType(UUID_STRING_MAX_LENGTH));
    public static final CassandraType TINYINT = new CassandraType(Kind.TINYINT, TinyintType.TINYINT);
    public static final CassandraType UUID = new CassandraType(Kind.UUID, createVarcharType(UUID_STRING_MAX_LENGTH));
    public static final CassandraType VARCHAR = new CassandraType(Kind.VARCHAR, createUnboundedVarcharType());
    public static final CassandraType VARINT = new CassandraType(Kind.VARINT, createUnboundedVarcharType());
}
