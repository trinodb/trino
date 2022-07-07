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

import io.trino.plugin.cassandra.CassandraTypeMapping.Kind;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;

import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;

public final class CassandraTypes
{
    private CassandraTypes() {}

    // IPv4: 255.255.255.255 - 15 characters
    // IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:FFFF - 39 characters
    // IPv4 embedded into IPv6: FFFF:FFFF:FFFF:FFFF:FFFF:FFFF:255.255.255.255 - 45 characters
    private static final int IP_ADDRESS_STRING_MAX_LENGTH = 45;

    public static final CassandraTypeMapping ASCII = new CassandraTypeMapping(Kind.ASCII, createUnboundedVarcharType());
    public static final CassandraTypeMapping BIGINT = new CassandraTypeMapping(Kind.BIGINT, BigintType.BIGINT);
    public static final CassandraTypeMapping BLOB = new CassandraTypeMapping(Kind.BLOB, VarbinaryType.VARBINARY);
    public static final CassandraTypeMapping BOOLEAN = new CassandraTypeMapping(Kind.BOOLEAN, BooleanType.BOOLEAN);
    public static final CassandraTypeMapping COUNTER = new CassandraTypeMapping(Kind.COUNTER, BigintType.BIGINT);
    public static final CassandraTypeMapping CUSTOM = new CassandraTypeMapping(Kind.CUSTOM, VarbinaryType.VARBINARY);
    public static final CassandraTypeMapping DATE = new CassandraTypeMapping(Kind.DATE, DateType.DATE);
    public static final CassandraTypeMapping DECIMAL = new CassandraTypeMapping(Kind.DECIMAL, DoubleType.DOUBLE);
    public static final CassandraTypeMapping DOUBLE = new CassandraTypeMapping(Kind.DOUBLE, DoubleType.DOUBLE);
    public static final CassandraTypeMapping FLOAT = new CassandraTypeMapping(Kind.FLOAT, RealType.REAL);
    public static final CassandraTypeMapping INET = new CassandraTypeMapping(Kind.INET, createVarcharType(IP_ADDRESS_STRING_MAX_LENGTH));
    public static final CassandraTypeMapping INT = new CassandraTypeMapping(Kind.INT, IntegerType.INTEGER);
    public static final CassandraTypeMapping LIST = new CassandraTypeMapping(Kind.LIST, createUnboundedVarcharType());
    public static final CassandraTypeMapping MAP = new CassandraTypeMapping(Kind.MAP, createUnboundedVarcharType());
    public static final CassandraTypeMapping SET = new CassandraTypeMapping(Kind.SET, createUnboundedVarcharType());
    public static final CassandraTypeMapping SMALLINT = new CassandraTypeMapping(Kind.SMALLINT, SmallintType.SMALLINT);
    public static final CassandraTypeMapping TEXT = new CassandraTypeMapping(Kind.TEXT, createUnboundedVarcharType());
    public static final CassandraTypeMapping TIMESTAMP = new CassandraTypeMapping(Kind.TIMESTAMP, TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS);
    public static final CassandraTypeMapping TIMEUUID = new CassandraTypeMapping(Kind.TIMEUUID, UuidType.UUID);
    public static final CassandraTypeMapping TINYINT = new CassandraTypeMapping(Kind.TINYINT, TinyintType.TINYINT);
    public static final CassandraTypeMapping UUID = new CassandraTypeMapping(Kind.UUID, UuidType.UUID);
    public static final CassandraTypeMapping VARCHAR = new CassandraTypeMapping(Kind.VARCHAR, createUnboundedVarcharType());
    public static final CassandraTypeMapping VARINT = new CassandraTypeMapping(Kind.VARINT, createUnboundedVarcharType());
}
