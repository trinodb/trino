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
package io.trino.spi.type;

// Should have the same types as in the ClientStandardTypes
public final class StandardTypes
{
    public static final String BIGINT = BigintType.NAME;
    public static final String INTEGER = IntegerType.NAME;
    public static final String SMALLINT = SmallintType.NAME;
    public static final String TINYINT = TinyintType.NAME;
    public static final String BOOLEAN = BooleanType.NAME;
    public static final String DATE = DateType.NAME;
    public static final String DECIMAL = DecimalType.NAME;
    public static final String REAL = RealType.NAME;
    public static final String DOUBLE = DoubleType.NAME;
    public static final String HYPER_LOG_LOG = HyperLogLogType.NAME;
    public static final String QDIGEST = QuantileDigestType.NAME;
    public static final String TDIGEST = "tdigest";
    public static final String SET_DIGEST = "SetDigest";
    public static final String P4_HYPER_LOG_LOG = P4HyperLogLogType.NAME;
    public static final String INTERVAL_DAY_TO_SECOND = "interval day to second";
    public static final String INTERVAL_YEAR_TO_MONTH = "interval year to month";
    public static final String TIMESTAMP = TimestampType.NAME;
    public static final String TIMESTAMP_WITH_TIME_ZONE = TimestampWithTimeZoneType.NAME;
    public static final String TIME = TimeType.NAME;
    public static final String TIME_WITH_TIME_ZONE = TimeWithTimeZoneType.NAME;
    public static final String VARBINARY = VarbinaryType.NAME;
    public static final String VARCHAR = VarcharType.NAME;
    public static final String CHAR = CharType.NAME;
    public static final String ROW = RowType.NAME;
    public static final String ARRAY = ArrayType.NAME;
    public static final String MAP = MapType.NAME;
    public static final String JSON = "json";
    public static final String JSON_2016 = "json2016";
    public static final String IPADDRESS = "ipaddress";
    public static final String UUID = "uuid";
    public static final String GEOMETRY = "Geometry";
    public static final String SPHERICAL_GEOGRAPHY = "SphericalGeography"; // SphericalGeographyType.NAME
    public static final String BING_TILE = "BingTile"; // BingTileType.NAME
    public static final String KDB_TREE = "KdbTree"; // KdbTreeType.NAME
    public static final String COLOR = "color"; // Color.NAME

    private StandardTypes() {}
}
