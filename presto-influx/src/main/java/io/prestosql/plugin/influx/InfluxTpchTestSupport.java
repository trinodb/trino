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
package io.prestosql.plugin.influx;

import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;

public class InfluxTpchTestSupport
{
    public static final String TEST_DATABASE = "_presto_influx_test_db";
    public static final String TPCH_SCHEMA = "tpch";

    private InfluxTpchTestSupport()
    {
    }

    public static List<InfluxColumn> getColumns(String databaseName, String schemaName, String tableName)
    {
        if (TEST_DATABASE.equals(databaseName) && TPCH_SCHEMA.equals(schemaName)) {
            switch (tableName) {
                case "customer":
                    return Arrays.asList(
                            time(),
                            key("custkey"),
                            varchar("name", 25),
                            varchar("address", 40),
                            key("nationkey"),
                            varchar("phone", 15),
                            real("acctbal"),
                            varchar("mktsegment", 10),
                            varchar("comment", 117));
                case "orders":
                    return Arrays.asList(
                            time(),
                            key("orderkey"),
                            key("custkey"),
                            varchar("orderstatus", 1),
                            real("totalprice"),
                            date("orderdate"),
                            varchar("orderpriority", 15),
                            varchar("clerk", 15),
                            integer("shippriority"),
                            varchar("comment", 79));
                case "region":
                    return Arrays.asList(
                            time(),
                            key("regionkey"),
                            varchar("name", 25),
                            varchar("comment", 152));
                case "nation":
                    return Arrays.asList(
                            time(),
                            key("nationkey"),
                            varchar("name", 25),
                            key("regionkey"),
                            varchar("comment", 152));
                case "lineitem":
                    return Arrays.asList(
                            time(),
                            key("orderkey"),
                            key("partkey"),
                            key("suppkey"),
                            integer("linenumber"),
                            real("quantity"),
                            real("extendedprice"),
                            real("discount"),
                            real("tax"),
                            varchar("returnflag", 1),
                            varchar("linestatus", 1),
                            date("shipdate"),
                            date("commitdate"),
                            date("receiptdate"),
                            varchar("shipinstruct", 25),
                            varchar("shipmode", 10),
                            varchar("comment", 44));
                default:
                    throw new UnsupportedOperationException("table " + tableName + " not supported");
            }
        }
        return null;
    }

    private static InfluxColumn time()
    {
        return new InfluxColumn("time", "time", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, InfluxColumn.Kind.TIME, true);
    }

    private static InfluxColumn key(String name)
    {
        return new InfluxColumn(name, "integer", BigintType.BIGINT, InfluxColumn.Kind.FIELD, false);
    }

    private static InfluxColumn varchar(String name, int length)
    {
        return new InfluxColumn(name, "string", VarcharType.createVarcharType(length), InfluxColumn.Kind.FIELD, false);
    }

    private static InfluxColumn bigint(String name)
    {
        return new InfluxColumn(name, "integer", BigintType.BIGINT, InfluxColumn.Kind.FIELD, false);
    }

    private static InfluxColumn integer(String name)
    {
        return new InfluxColumn(name, "integer", IntegerType.INTEGER, InfluxColumn.Kind.FIELD, false);
    }

    private static InfluxColumn date(String name)
    {
        return new InfluxColumn(name, "integer", DateType.DATE, InfluxColumn.Kind.FIELD, false);
    }

    private static InfluxColumn real(String name)
    {
        return new InfluxColumn(name, "float", DoubleType.DOUBLE, InfluxColumn.Kind.FIELD, false);
    }
}
