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
package io.trino.plugin.druid;

import org.intellij.lang.annotations.Language;

import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.PART;
import static io.trino.tpch.TpchTable.REGION;

public final class DruidTpchTables
{
    @Language("SQL")
    public static final String SELECT_FROM_ORDERS = "SELECT " +
            "orderdate, " +
            "orderdate AS orderdate_druid_ts, " + // Druid stores the orderdate_druid_ts column as __time column.
            "orderkey, " +
            "custkey, " +
            "orderstatus, " +
            "totalprice, " +
            "orderpriority, " +
            "clerk, " +
            "shippriority, " +
            "comment " +
            "FROM tpch.tiny.orders";

    @Language("SQL")
    public static final String SELECT_FROM_LINEITEM = " SELECT " +
            "orderkey, " +
            "partkey, " +
            "suppkey, " +
            "linenumber, " +
            "quantity, " +
            "extendedprice, " +
            "discount, " +
            "tax, " +
            "returnflag, " +
            "linestatus, " +
            "shipdate, " +
            "shipdate AS shipdate_druid_ts, " +  // Druid stores the shipdate_druid_ts column as __time column.
            "commitdate, " +
            "receiptdate, " +
            "shipinstruct, " +
            "shipmode, " +
            "comment " +
            "FROM tpch.tiny.lineitem";

    @Language("SQL")
    public static final String SELECT_FROM_NATION = " SELECT " +
            "nationkey, " +
            "name, " +
            "regionkey, " +
            "comment, " +
            "'1995-01-02' AS nation_druid_dummy_ts " + // Dummy timestamp for Druid __time column
            "FROM tpch.tiny.nation";

    @Language("SQL")
    public static final String SELECT_FROM_REGION = " SELECT " +
            "regionkey, " +
            "name, " +
            "comment, " +
            "'1995-01-02' AS region_druid_dummy_ts " + // Dummy timestamp for Druid __time column
            "FROM tpch.tiny.region";

    @Language("SQL")
    public static final String SELECT_FROM_PART = " SELECT " +
            "partkey, " +
            "name, " +
            "mfgr, " +
            "brand, " +
            "type, " +
            "size, " +
            "container, " +
            "retailprice, " +
            "comment, " +
            "'1995-01-02' AS part_druid_dummy_ts " + // Dummy timestamp for Druid __time column;
            "FROM tpch.tiny.part";

    @Language("SQL")
    public static final String SELECT_FROM_CUSTOMER = " SELECT " +
            "custkey, " +
            "name, " +
            "address, " +
            "nationkey, " +
            "phone, " +
            "acctbal, " +
            "mktsegment, " +
            "comment, " +
            "'1995-01-02' AS customer_druid_dummy_ts " +  // Dummy timestamp for Druid __time column
            "FROM tpch.tiny.customer";

    private DruidTpchTables() {}

    public static String getSelectQuery(String tpchTableName)
    {
        if (tpchTableName.equals(ORDERS.getTableName())) {
            return SELECT_FROM_ORDERS;
        }
        if (tpchTableName.equals(LINE_ITEM.getTableName())) {
            return SELECT_FROM_LINEITEM;
        }
        if (tpchTableName.equals(NATION.getTableName())) {
            return SELECT_FROM_NATION;
        }
        if (tpchTableName.equals(REGION.getTableName())) {
            return SELECT_FROM_REGION;
        }
        if (tpchTableName.equals(PART.getTableName())) {
            return SELECT_FROM_PART;
        }
        if (tpchTableName.equals(CUSTOMER.getTableName())) {
            return SELECT_FROM_CUSTOMER;
        }
        throw new IllegalArgumentException("Unsupported tpch table name: " + tpchTableName);
    }
}
