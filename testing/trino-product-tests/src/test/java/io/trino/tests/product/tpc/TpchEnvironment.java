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
package io.trino.tests.product.tpc;

import java.util.List;
import java.util.Map;

public class TpchEnvironment
        extends AbstractTpcEnvironment
{
    @Override
    protected String sourceCatalog()
    {
        return "tpch";
    }

    @Override
    protected Map<String, String> sourceCatalogProperties()
    {
        return Map.of(
                "connector.name", "tpch",
                "tpch.double-type-mapping", "DECIMAL");
    }

    @Override
    protected String targetSchema()
    {
        return "tpch";
    }

    @Override
    protected List<String> tables()
    {
        return List.of("nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem");
    }

    @Override
    protected String sourceTableQuery(String table)
    {
        return switch (table) {
            case "nation" -> "SELECT nationkey AS n_nationkey, name AS n_name, regionkey AS n_regionkey, comment AS n_comment FROM tpch.sf1.nation";
            case "region" -> "SELECT regionkey AS r_regionkey, name AS r_name, comment AS r_comment FROM tpch.sf1.region";
            case "part" -> "SELECT partkey AS p_partkey, name AS p_name, mfgr AS p_mfgr, brand AS p_brand, type AS p_type, size AS p_size, container AS p_container, retailprice AS p_retailprice, comment AS p_comment FROM tpch.sf1.part";
            case "supplier" -> "SELECT suppkey AS s_suppkey, name AS s_name, address AS s_address, nationkey AS s_nationkey, phone AS s_phone, acctbal AS s_acctbal, comment AS s_comment FROM tpch.sf1.supplier";
            case "partsupp" -> "SELECT partkey AS ps_partkey, suppkey AS ps_suppkey, availqty AS ps_availqty, supplycost AS ps_supplycost, comment AS ps_comment FROM tpch.sf1.partsupp";
            case "customer" -> "SELECT custkey AS c_custkey, name AS c_name, address AS c_address, nationkey AS c_nationkey, phone AS c_phone, acctbal AS c_acctbal, mktsegment AS c_mktsegment, comment AS c_comment FROM tpch.sf1.customer";
            case "orders" -> "SELECT orderkey AS o_orderkey, custkey AS o_custkey, orderstatus AS o_orderstatus, totalprice AS o_totalprice, orderdate AS o_orderdate, orderpriority AS o_orderpriority, clerk AS o_clerk, shippriority AS o_shippriority, comment AS o_comment FROM tpch.sf1.orders";
            case "lineitem" -> "SELECT orderkey AS l_orderkey, partkey AS l_partkey, suppkey AS l_suppkey, linenumber AS l_linenumber, quantity AS l_quantity, extendedprice AS l_extendedprice, discount AS l_discount, tax AS l_tax, returnflag AS l_returnflag, linestatus AS l_linestatus, shipdate AS l_shipdate, commitdate AS l_commitdate, receiptdate AS l_receiptdate, shipinstruct AS l_shipinstruct, shipmode AS l_shipmode, comment AS l_comment FROM tpch.sf1.lineitem";
            default -> throw new IllegalArgumentException("Unknown TPCH table: " + table);
        };
    }
}
