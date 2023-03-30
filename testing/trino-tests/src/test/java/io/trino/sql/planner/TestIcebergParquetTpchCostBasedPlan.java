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

package io.trino.sql.planner;

import io.trino.tpch.TpchTable;

import java.util.stream.Stream;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPC-H queries.
 * This class is using Iceberg connector unpartitioned TPC-H tables.
 */
public class TestIcebergParquetTpchCostBasedPlan
        extends BaseIcebergCostBasedPlanTest
{
    public TestIcebergParquetTpchCostBasedPlan()
    {
        super("tpch_sf1000_parquet", "parquet", false);
    }

    @Override
    protected void doPrepareTables()
    {
        TpchTable.getTables().forEach(table -> {
            populateTableFromResource(
                    table.getTableName(),
                    "iceberg/tpch/sf1000/parquet/unpartitioned/" + table.getTableName(),
                    "iceberg-tpch-sf1000-parquet/" + table.getTableName());
        });
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return TPCH_SQL_FILES.stream();
    }

    public static void main(String[] args)
    {
        new TestIcebergParquetTpchCostBasedPlan().generate();
    }
}
