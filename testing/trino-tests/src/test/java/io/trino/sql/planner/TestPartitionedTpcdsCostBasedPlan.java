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

import io.trino.tpcds.Table;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * This class tests cost-based optimization rules. It contains unmodified TPC-DS queries.
 * This class is using Iceberg connector un-partitioned TPC-DS tables.
 */
public class TestPartitionedTpcdsCostBasedPlan
        extends BaseCostBasedPlanTest
{
    protected TestPartitionedTpcdsCostBasedPlan()
    {
        super("tpcds_sf1000_parquet_part", true);
    }

    @Override
    protected List<String> getTableNames()
    {
        return io.trino.tpcds.Table.getBaseTables().stream()
                .filter(table -> table != io.trino.tpcds.Table.DBGEN_VERSION)
                .map(Table::getName)
                .collect(toImmutableList());
    }

    @Override
    protected String getTableResourceDirectory()
    {
        return "iceberg/tpcds/sf1000/partitioned/";
    }

    @Override
    protected String getTableTargetDirectory()
    {
        return "iceberg-tpcds-sf1000-parquet-part/";
    }

    @Override
    protected List<String> getQueryResourcePaths()
    {
        return TPCDS_SQL_FILES;
    }

    public static void main(String[] args)
    {
        new TestPartitionedTpcdsCostBasedPlan().generate();
    }
}
