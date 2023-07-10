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

import java.util.stream.Stream;

/**
 * This class tests cost-based optimization rules related to joins. It contains unmodified TPC-H queries.
 * This class is using Hive connector with mocked in memory thrift metastore with partitioned TPC-H tables.
 */
public class TestHivePartitionedTpchCostBasedPlan
        extends BaseHiveCostBasedPlanTest
{
    /*
     * CAUTION: The expected plans here are not necessarily optimal yet. Their role is to prevent
     * inadvertent regressions. A conscious improvement to the planner may require changing some
     * of the expected plans, but any such change should be verified on an actual cluster with
     * large amount of data.
     */

    public static final String PARTITIONED_TPCH_METADATA_DIR = "/hive_metadata/partitioned_tpch";

    public TestHivePartitionedTpchCostBasedPlan()
    {
        super(PARTITIONED_TPCH_METADATA_DIR, true);
    }

    @Override
    protected Stream<String> getQueryResourcePaths()
    {
        return TPCH_SQL_FILES.stream();
    }

    public static void main(String[] args)
    {
        new TestHivePartitionedTpchCostBasedPlan().generate();
    }
}
