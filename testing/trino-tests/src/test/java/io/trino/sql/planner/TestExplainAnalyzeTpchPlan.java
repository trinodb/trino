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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestExplainAnalyzeTpchPlan
        extends BaseCostBasedPlanTest
{
    public TestExplainAnalyzeTpchPlan()
    {
        super("tpch_sf1000_parquet", false);
    }

    @Override
    @ParameterizedTest
    @MethodSource("getQueryResourcePaths")
    public void test(String queryResourcePath)
    {
        assertExplainAnalyzePlan(queryResourcePath);
    }

    @Override
    protected List<String> getTableNames()
    {
        return TpchTable.getTables().stream()
                .map(TpchTable::getTableName)
                .collect(toImmutableList());
    }

    @Override
    protected String getTableResourceDirectory()
    {
        return "iceberg/tpch/sf1000/unpartitioned/";
    }

    @Override
    protected String getTableTargetDirectory()
    {
        return "iceberg-tpch-sf1000-parquet/";
    }

    @Override
    protected List<String> getQueryResourcePaths()
    {
        return TPCH_SQL_FILES;
    }
}
