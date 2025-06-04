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
package io.trino.tests;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

public class TestDistinctToSubqueriesAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // using memory connector, because it enables ConnectorMetadata#allowSplittingReadIntoMultipleSubQueries
        return MemoryQueryRunner.builder()
                .setInitialTables(TpchTable.getTables())
                .setCoordinatorProperties(ImmutableMap.of("optimizer.distinct-aggregations-strategy", "split_to_subqueries"))
                .build();
    }
}
