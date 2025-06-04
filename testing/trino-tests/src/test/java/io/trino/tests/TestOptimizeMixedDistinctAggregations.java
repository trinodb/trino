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
import io.trino.testing.AbstractTestAggregations;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;

public class TestOptimizeMixedDistinctAggregations
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder()
                .setCoordinatorProperties(ImmutableMap.of("optimizer.distinct-aggregations-strategy", "pre_aggregate"))
                .build();
    }

    // TODO add dedicated test cases and remove `extends AbstractTestAggregation`
}
