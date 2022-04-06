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
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

public class TestAdaptivePartialAggregation
        extends AbstractTestAggregations
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder()
                .setExtraProperties(ImmutableMap.of(
                        "adaptive-partial-aggregation.min-rows", "0",
                        "task.max-partial-aggregation-memory", "0B",
                        "optimizer.push-partial-aggregation-through-join", "true"))
                .build();
    }

    @Test
    public void testAggregationWithLambda()
    {
        // case with partial aggregation disabled adaptively.
        // orderkey + 1 is needed to avoid streaming aggregation.
        assertQuery(
                "SELECT orderkey + 1, reduce_agg(orderkey, 1, (a, b) -> a * b, (a, b) -> a * b) " +
                        "FROM orders " +
                        "GROUP BY orderkey + 1",
                "SELECT orderkey + 1, orderkey from orders");
    }

    @Test
    public void testPushPartialAggregationThroughJoin()
    {
        assertQuery("select count(*) from orders o join customer c on o.custkey = c.custkey");
    }
}
