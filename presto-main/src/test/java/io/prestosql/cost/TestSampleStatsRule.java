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
package io.prestosql.cost;

import io.prestosql.sql.planner.plan.SampleNode;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class TestSampleStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testBernoulliSample()
    {
        tester().assertStatsFor(pb -> pb
                .sample(0.5,
                        SampleNode.Type.BERNOULLI,
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .build())
                .check(check -> check
                        .outputRowsCount(5));
    }

    @Test
    public void testSystemSample()
    {
        tester().assertStatsFor(pb -> pb
                .sample(0.5,
                        SampleNode.Type.SYSTEM,
                        pb.values(pb.symbol("x", BIGINT), pb.symbol("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .build())
                .check(check -> check
                        .outputRowsCount(5));
    }
}
