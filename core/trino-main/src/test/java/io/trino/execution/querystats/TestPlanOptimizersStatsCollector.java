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
package io.trino.execution.querystats;

import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.api.Test;

import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPlanOptimizersStatsCollector
{
    @Test
    void testClose()
    {
        PlanOptimizersStatsCollector collector = createPlanOptimizersStatsCollector();
        PlanOptimizer optimizer = new IdentityOptimizer();
        collector.recordOptimizer(optimizer, 10);
        assertThat(collector.getTopRuleStats()).hasSize(1);

        collector.close();
        assertThat(collector.getTopRuleStats()).isEmpty();
        assertThatThrownBy(() -> collector.recordOptimizer(optimizer, 10))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Plan optimizer stats collector is closed");
    }

    private static class IdentityOptimizer
            implements PlanOptimizer
    {
        @Override
        public PlanNode optimize(PlanNode plan, Context context)
        {
            return plan;
        }
    }
}
