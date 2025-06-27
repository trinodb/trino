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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableScanNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSchedulingOrderVisitor
{
    @Test
    public void testJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
        TableScanNode a = planBuilder.tableScan(emptyList(), emptyMap());
        TableScanNode b = planBuilder.tableScan(emptyList(), emptyMap());
        List<PlanNodeId> order = scheduleOrder(planBuilder.join(JoinType.INNER, a, b));
        assertThat(order).isEqualTo(ImmutableList.of(b.getId(), a.getId()));
    }

    @Test
    public void testIndexJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
        TableScanNode a = planBuilder.tableScan(emptyList(), emptyMap());
        TableScanNode b = planBuilder.tableScan(emptyList(), emptyMap());
        List<PlanNodeId> order = scheduleOrder(planBuilder.indexJoin(IndexJoinNode.Type.INNER, a, b));
        assertThat(order).isEqualTo(ImmutableList.of(b.getId(), a.getId()));
    }

    @Test
    public void testSemiJoinOrder()
    {
        PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), PLANNER_CONTEXT, TEST_SESSION);
        Symbol sourceJoin = planBuilder.symbol("sourceJoin");
        TableScanNode a = planBuilder.tableScan(ImmutableList.of(sourceJoin), ImmutableMap.of(sourceJoin, new TestingColumnHandle("sourceJoin")));
        Symbol filteringSource = planBuilder.symbol("filteringSource");
        TableScanNode b = planBuilder.tableScan(ImmutableList.of(filteringSource), ImmutableMap.of(filteringSource, new TestingColumnHandle("filteringSource")));
        List<PlanNodeId> order = scheduleOrder(planBuilder.semiJoin(
                sourceJoin,
                filteringSource,
                planBuilder.symbol("semiJoinOutput"),
                a,
                b));
        assertThat(order).isEqualTo(ImmutableList.of(b.getId(), a.getId()));
    }
}
