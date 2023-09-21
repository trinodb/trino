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
import io.trino.cost.StatsAndCosts;
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.StringLiteral;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.trino.sql.planner.TopologicalOrderSubPlanVisitor.sortPlanInTopologicalOrder;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTopologicalOrderSubPlanVisitor
{
    private interface JoinFunction
    {
        PlanNode apply(String id, PlanNode left, PlanNode right);
    }

    private void test(JoinFunction f)
    {
        //   * root
        //  / \
        // A   * middle
        //    / \
        //   B   C
        SubPlan a = valuesSubPlan("a");
        SubPlan b = valuesSubPlan("b");
        SubPlan c = valuesSubPlan("c");

        SubPlan middle = createSubPlan("middle",
                f.apply("middle_join", remoteSource("b"), remoteSource("c")),
                ImmutableList.of(b, c));

        SubPlan root = createSubPlan("root",
                f.apply("root_join", remoteSource("a"), remoteSource("middle")),
                ImmutableList.of(a, middle));

        assertThat(sortPlanInTopologicalOrder(root))
                .isEqualTo(ImmutableList.of(c, b, middle, a, root));
    }

    @Test
    public void testJoinOrder()
    {
        test(TestTopologicalOrderSubPlanVisitor::join);
    }

    @Test
    public void testSemiJoinOrder()
    {
        test(TestTopologicalOrderSubPlanVisitor::semiJoin);
    }

    @Test
    public void testIndexJoin()
    {
        test(TestTopologicalOrderSubPlanVisitor::indexJoin);
    }

    @Test
    public void testSpatialJoin()
    {
        test(TestTopologicalOrderSubPlanVisitor::spatialJoin);
    }

    private static RemoteSourceNode remoteSource(String fragmentId)
    {
        return remoteSource(ImmutableList.of(fragmentId));
    }

    private static RemoteSourceNode remoteSource(List<String> fragmentIds)
    {
        return new RemoteSourceNode(
                new PlanNodeId(fragmentIds.get(0)),
                fragmentIds.stream().map(PlanFragmentId::new).collect(toImmutableList()),
                ImmutableList.of(new Symbol("blah")),
                Optional.empty(),
                REPARTITION,
                RetryPolicy.TASK);
    }

    private static JoinNode join(String id, PlanNode left, PlanNode right)
    {
        return new JoinNode(
                new PlanNodeId(id),
                INNER,
                left,
                right,
                ImmutableList.of(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }

    private static SemiJoinNode semiJoin(String id, PlanNode left, PlanNode right)
    {
        return new SemiJoinNode(
                new PlanNodeId(id),
                left,
                right,
                left.getOutputSymbols().get(0),
                right.getOutputSymbols().get(0),
                new Symbol(id),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static IndexJoinNode indexJoin(String id, PlanNode left, PlanNode right)
    {
        return new IndexJoinNode(
                new PlanNodeId(id),
                IndexJoinNode.Type.INNER,
                left,
                right,
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty());
    }

    private static SpatialJoinNode spatialJoin(String id, PlanNode left, PlanNode right)
    {
        return new SpatialJoinNode(
                new PlanNodeId(id),
                SpatialJoinNode.Type.INNER,
                left,
                right,
                left.getOutputSymbols(),
                BooleanLiteral.TRUE_LITERAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static SubPlan valuesSubPlan(String fragmentId)
    {
        Symbol symbol = new Symbol("column");
        return createSubPlan(fragmentId, new ValuesNode(new PlanNodeId(fragmentId + "Values"),
                        ImmutableList.of(symbol),
                        ImmutableList.of(new Row(ImmutableList.of(new StringLiteral("foo"))))),
                ImmutableList.of());
    }

    private static SubPlan createSubPlan(String fragmentId, PlanNode plan, List<SubPlan> children)
    {
        Symbol symbol = plan.getOutputSymbols().get(0);
        PlanNodeId valuesNodeId = new PlanNodeId("plan");
        PlanFragment planFragment = new PlanFragment(
                new PlanFragmentId(fragmentId),
                plan,
                ImmutableMap.of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                Optional.empty(),
                ImmutableList.of(valuesNodeId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                StatsAndCosts.empty(),
                ImmutableList.of(),
                Optional.empty());
        return new SubPlan(planFragment, children);
    }
}
