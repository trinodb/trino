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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.trino.cost.StatsAndCosts;
import io.trino.operator.RetryPolicy;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.UnionNode;
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
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSchedulingUtils
{
    @Test
    public void testCanStreamNoJoin()
    {
        /*
                  parent(remote)
                     |
     -------------------------------------- stage boundary
                     a
         */
        SubPlan parentSubPlan = createSubPlan(
                "parent",
                remoteSource("a"),
                ImmutableList.of(valuesSubPlan("a")));

        assertThat(SchedulingUtils.canStream(parentSubPlan, valuesSubPlan("a"))).isTrue();
    }

    @Test
    public void testCanStreamJoin()
    {
        /*
                  parent(join)
                   /   \
     -------------------------------------- stage boundary
                  a     b
         */
        SubPlan aSubPlan = valuesSubPlan("a");
        RemoteSourceNode remoteSourceA = remoteSource("a");

        SubPlan bSubPlan = valuesSubPlan("b");
        RemoteSourceNode remoteSourceB = remoteSource("b");

        SubPlan parentSubPlan = createSubPlan(
                "parent",
                join("join", remoteSourceA, remoteSourceB),
                ImmutableList.of(aSubPlan, bSubPlan));

        assertThat(SchedulingUtils.canStream(parentSubPlan, aSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, bSubPlan)).isFalse();
    }

    @Test
    public void testCanStreamTwoJoins()
    {
        /*
                  parent(join1)
                   /   \
                  /     join2
                 /       /   \
     -------------------------------------- stage boundary
                a       b     c

         */
        SubPlan aSubPlan = valuesSubPlan("a");
        RemoteSourceNode remoteSourceA = remoteSource("a");

        SubPlan bSubPlan = valuesSubPlan("b");
        RemoteSourceNode remoteSourceB = remoteSource("b");

        SubPlan cSubPlan = valuesSubPlan("c");
        RemoteSourceNode remoteSourceC = remoteSource("c");

        SubPlan parentSubPlan = createSubPlan(
                "parent",
                join("join1", remoteSourceA, join("join2", remoteSourceB, remoteSourceC)),
                ImmutableList.of(bSubPlan, cSubPlan, aSubPlan));

        assertThat(SchedulingUtils.canStream(parentSubPlan, aSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, bSubPlan)).isFalse();
        assertThat(SchedulingUtils.canStream(parentSubPlan, cSubPlan)).isFalse();
    }

    @Test
    public void testCanStreamJoinWithUnion()
    {
        /*
                  parent(join)
                   /       \
                union1     union2
                /   \      /   \
     -------------------------------------- stage boundary
               a     b    c     d
         */
        SubPlan aSubPlan = valuesSubPlan("a");
        RemoteSourceNode remoteSourceA = remoteSource("a");

        SubPlan bSubPlan = valuesSubPlan("b");
        RemoteSourceNode remoteSourceB = remoteSource("b");

        SubPlan cSubPlan = valuesSubPlan("c");
        RemoteSourceNode remoteSourceC = remoteSource("c");

        SubPlan dSubPlan = valuesSubPlan("d");
        RemoteSourceNode remoteSourceD = remoteSource("d");

        UnionNode union1 = union("union1", ImmutableList.of(remoteSourceA, remoteSourceB));
        UnionNode union2 = union("union2", ImmutableList.of(remoteSourceC, remoteSourceD));

        SubPlan parentSubPlan = createSubPlan(
                "parent",
                join("join", union1, union2),
                ImmutableList.of(bSubPlan, cSubPlan, aSubPlan, dSubPlan));

        assertThat(SchedulingUtils.canStream(parentSubPlan, aSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, bSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, cSubPlan)).isFalse();
        assertThat(SchedulingUtils.canStream(parentSubPlan, dSubPlan)).isFalse();
    }

    @Test
    public void testCanStreamJoinMultipleSubPlanPerRemoteSource()
    {
        /*
                  parent(join)
                   /      \
     -------------------------------------- stage boundary
                 a+b      c+d  (each side of join reads from two remote sources)
         */
        SubPlan aSubPlan = valuesSubPlan("a");
        SubPlan bSubPlan = valuesSubPlan("b");
        RemoteSourceNode remoteSourceAB = remoteSource(ImmutableList.of("a", "b"));

        SubPlan cSubPlan = valuesSubPlan("c");
        SubPlan dSubPlan = valuesSubPlan("d");
        RemoteSourceNode remoteSourceCD = remoteSource(ImmutableList.of("c", "d"));

        SubPlan parentSubPlan = createSubPlan(
                "parent",
                join("join", remoteSourceAB, remoteSourceCD),
                ImmutableList.of(bSubPlan, cSubPlan, aSubPlan, dSubPlan));

        assertThat(SchedulingUtils.canStream(parentSubPlan, aSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, bSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, cSubPlan)).isFalse();
        assertThat(SchedulingUtils.canStream(parentSubPlan, dSubPlan)).isFalse();
    }

    @Test
    public void testCanStreamSemiJoin()
    {
        /*
                  parent(semijoin)
                   /   \
     -------------------------------------- stage boundary
                  a     b
         */
        SubPlan aSubPlan = valuesSubPlan("a");
        RemoteSourceNode remoteSourceA = remoteSource("a");

        SubPlan bSubPlan = valuesSubPlan("b");
        RemoteSourceNode remoteSourceB = remoteSource("b");

        SubPlan parentSubPlan = createSubPlan(
                "parent",
                semiJoin("semijoin", remoteSourceA, remoteSourceB),
                ImmutableList.of(aSubPlan, bSubPlan));

        assertThat(SchedulingUtils.canStream(parentSubPlan, aSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, bSubPlan)).isFalse();
    }

    @Test
    public void testCanStreamIndexJoin()
    {
        /*
                  parent(indexjoin)
                   /   \
     -------------------------------------- stage boundary
                  a     b
         */
        SubPlan aSubPlan = valuesSubPlan("a");
        RemoteSourceNode remoteSourceA = remoteSource("a");

        SubPlan bSubPlan = valuesSubPlan("b");
        RemoteSourceNode remoteSourceB = remoteSource("b");

        SubPlan parentSubPlan = createSubPlan(
                "parent",
                indexJoin("indexjoin", remoteSourceA, remoteSourceB),
                ImmutableList.of(aSubPlan, bSubPlan));

        assertThat(SchedulingUtils.canStream(parentSubPlan, aSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, bSubPlan)).isFalse();
    }

    @Test
    public void testCanStreamSpatialJoin()
    {
        /*
                  parent(spatialjoin)
                   /   \
     -------------------------------------- stage boundary
                  a     b
         */
        SubPlan aSubPlan = valuesSubPlan("a");
        RemoteSourceNode remoteSourceA = remoteSource("a");

        SubPlan bSubPlan = valuesSubPlan("b");
        RemoteSourceNode remoteSourceB = remoteSource("b");

        SubPlan parentSubPlan = createSubPlan(
                "parent",
                spatialJoin("spatialjoin", remoteSourceA, remoteSourceB),
                ImmutableList.of(aSubPlan, bSubPlan));

        assertThat(SchedulingUtils.canStream(parentSubPlan, aSubPlan)).isTrue();
        assertThat(SchedulingUtils.canStream(parentSubPlan, bSubPlan)).isFalse();
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

    private static UnionNode union(String id, List<PlanNode> sources)
    {
        Symbol symbol = new Symbol(id);
        return new UnionNode(new PlanNodeId(id), sources, ImmutableListMultimap.of(), ImmutableList.of(symbol));
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
