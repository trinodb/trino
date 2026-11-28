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
package io.trino.sql.planner.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJoinNode
{
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private final PlanBuilder planBuilder = new PlanBuilder(idAllocator, PLANNER_CONTEXT, TEST_SESSION);
    private final Symbol a = planBuilder.symbol("a");
    private final Symbol b = planBuilder.symbol("b");
    private final Symbol c = planBuilder.symbol("c");
    private final Symbol d = planBuilder.symbol("d");
    private final Symbol e = planBuilder.symbol("e");
    private final PlanNode left = planBuilder.values(a, b, c);
    private final PlanNode right = planBuilder.values(d, e);

    @Test
    public void testOutputSymbolsMustComeFromSources()
    {
        // in source order, a subset in source order, out of source order, and repeated
        for (List<Symbol> leftOutputs : ImmutableList.of(
                ImmutableList.of(a, b, c),
                ImmutableList.of(a, c),
                ImmutableList.of(c, a),
                ImmutableList.of(b, b),
                ImmutableList.<Symbol>of())) {
            assertThat(join(leftOutputs, ImmutableList.of(e, d), new EquiJoinClause(a, d)).getOutputSymbols())
                    .containsExactlyElementsOf(ImmutableList.<Symbol>builder().addAll(leftOutputs).add(e, d).build());
        }

        assertThatThrownBy(() -> join(ImmutableList.of(a, d), ImmutableList.of(e), new EquiJoinClause(a, d)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Left source inputs do not contain all left output symbols");
        assertThatThrownBy(() -> join(ImmutableList.of(c, a), ImmutableList.of(e, a), new EquiJoinClause(a, d)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Right source inputs do not contain all right output symbols");
        assertThatThrownBy(() -> join(ImmutableList.of(a), ImmutableList.of(d), new EquiJoinClause(d, a)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Equality join criteria should be normalized according to join sides");
    }

    @Test
    public void testCopiesKeepOutputSymbols()
    {
        JoinNode join = join(ImmutableList.of(c, a), ImmutableList.of(d), new EquiJoinClause(a, d));
        assertThat(join.withDistributionType(PARTITIONED).getOutputSymbols()).containsExactly(c, a, d);
        assertThat(join.flipChildren().getOutputSymbols()).containsExactly(d, c, a);
        assertThat(join.flipChildren().getCriteria()).containsExactly(new EquiJoinClause(d, a));
        assertThat(join.flipChildren().withDistributionType(PARTITIONED).getOutputSymbols()).containsExactly(d, c, a);
    }

    private JoinNode join(List<Symbol> leftOutputs, List<Symbol> rightOutputs, EquiJoinClause clause)
    {
        return new JoinNode(
                idAllocator.getNextId(),
                INNER,
                left,
                right,
                ImmutableList.of(clause),
                leftOutputs,
                rightOutputs,
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                Optional.empty());
    }
}
