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
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPlanCopier
{
    @Test
    public void testCopySemiJoinAndRowNumber()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(idAllocator, PLANNER_CONTEXT, TEST_SESSION);
        Symbol a = p.symbol("a", BIGINT);
        Symbol b = p.symbol("b", BIGINT);
        Symbol match = p.symbol("match", BOOLEAN);
        Symbol rowNumber = p.symbol("row_number", BIGINT);
        PlanNode plan = p.rowNumber(ImmutableList.of(a), Optional.empty(), rowNumber, p.semiJoin(a, b, match, p.values(a), p.values(b)));

        NodeAndMappings copy = PlanCopier.copyPlan(plan, plan.getOutputSymbols(), emptySymbolAllocator(), idAllocator);

        assertThat(searchFrom(copy.getNode()).whereIsInstanceOfAny(SemiJoinNode.class).findAll()).hasSize(1);
        assertThat(searchFrom(copy.getNode()).whereIsInstanceOfAny(RowNumberNode.class).findAll()).hasSize(1);
        assertThat(copy.getFields()).hasSize(plan.getOutputSymbols().size());
    }

    @Test
    public void testCopyUniqueIdOnlyWhenRequested()
    {
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(idAllocator, PLANNER_CONTEXT, TEST_SESSION);
        Symbol a = p.symbol("a", BIGINT);
        Symbol unique = p.symbol("unique", BIGINT);
        PlanNode plan = p.assignUniqueId(unique, p.values(a));

        assertThatThrownBy(() -> PlanCopier.copyPlan(plan, plan.getOutputSymbols(), emptySymbolAllocator(), idAllocator))
                .isInstanceOf(UnsupportedOperationException.class);

        NodeAndMappings copy = PlanCopier.copyPlanAssigningNewUniqueIds(plan, plan.getOutputSymbols(), emptySymbolAllocator(), idAllocator, Lookup.noLookup());
        assertThat(searchFrom(copy.getNode()).whereIsInstanceOfAny(AssignUniqueId.class).findAll()).hasSize(1);
    }
}
