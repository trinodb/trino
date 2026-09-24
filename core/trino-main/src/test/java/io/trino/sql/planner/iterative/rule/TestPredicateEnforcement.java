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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PlanNode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.TestingIr.comparison;
import static org.assertj.core.api.Assertions.assertThat;

class TestPredicateEnforcement
        extends BaseRuleTest
{
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testEnforcementAcrossMemoAlternatives(boolean filterFirst)
    {
        PlanNodeIdAllocator ids = new PlanNodeIdAllocator();
        PlanBuilder p = new PlanBuilder(ids, tester().getPlannerContext(), tester().getSession());
        Symbol a = p.symbol("a");
        Symbol b = p.symbol("b");
        PlanNode rows = p.values(ImmutableList.of(a), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 11L))));
        PlanNode filtered = p.filter(greaterThanTen(a), rows);
        PlanNode limited = p.limit(100, rows);
        AtomicReference<List<PlanNode>> alternatives = new AtomicReference<>(filterFirst ? ImmutableList.of(filtered, limited) : ImmutableList.of(limited, filtered));
        GroupReference group = new GroupReference(ids.getNextId(), 0, ImmutableList.of(a));
        PlanNode project = p.project(Assignments.of(b, a.toSymbolReference()), group);
        PredicateEnforcement enforcement = new PredicateEnforcement(
                tester().getPlannerContext(), tester().getSession(), new SymbolAllocator(ImmutableList.of(a, b)), Lookup.from(_ -> alternatives.get().stream()));

        assertThat(enforcement.isEnforcedBy(project, greaterThanTen(b))).isTrue();

        // Replacing the group's alternatives must invalidate the earlier proof.
        PlanNode replacement = p.values(ImmutableList.of(a), ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 6L))));
        alternatives.set(ImmutableList.of(p.limit(100, replacement), replacement));
        assertThat(enforcement.isEnforcedBy(project, greaterThanTen(b))).isFalse();
    }

    private static Expression greaterThanTen(Symbol symbol)
    {
        return comparison(GREATER_THAN, symbol.toSymbolReference(), new Constant(BIGINT, 10L));
    }
}
