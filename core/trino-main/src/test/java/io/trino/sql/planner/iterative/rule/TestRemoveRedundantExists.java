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
import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.SymbolReference;
import io.trino.testing.TestingMetadata;
import org.junit.jupiter.api.Test;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class TestRemoveRedundantExists
        extends BaseRuleTest
{
    @Test
    public void testExistsFalse()
    {
        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(Assignments.of(p.symbol("exists"), new ExistsPredicate(TRUE_LITERAL)),
                        ImmutableList.of(),
                        p.values(1),
                        p.values(0)))
                .matches(
                        project(
                                ImmutableMap.of("exists", expression("false")),
                                values()));
    }

    @Test
    public void testExistsTrue()
    {
        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(Assignments.of(p.symbol("exists"), new ExistsPredicate(TRUE_LITERAL)),
                        ImmutableList.of(),
                        p.values(1),
                        p.values(1)))
                .matches(
                        project(
                                ImmutableMap.of("exists", expression("true")),
                                values()));
    }

    @Test
    public void testDoesNotFire()
    {
        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(Assignments.of(p.symbol("exists"), new ExistsPredicate(TRUE_LITERAL)),
                        ImmutableList.of(),
                        p.values(1),
                        p.tableScan(ImmutableList.of(), ImmutableMap.of())))
                .doesNotFire();

        tester().assertThat(new RemoveRedundantExists())
                .on(p -> p.apply(
                        Assignments.builder()
                                .put(p.symbol("exists"), new ExistsPredicate(TRUE_LITERAL))
                                .put(p.symbol("other"), new InPredicate(new SymbolReference("value"), new SymbolReference("list")))
                                .build(),
                        ImmutableList.of(),
                        p.values(1, p.symbol("value")),
                        p.tableScan(ImmutableList.of(p.symbol("list")), ImmutableMap.of(p.symbol("list"), new TestingMetadata.TestingColumnHandle("list")))))
                .doesNotFire();
    }
}
