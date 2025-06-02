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
import io.trino.operator.table.ExcludeColumnsFunction.ExcludeColumnsFunctionHandle;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

class TestRewriteExcludeColumnsFunctionToProjection
        extends BaseRuleTest
{
    @Test
    public void rewriteExcludeColumnsFunction()
    {
        tester().assertThat(new RewriteExcludeColumnsFunctionToProjection())
                .on(p -> {
                    Symbol a = p.symbol("a", BOOLEAN);
                    Symbol b = p.symbol("b", BIGINT);
                    Symbol c = p.symbol("c", SMALLINT);
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol y = p.symbol("y", SMALLINT);
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("exclude_columns")
                                    .properOutputs(x, y)
                                    .pruneWhenEmpty()
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(b, c)))
                                    .connectorHandle(new ExcludeColumnsFunctionHandle())
                                    .source(p.values(a, b, c)));
                })
                .matches(PlanMatchPattern.strictProject(
                        ImmutableMap.of(
                                "x", expression(new Reference(BIGINT, "b")),
                                "y", expression(new Reference(SMALLINT, "c"))),
                        values("a", "b", "c")));
    }

    @Test
    public void doNotRewriteOtherFunction()
    {
        tester().assertThat(new RewriteExcludeColumnsFunctionToProjection())
                .on(p -> {
                    Symbol a = p.symbol("a", BOOLEAN);
                    Symbol b = p.symbol("b", BIGINT);
                    Symbol c = p.symbol("c", SMALLINT);
                    return p.tableFunctionProcessor(
                            builder -> builder
                                    .name("testing_function")
                                    .requiredSymbols(ImmutableList.of(ImmutableList.of(b, c)))
                                    .source(p.values(a, b, c)));
                }).doesNotFire();
    }
}
