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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestRewriteExcludeColumns
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new RewriteExcludeColumns())
                .on(p -> p.tableFunctionProcessor(
                        builder -> builder
                                .name("exclude_columns")
                                .properOutputs(p.symbol("p", BIGINT))
                                .source(p.values(p.symbol("x", BIGINT), p.symbol("y", BIGINT)))))
                .matches(project(
                        ImmutableMap.of("p", expression(new Reference(BIGINT, "x"))), values("x", "y")));
    }
}
