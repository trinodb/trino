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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestMergeFilters
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new MergeFilters())
                .on(p ->
                        p.filter(
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 44L)),
                                p.filter(
                                        new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 42L)),
                                        p.values(p.symbol("a"), p.symbol("b")))))
                .matches(filter(
                        new Logical(AND, ImmutableList.of(new Comparison(LESS_THAN, new Reference(INTEGER, "a"), new Constant(INTEGER, 42L)), new Comparison(GREATER_THAN, new Reference(INTEGER, "b"), new Constant(INTEGER, 44L)))),
                        values(ImmutableMap.of("a", 0, "b", 1))));
    }
}
