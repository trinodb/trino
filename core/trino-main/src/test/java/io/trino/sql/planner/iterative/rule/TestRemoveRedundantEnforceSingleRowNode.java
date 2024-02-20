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
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestRemoveRedundantEnforceSingleRowNode
        extends BaseRuleTest
{
    @Test
    public void testRemoveEnforceWhenSourceScalar()
    {
        tester().assertThat(new RemoveRedundantEnforceSingleRowNode())
                .on(p -> p.enforceSingleRow(p.aggregation(builder -> builder
                        .addAggregation(p.symbol("c"), expression("count(a)"), ImmutableList.of(BIGINT))
                        .globalGrouping()
                        .source(p.values(p.symbol("a"))))))
                .matches(node(AggregationNode.class, values("a")));
    }

    @Test
    public void testDoNotFireWhenSourceNotScalar()
    {
        tester().assertThat(new RemoveRedundantEnforceSingleRowNode())
                .on(p -> p.enforceSingleRow(p.values(10, p.symbol("a"))))
                .doesNotFire();

        tester().assertThat(new RemoveRedundantEnforceSingleRowNode())
                .on(p -> p.enforceSingleRow(p.values(p.symbol("a"))))
                .doesNotFire();
    }
}
