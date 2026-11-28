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
import io.trino.spi.type.Type;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ProjectNode;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.type.collation.CollationRegistry.collationRegistry;

public class TestGroupByCollationKey
        extends BaseRuleTest
{
    private static final Type COLLATED = VARCHAR.withCollation(collationRegistry().getCollation("unicode_ci"));

    @Test
    public void testGroupsByCollationKey()
    {
        tester().assertThat(new GroupByCollationKey(tester().getMetadata()))
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("name", COLLATED))
                        .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                        .source(p.values(p.symbol("name", COLLATED)))))
                .matches(node(ProjectNode.class,
                        node(AggregationNode.class,
                                node(ProjectNode.class, values("name"))))
                        .with(ProjectNode.class, project -> {
                            AggregationNode aggregation = (AggregationNode) project.getSource();
                            // grouped by a binary key, with the string as one of the group's values,
                            // and the output keeps the original symbols
                            return aggregation.getGroupingKeys().size() == 1
                                    && aggregation.getGroupingKeys().getFirst().type().equals(VARBINARY)
                                    && aggregation.getAggregations().values().stream()
                                    .anyMatch(value -> value.getResolvedFunction().name().functionName().equals("any_value"))
                                    && project.getOutputSymbols().stream().map(symbol -> symbol.name()).toList().equals(ImmutableList.of("name", "count"));
                        }));
    }

    @Test
    public void testDoesNotFireForDefaultCollation()
    {
        tester().assertThat(new GroupByCollationKey(tester().getMetadata()))
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("name", VARCHAR))
                        .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                        .source(p.values(p.symbol("name", VARCHAR)))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireAfterSplit()
    {
        tester().assertThat(new GroupByCollationKey(tester().getMetadata()))
                .on(p -> p.aggregation(builder -> builder
                        .singleGroupingSet(p.symbol("name", COLLATED))
                        .step(PARTIAL)
                        .addAggregation(p.symbol("count", BIGINT), PlanBuilder.aggregation("count", ImmutableList.of()), ImmutableList.of())
                        .source(p.values(p.symbol("name", COLLATED)))))
                .doesNotFire();
    }
}
