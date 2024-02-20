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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.StatisticAggregationsDescriptor;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneTableWriterSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneTableWriterSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.tableWriter(
                            ImmutableList.of(a),
                            ImmutableList.of("column_a"),
                            p.values(a, b));
                })
                .matches(
                        tableWriter(
                                ImmutableList.of("a"),
                                ImmutableList.of("column_a"),
                                strictProject(
                                        ImmutableMap.of("a", expression("a")),
                                        values("a", "b"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneTableWriterSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.tableWriter(
                            ImmutableList.of(a, b),
                            ImmutableList.of("column_a", "column_b"),
                            p.values(a, b));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPrunePartitioningSchemeSymbols()
    {
        tester().assertThat(new PruneTableWriterSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol partition = p.symbol("partition");
                    Symbol hash = p.symbol("hash");
                    return p.tableWriter(
                            ImmutableList.of(a),
                            ImmutableList.of("column_a"),
                            Optional.of(p.partitioningScheme(
                                    ImmutableList.of(partition, hash),
                                    ImmutableList.of(partition),
                                    hash)),
                            Optional.empty(),
                            Optional.empty(),
                            p.values(a, partition, hash));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneStatisticAggregationSymbols()
    {
        tester().assertThat(new PruneTableWriterSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol group = p.symbol("group");
                    Symbol argument = p.symbol("argument");
                    Symbol aggregation = p.symbol("aggregation");
                    return p.tableWriter(
                            ImmutableList.of(a),
                            ImmutableList.of("column_a"),
                            Optional.empty(),
                            Optional.of(
                                    p.statisticAggregations(
                                            ImmutableMap.of(aggregation, p.aggregation(PlanBuilder.expression("avg(argument)"), ImmutableList.of(BIGINT))),
                                            ImmutableList.of(group))),
                            Optional.of(StatisticAggregationsDescriptor.empty()),
                            p.values(a, group, argument));
                })
                .doesNotFire();
    }
}
