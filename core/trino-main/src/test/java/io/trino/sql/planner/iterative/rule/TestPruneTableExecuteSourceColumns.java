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
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableExecute;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneTableExecuteSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllInputsReferenced()
    {
        tester().assertThat(new PruneTableExecuteSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.tableExecute(
                            ImmutableList.of(a),
                            ImmutableList.of("column_a"),
                            p.values(a, b));
                })
                .matches(
                        tableExecute(
                                ImmutableList.of("a"),
                                ImmutableList.of("column_a"),
                                strictProject(
                                        ImmutableMap.of("a", expression("a")),
                                        values("a", "b"))));
    }

    @Test
    public void testAllInputsReferenced()
    {
        tester().assertThat(new PruneTableExecuteSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    return p.tableExecute(
                            ImmutableList.of(a, b),
                            ImmutableList.of("column_a", "column_b"),
                            p.values(a, b));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPrunePartitioningSchemeSymbols()
    {
        tester().assertThat(new PruneTableExecuteSourceColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol partition = p.symbol("partition");
                    Symbol hash = p.symbol("hash");
                    return p.tableExecute(
                            ImmutableList.of(a),
                            ImmutableList.of("column_a"),
                            Optional.of(p.partitioningScheme(
                                    ImmutableList.of(partition, hash),
                                    ImmutableList.of(partition),
                                    hash)),
                            p.values(a, partition, hash));
                })
                .doesNotFire();
    }
}
