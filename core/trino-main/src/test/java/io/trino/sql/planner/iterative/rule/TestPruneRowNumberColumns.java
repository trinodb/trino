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
import io.trino.sql.planner.assertions.RowNumberSymbolMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.Assignments;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.rowNumber;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneRowNumberColumns
        extends BaseRuleTest
{
    @Test
    public void testRowNumberSymbolNotReferenced()
    {
        // no partitioning, no limit per partition
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a),
                            p.rowNumber(ImmutableList.of(), Optional.empty(), rowNumber, p.values(a)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                values(ImmutableList.of("a"))));

        // partitioning is present, no limit per partition
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a),
                            p.rowNumber(ImmutableList.of(a), Optional.empty(), rowNumber, p.values(a)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                values(ImmutableList.of("a"))));

        // no partitioning, limit per partition is present
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a),
                            p.rowNumber(ImmutableList.of(), Optional.of(5), rowNumber, p.values(a)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                limit(
                                        5,
                                        values(ImmutableList.of("a")))));

        // partitioning and limit per partition are present
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol b = p.symbol("b");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a),
                            p.rowNumber(ImmutableList.of(a), Optional.of(5), rowNumber, p.values(a, b)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("a", expression("a")),
                                rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of("a"))
                                                .maxRowCountPerPartition(Optional.of(5)),
                                        strictProject(
                                                ImmutableMap.of("a", expression("a")),
                                                values(ImmutableList.of("a", "b"))))));
    }

    @Test
    public void testDoNotPrunePartitioningSymbol()
    {
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(rowNumber),
                            p.rowNumber(ImmutableList.of(a), Optional.empty(), rowNumber, p.values(a)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneHashSymbol()
    {
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    Symbol hash = p.symbol("hash");
                    return p.project(
                            Assignments.identity(a, rowNumber),
                            p.rowNumber(ImmutableList.of(a), Optional.empty(), rowNumber, Optional.of(hash), p.values(a, hash)));
                })
                .doesNotFire();
    }

    @Test
    public void testSourceSymbolNotReferenced()
    {
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(rowNumber),
                            p.rowNumber(ImmutableList.of(), Optional.empty(), rowNumber, p.values(a)));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("row_number", expression("row_number")),
                                rowNumber(
                                        pattern -> pattern
                                                .partitionBy(ImmutableList.of()),
                                        strictProject(
                                                ImmutableMap.of(),
                                                values(ImmutableList.of("a"))))
                                        .withAlias("row_number", new RowNumberSymbolMatcher())));
    }

    @Test
    public void testAllSymbolsReferenced()
    {
        tester().assertThat(new PruneRowNumberColumns())
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol rowNumber = p.symbol("row_number");
                    return p.project(
                            Assignments.identity(a, rowNumber),
                            p.rowNumber(ImmutableList.of(), Optional.empty(), rowNumber, p.values(a)));
                })
                .doesNotFire();
    }
}
