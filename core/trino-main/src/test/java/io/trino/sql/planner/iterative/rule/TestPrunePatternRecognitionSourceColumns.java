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
import com.google.common.collect.ImmutableSet;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.rowpattern.LogicalIndexPointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import org.junit.jupiter.api.Test;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.RowsPerMatch.ALL_SHOW_EMPTY;
import static io.trino.sql.planner.plan.RowsPerMatch.ONE;

public class TestPrunePatternRecognitionSourceColumns
        extends BaseRuleTest
{
    @Test
    public void testPruneUnreferencedInput()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.values(p.symbol("a")))))
                .matches(
                        patternRecognition(builder -> builder
                                        .rowsPerMatch(ONE)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), TRUE),
                                strictProject(
                                        ImmutableMap.of(),
                                        values("a"))));
    }

    @Test
    public void testDoNotPruneInputsWithAllRowsPerMatch()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .rowsPerMatch(ALL_SHOW_EMPTY)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPrunePartitionByInputs()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .partitionBy(ImmutableList.of(p.symbol("a")))
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneOrderByInputs()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .orderBy(new OrderingScheme(ImmutableList.of(p.symbol("a")), ImmutableMap.of(p.symbol("a"), ASC_NULLS_LAST)))
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneMeasureInputs()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .addMeasure(
                                p.symbol("measure", BIGINT),
                                new Reference(BIGINT, "pointer"),
                                ImmutableMap.of("pointer", new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(BIGINT, "a"))))
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), TRUE)
                        .source(p.values(p.symbol("a", BIGINT)))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneVariableDefinitionInputs()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(
                                new IrLabel("X"),
                                new Comparison(GREATER_THAN, new Reference(INTEGER, "pointer"), new Constant(INTEGER, 0L)),
                                ImmutableMap.of(new Symbol(INTEGER, "pointer"), new ScalarValuePointer(
                                        new LogicalIndexPointer(ImmutableSet.of(new IrLabel("X")), true, true, 0, 0),
                                        new Symbol(INTEGER, "a"))))
                        .source(p.values(p.symbol("a", INTEGER)))))
                .doesNotFire();
    }
}
