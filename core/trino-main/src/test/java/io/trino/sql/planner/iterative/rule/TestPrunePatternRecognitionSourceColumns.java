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
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import org.testng.annotations.Test;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.patternRecognition;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ALL_SHOW_EMPTY;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;

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
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.values(p.symbol("a")))))
                .matches(
                        patternRecognition(builder -> builder
                                        .rowsPerMatch(ONE)
                                        .pattern(new IrLabel("X"))
                                        .addVariableDefinition(new IrLabel("X"), "true"),
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
                        .addVariableDefinition(new IrLabel("X"), "true")
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
                        .addVariableDefinition(new IrLabel("X"), "true")
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
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneMeasureInputs()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .addMeasure(p.symbol("measure"), "LAST(X.a)", BIGINT)
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "true")
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }

    @Test
    public void testDoNotPruneVariableDefinitionInputs()
    {
        tester().assertThat(new PrunePatternRecognitionSourceColumns())
                .on(p -> p.patternRecognition(builder -> builder
                        .rowsPerMatch(ONE)
                        .pattern(new IrLabel("X"))
                        .addVariableDefinition(new IrLabel("X"), "LAST(X.a > 0)")
                        .source(p.values(p.symbol("a")))))
                .doesNotFire();
    }
}
