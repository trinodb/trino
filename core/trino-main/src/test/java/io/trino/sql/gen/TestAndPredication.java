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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableMap;
import io.trino.operator.project.PageProjection;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Arrays.asList;

public class TestAndPredication
{
    private static final Boolean T = true;
    private static final Boolean F = false;
    private static final Boolean N = null;

    @Test
    void testTwoTermAndMatchesShortCircuitAcrossThreeValuedLogic()
    {
        // every combination of (left, right) over {TRUE, FALSE, NULL}
        List<Boolean> left = asList(T, T, T, F, F, F, N, N, N);
        List<Boolean> right = asList(T, F, N, T, F, N, T, F, N);
        List<Boolean> expected = asList(T, F, N, F, F, F, N, F, N);

        Expression and = new Logical(Logical.Operator.AND, List.of(new Reference(BOOLEAN, "a"), new Reference(BOOLEAN, "b")));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BOOLEAN, "a"), 0, new Symbol(BOOLEAN, "b"), 1);
        Page page = new Page(createBooleansBlock(left), createBooleansBlock(right));

        Block shortCircuit = project(and, layout, page, false);
        Block predicated = project(and, layout, page, true);

        // predication must not change results, and must match the SQL three-valued AND truth table
        assertBlockEquals(BOOLEAN, predicated, shortCircuit);
        assertBlockEquals(BOOLEAN, predicated, createBooleansBlock(expected));
    }

    @Test
    void testThreeTermAndMatchesShortCircuit()
    {
        List<Boolean> a = asList(T, T, F, N, T, N);
        List<Boolean> b = asList(T, N, T, T, F, N);
        List<Boolean> c = asList(T, T, T, F, N, F);

        Expression and = new Logical(Logical.Operator.AND, List.of(new Reference(BOOLEAN, "a"), new Reference(BOOLEAN, "b"), new Reference(BOOLEAN, "c")));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BOOLEAN, "a"), 0, new Symbol(BOOLEAN, "b"), 1, new Symbol(BOOLEAN, "c"), 2);
        Page page = new Page(createBooleansBlock(a), createBooleansBlock(b), createBooleansBlock(c));

        assertBlockEquals(BOOLEAN, project(and, layout, page, true), project(and, layout, page, false));
    }

    private static Block project(Expression expression, Map<Symbol, Integer> layout, Page page, boolean predicationEnabled)
    {
        PageFunctionCompiler compiler = new PageFunctionCompiler(
                PLANNER_CONTEXT.getFunctionManager(),
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getTypeManager(),
                new CompilerConfig().setConditionalPredicationEnabled(predicationEnabled));
        PageProjection projection = compiler.compileProjection(expression, layout, Optional.empty()).get();
        SourcePage input = projection.getInputChannels().getInputChannels(SourcePage.create(page));
        return projection.project(SESSION, input, positionsRange(0, page.getPositionCount()));
    }
}
