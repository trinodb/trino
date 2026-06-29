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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Arrays.asList;

public class TestCoalescePredication
{
    private static final Long N = null;

    @Test
    void testBigintCoalesceMatchesShortCircuit()
    {
        List<Long> a = asList(N, N, 3L, 3L);
        List<Long> b = asList(N, 5L, N, 5L);
        List<Long> expected = asList(N, 5L, 3L, 3L);

        Expression coalesce = new Coalesce(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "a"), 0, new Symbol(BIGINT, "b"), 1);
        Page page = new Page(createLongsBlock(a), createLongsBlock(b));

        Block shortCircuit = project(coalesce, layout, page, false);
        Block predicated = project(coalesce, layout, page, true);

        assertBlockEquals(BIGINT, predicated, shortCircuit);
        assertBlockEquals(BIGINT, predicated, createLongsBlock(expected));
    }

    @Test
    void testThreeOperandCoalesce()
    {
        List<Long> a = asList(N, N, N, 1L);
        List<Long> b = asList(N, N, 2L, 2L);
        List<Long> c = asList(N, 3L, 3L, 3L);
        List<Long> expected = asList(N, 3L, 2L, 1L);

        Expression coalesce = new Coalesce(new Reference(BIGINT, "a"), new Reference(BIGINT, "b"), new Reference(BIGINT, "c"));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(BIGINT, "a"), 0, new Symbol(BIGINT, "b"), 1, new Symbol(BIGINT, "c"), 2);
        Page page = new Page(createLongsBlock(a), createLongsBlock(b), createLongsBlock(c));

        assertBlockEquals(BIGINT, project(coalesce, layout, page, true), project(coalesce, layout, page, false));
        assertBlockEquals(BIGINT, project(coalesce, layout, page, true), createLongsBlock(expected));
    }

    @Test
    void testVarcharCoalesceMatchesShortCircuit()
    {
        // reference-typed value select (Slice), not just primitives
        List<String> a = asList(null, null, "x", "x");
        List<String> b = asList(null, "y", null, "y");

        Expression coalesce = new Coalesce(new Reference(VARCHAR, "a"), new Reference(VARCHAR, "b"));
        Map<Symbol, Integer> layout = ImmutableMap.of(new Symbol(VARCHAR, "a"), 0, new Symbol(VARCHAR, "b"), 1);
        Page page = new Page(createStringsBlock(a), createStringsBlock(b));

        assertBlockEquals(VARCHAR, project(coalesce, layout, page, true), project(coalesce, layout, page, false));
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
