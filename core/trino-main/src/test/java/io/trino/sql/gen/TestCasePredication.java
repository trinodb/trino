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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.CompilerConfig;
import io.trino.sql.planner.Symbol;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.Arrays.asList;

public class TestCasePredication
{
    private static final Boolean T = true;
    private static final Boolean F = false;
    private static final Boolean BN = null;
    private static final Long N = null;

    // CASE WHEN c1 THEN r1 WHEN c2 THEN r2 ELSE d END
    private static Expression searchedCase()
    {
        return new Case(
                List.of(
                        new WhenClause(new Reference(BOOLEAN, "c1"), new Reference(BIGINT, "r1")),
                        new WhenClause(new Reference(BOOLEAN, "c2"), new Reference(BIGINT, "r2"))),
                new Reference(BIGINT, "d"));
    }

    private static final Map<Symbol, Integer> LAYOUT = ImmutableMap.of(
            new Symbol(BOOLEAN, "c1"), 0,
            new Symbol(BOOLEAN, "c2"), 1,
            new Symbol(BIGINT, "r1"), 2,
            new Symbol(BIGINT, "r2"), 3,
            new Symbol(BIGINT, "d"), 4);

    @Test
    void testFirstMatchWinsAndNullConditionDoesNotMatch()
    {
        List<Boolean> c1 = asList(T, F, BN, F, F);
        List<Boolean> c2 = asList(F, T, T, F, BN);
        List<Long> r1 = asList(10L, 10L, 10L, 10L, 10L);
        List<Long> r2 = asList(20L, 20L, 20L, 20L, 20L);
        List<Long> d = asList(99L, 99L, 99L, 99L, 99L);
        List<Long> expected = asList(10L, 20L, 20L, 99L, 99L);

        Page page = new Page(createBooleansBlock(c1), createBooleansBlock(c2), createLongsBlock(r1), createLongsBlock(r2), createLongsBlock(d));

        Block shortCircuit = project(searchedCase(), LAYOUT, page, false);
        Block predicated = project(searchedCase(), LAYOUT, page, true);

        assertBlockEquals(BIGINT, predicated, shortCircuit);
        assertBlockEquals(BIGINT, predicated, createLongsBlock(expected));
    }

    @Test
    void testNullableResultsAndDefaultMatchShortCircuit()
    {
        List<Boolean> c1 = asList(T, T, F);
        List<Boolean> c2 = asList(F, F, T);
        List<Long> r1 = asList(N, 5L, 5L);
        List<Long> r2 = asList(7L, 7L, N);
        List<Long> d = asList(N, 8L, 8L);

        Page page = new Page(createBooleansBlock(c1), createBooleansBlock(c2), createLongsBlock(r1), createLongsBlock(r2), createLongsBlock(d));

        assertBlockEquals(BIGINT, project(searchedCase(), LAYOUT, page, true), project(searchedCase(), LAYOUT, page, false));
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
