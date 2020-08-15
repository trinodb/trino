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
package io.trino.operator.project;

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.Expression;
import io.trino.testing.TestingSession;
import io.trino.transaction.TransactionId;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.Result;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ExpressionTestUtils.createExpression;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageFieldsToInputParametersRewriter
{
    private static final TypeAnalyzer TYPE_ANALYZER = createTestingTypeAnalyzer(PLANNER_CONTEXT);
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder()
            .setTransactionId(TransactionId.create())
            .build();

    @Test
    public void testEagerLoading()
    {
        RowExpressionBuilder builder = RowExpressionBuilder.create()
                .addSymbol("bigint0", BIGINT)
                .addSymbol("bigint1", BIGINT);
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 + 5"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("CAST((bigint0 * 10) AS INT)"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("COALESCE((bigint0 % 2), bigint0)"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 IN (1, 2, 3)"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 > 0"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 + 1 = 0"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 BETWEEN 1 AND 10"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("CASE WHEN (bigint0 > 0) THEN bigint0 ELSE null END"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("CASE bigint0 WHEN 1 THEN 1 ELSE -bigint0 END"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("IF(bigint0 >= 150000, 0, 1)"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("IF(bigint0 >= 150000, bigint0, 0)"), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression("COALESCE(0, bigint0) + bigint0"), 1);

        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 + (2 * bigint1)"), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression("NULLIF(bigint0, bigint1)"), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression("COALESCE(CEIL(bigint0 / bigint1), 0)"), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression("CASE WHEN (bigint0 > bigint1) THEN 1 ELSE 0 END"), 2);
        verifyEagerlyLoadedColumns(
                builder.buildExpression("CASE WHEN (bigint0 > 0) THEN bigint1 ELSE 0 END"), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression("COALESCE(ROUND(bigint0), bigint1)"), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 > 0 AND bigint1 > 0"), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 > 0 OR bigint1 > 0"), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression("bigint0 BETWEEN 0 AND bigint1"), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression("IF(bigint1 >= 150000, 0, bigint0)"), 2, ImmutableSet.of(0));

        builder = RowExpressionBuilder.create()
                .addSymbol("array_bigint0", new ArrayType(BIGINT))
                .addSymbol("array_bigint1", new ArrayType(BIGINT));
        verifyEagerlyLoadedColumns(builder.buildExpression("TRANSFORM(array_bigint0, x -> 1)"), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression("TRANSFORM(array_bigint0, x -> 2 * x)"), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression("ZIP_WITH(array_bigint0, array_bigint1, (x, y) -> 2 * x)"), 2, ImmutableSet.of());
    }

    private static void verifyEagerlyLoadedColumns(RowExpression rowExpression, int columnCount)
    {
        verifyEagerlyLoadedColumns(rowExpression, columnCount, IntStream.range(0, columnCount).boxed().collect(toImmutableSet()));
    }

    private static void verifyEagerlyLoadedColumns(RowExpression rowExpression, int columnCount, Set<Integer> eagerlyLoadedChannels)
    {
        Result result = rewritePageFieldsToInputParameters(rowExpression);
        Block[] blocks = new Block[columnCount];
        for (int channel = 0; channel < columnCount; channel++) {
            blocks[channel] = lazyWrapper(createLongSequenceBlock(0, 100));
        }
        Page page = result.getInputChannels().getInputChannels(new Page(blocks));
        for (int channel = 0; channel < columnCount; channel++) {
            assertThat(page.getBlock(channel).isLoaded()).isEqualTo(eagerlyLoadedChannels.contains(channel));
        }
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }

    private static class RowExpressionBuilder
    {
        private final Map<Symbol, Type> symbolTypes = new HashMap<>();
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();
        private final List<Type> types = new LinkedList<>();

        private static RowExpressionBuilder create()
        {
            return new RowExpressionBuilder();
        }

        private RowExpressionBuilder addSymbol(String name, Type type)
        {
            Symbol symbol = new Symbol(name);
            symbolTypes.put(symbol, type);
            sourceLayout.put(symbol, types.size());
            types.add(type);
            return this;
        }

        private RowExpression buildExpression(String value)
        {
            Expression expression = createExpression(value, PLANNER_CONTEXT, TypeProvider.copyOf(symbolTypes));

            return SqlToRowExpressionTranslator.translate(
                    expression,
                    TYPE_ANALYZER.getTypes(TEST_SESSION, TypeProvider.copyOf(symbolTypes), expression),
                    sourceLayout,
                    PLANNER_CONTEXT.getMetadata(),
                    PLANNER_CONTEXT.getFunctionManager(),
                    TEST_SESSION,
                    true);
        }
    }
}
