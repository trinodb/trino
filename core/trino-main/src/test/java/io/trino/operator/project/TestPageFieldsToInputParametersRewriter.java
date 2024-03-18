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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.ArithmeticUnaryExpression;
import io.trino.sql.ir.BetweenPredicate;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.CoalesceExpression;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FunctionCall;
import io.trino.sql.ir.IfExpression;
import io.trino.sql.ir.InPredicate;
import io.trino.sql.ir.LambdaExpression;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NullIfExpression;
import io.trino.sql.ir.SearchedCaseExpression;
import io.trino.sql.ir.SimpleCaseExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.transaction.TestingTransactionManager;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.Result;
import static io.trino.operator.project.PageFieldsToInputParametersRewriter.rewritePageFieldsToInputParameters;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.DIVIDE;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.ir.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageFieldsToInputParametersRewriter
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();
    private static final IrTypeAnalyzer TYPE_ANALYZER = new IrTypeAnalyzer(PLANNER_CONTEXT);

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction CEIL = FUNCTIONS.resolveFunction("ceil", fromTypes(BIGINT));
    private static final ResolvedFunction ROUND = FUNCTIONS.resolveFunction("round", fromTypes(BIGINT));
    private static final ResolvedFunction TRANSFORM = FUNCTIONS.resolveFunction("transform", fromTypes(new ArrayType(BIGINT), new FunctionType(ImmutableList.of(BIGINT), INTEGER)));
    private static final ResolvedFunction ZIP_WITH = FUNCTIONS.resolveFunction("zip_with", fromTypes(new ArrayType(BIGINT), new ArrayType(BIGINT), new FunctionType(ImmutableList.of(BIGINT, BIGINT), INTEGER)));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MODULUS_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(INTEGER, INTEGER));

    @Test
    public void testEagerLoading()
    {
        RowExpressionBuilder builder = RowExpressionBuilder.create()
                .addSymbol("bigint0", BIGINT)
                .addSymbol("bigint1", BIGINT);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ArithmeticBinaryExpression(ADD_INTEGER, ADD, new SymbolReference("bigint0"), new Constant(INTEGER, 5L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Cast(new ArithmeticBinaryExpression(MULTIPLY_INTEGER, MULTIPLY, new SymbolReference("bigint0"), new Constant(INTEGER, 10L)), INTEGER)), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new CoalesceExpression(new ArithmeticBinaryExpression(MODULUS_INTEGER, MODULUS, new SymbolReference("bigint0"), new Constant(INTEGER, 2L)), new SymbolReference("bigint0"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new InPredicate(new SymbolReference("bigint0"), ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L), new Constant(BIGINT, 3L)))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new Constant(INTEGER, 0L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference("bigint0"), new Constant(BIGINT, 1L)), new Constant(BIGINT, 0L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new BetweenPredicate(new SymbolReference("bigint0"), new Constant(INTEGER, 1L), new Constant(INTEGER, 10L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new Constant(INTEGER, 0L)), new SymbolReference("bigint0"))), Optional.of(new Constant(BIGINT, null)))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new SimpleCaseExpression(new SymbolReference("bigint0"), ImmutableList.of(new WhenClause(new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))), Optional.of(new ArithmeticUnaryExpression(MINUS, new SymbolReference("bigint0"))))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new IfExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("bigint0"), new Constant(INTEGER, 150000L)), new Constant(INTEGER, 0L), new Constant(INTEGER, 1L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new IfExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("bigint0"), new Constant(BIGINT, 150000L)), new SymbolReference("bigint0"), new Constant(BIGINT, 0L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new CoalesceExpression(new Constant(BIGINT, 0L), new SymbolReference("bigint0")), new SymbolReference("bigint0"))), 1);

        verifyEagerlyLoadedColumns(builder.buildExpression(new ArithmeticBinaryExpression(ADD_BIGINT, ADD, new SymbolReference("bigint0"), new ArithmeticBinaryExpression(MULTIPLY_BIGINT, MULTIPLY, new Constant(BIGINT, 2L), new SymbolReference("bigint1")))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new NullIfExpression(new SymbolReference("bigint0"), new SymbolReference("bigint1"))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new CoalesceExpression(new FunctionCall(CEIL, ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE_BIGINT, DIVIDE, new SymbolReference("bigint0"), new SymbolReference("bigint1")))), new Constant(BIGINT, 0L))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new SymbolReference("bigint1")), new Constant(INTEGER, 1L))), Optional.of(new Constant(INTEGER, 0L)))), 2);
        verifyEagerlyLoadedColumns(
                builder.buildExpression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new Constant(BIGINT, 0L)), new SymbolReference("bigint1"))), Optional.of(new Constant(BIGINT, 0L)))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new CoalesceExpression(new FunctionCall(ROUND, ImmutableList.of(new SymbolReference("bigint0"))), new SymbolReference("bigint1"))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new Constant(BIGINT, 0L)), new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint1"), new Constant(BIGINT, 0L))))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new Constant(BIGINT, 0L)), new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint1"), new Constant(BIGINT, 0L))))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new BetweenPredicate(new SymbolReference("bigint0"), new Constant(BIGINT, 0L), new SymbolReference("bigint1"))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new IfExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("bigint1"), new Constant(BIGINT, 150000L)), new Constant(BIGINT, 0L), new SymbolReference("bigint0"))), 2, ImmutableSet.of(0));

        builder = RowExpressionBuilder.create()
                .addSymbol("array_bigint0", new ArrayType(BIGINT))
                .addSymbol("array_bigint1", new ArrayType(BIGINT));
        verifyEagerlyLoadedColumns(builder.buildExpression(new FunctionCall(TRANSFORM, ImmutableList.of(new SymbolReference("array_bigint0"), new LambdaExpression(ImmutableList.of("x"), new Constant(INTEGER, 1L))))), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression(new FunctionCall(TRANSFORM, ImmutableList.of(new SymbolReference("array_bigint0"), new LambdaExpression(ImmutableList.of("x"), new ArithmeticBinaryExpression(MULTIPLY_INTEGER, MULTIPLY, new Constant(INTEGER, 2L), new SymbolReference("x")))))), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression(new FunctionCall(ZIP_WITH, ImmutableList.of(new SymbolReference("array_bigint0"), new SymbolReference("array_bigint1"), new LambdaExpression(ImmutableList.of("x", "y"), new ArithmeticBinaryExpression(MULTIPLY_BIGINT, MULTIPLY, new Constant(BIGINT, 2L), new SymbolReference("x")))))), 2, ImmutableSet.of());
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

        private RowExpression buildExpression(Expression expression)
        {
            return SqlToRowExpressionTranslator.translate(
                    expression,
                    TYPE_ANALYZER.getTypes(TypeProvider.copyOf(symbolTypes), expression),
                    sourceLayout,
                    PLANNER_CONTEXT.getMetadata(),
                    PLANNER_CONTEXT.getFunctionManager(),
                    PLANNER_CONTEXT.getTypeManager(),
                    TEST_SESSION,
                    true);
        }
    }
}
