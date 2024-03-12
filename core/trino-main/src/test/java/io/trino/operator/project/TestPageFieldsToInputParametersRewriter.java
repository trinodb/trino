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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.WhenClause;
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
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MODULUS;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
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

    @Test
    public void testEagerLoading()
    {
        RowExpressionBuilder builder = RowExpressionBuilder.create()
                .addSymbol("bigint0", BIGINT)
                .addSymbol("bigint1", BIGINT);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ArithmeticBinaryExpression(ADD, new SymbolReference("bigint0"), new LongLiteral("5"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Cast(new ArithmeticBinaryExpression(MULTIPLY, new SymbolReference("bigint0"), new LongLiteral("10")), dataType("int"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new CoalesceExpression(new ArithmeticBinaryExpression(MODULUS, new SymbolReference("bigint0"), new LongLiteral("2")), new SymbolReference("bigint0"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new InPredicate(new SymbolReference("bigint0"), new InListExpression(ImmutableList.of(new GenericLiteral("bigint", "1"), new GenericLiteral("bigint", "2"), new GenericLiteral("bigint", "3"))))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new LongLiteral("0"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ComparisonExpression(EQUAL, new ArithmeticBinaryExpression(ADD, new SymbolReference("bigint0"), new LongLiteral("1")), new LongLiteral("0"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new BetweenPredicate(new SymbolReference("bigint0"), new LongLiteral("1"), new LongLiteral("10"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new LongLiteral("0")), new SymbolReference("bigint0"))), Optional.of(new Cast(new NullLiteral(), dataType("bigint"))))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new SimpleCaseExpression(new SymbolReference("bigint0"), ImmutableList.of(new WhenClause(new GenericLiteral("bigint", "1"), new GenericLiteral("bigint", "1"))), Optional.of(new ArithmeticUnaryExpression(MINUS, new SymbolReference("bigint0"))))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new IfExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("bigint0"), new LongLiteral("150000")), new LongLiteral("0"), new LongLiteral("1"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new IfExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("bigint0"), new GenericLiteral("bigint", "150000")), new SymbolReference("bigint0"), new GenericLiteral("bigint", "0"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new ArithmeticBinaryExpression(ADD, new CoalesceExpression(new GenericLiteral("bigint", "0"), new SymbolReference("bigint0")), new SymbolReference("bigint0"))), 1);

        verifyEagerlyLoadedColumns(builder.buildExpression(new ArithmeticBinaryExpression(ADD, new SymbolReference("bigint0"), new ArithmeticBinaryExpression(MULTIPLY, new GenericLiteral("BIGINT", "2"), new SymbolReference("bigint1")))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new NullIfExpression(new SymbolReference("bigint0"), new SymbolReference("bigint1"))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new CoalesceExpression(new FunctionCall(CEIL.toQualifiedName(), ImmutableList.of(new ArithmeticBinaryExpression(DIVIDE, new SymbolReference("bigint0"), new SymbolReference("bigint1")))), new GenericLiteral("BIGINT", "0"))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new SymbolReference("bigint1")), new LongLiteral("1"))), Optional.of(new LongLiteral("0")))), 2);
        verifyEagerlyLoadedColumns(
                builder.buildExpression(new SearchedCaseExpression(ImmutableList.of(new WhenClause(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new GenericLiteral("BIGINT", "0")), new SymbolReference("bigint1"))), Optional.of(new GenericLiteral("BIGINT", "0")))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new CoalesceExpression(new FunctionCall(ROUND.toQualifiedName(), ImmutableList.of(new SymbolReference("bigint0"))), new SymbolReference("bigint1"))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new GenericLiteral("BIGINT", "0")), new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint1"), new GenericLiteral("BIGINT", "0"))))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint0"), new GenericLiteral("BIGINT", "0")), new ComparisonExpression(GREATER_THAN, new SymbolReference("bigint1"), new GenericLiteral("BIGINT", "0"))))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new BetweenPredicate(new SymbolReference("bigint0"), new GenericLiteral("BIGINT", "0"), new SymbolReference("bigint1"))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new IfExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("bigint1"), new GenericLiteral("BIGINT", "150000")), new GenericLiteral("BIGINT", "0"), new SymbolReference("bigint0"))), 2, ImmutableSet.of(0));

        builder = RowExpressionBuilder.create()
                .addSymbol("array_bigint0", new ArrayType(BIGINT))
                .addSymbol("array_bigint1", new ArrayType(BIGINT));
        verifyEagerlyLoadedColumns(builder.buildExpression(new FunctionCall(TRANSFORM.toQualifiedName(), ImmutableList.of(new SymbolReference("array_bigint0"), new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(new Identifier("x"))), new LongLiteral("1"))))), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression(new FunctionCall(TRANSFORM.toQualifiedName(), ImmutableList.of(new SymbolReference("array_bigint0"), new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(new Identifier("x"))), new ArithmeticBinaryExpression(MULTIPLY, new LongLiteral("2"), new SymbolReference("x")))))), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression(new FunctionCall(ZIP_WITH.toQualifiedName(), ImmutableList.of(new SymbolReference("array_bigint0"), new SymbolReference("array_bigint1"), new LambdaExpression(ImmutableList.of(new LambdaArgumentDeclaration(new Identifier("x")), new LambdaArgumentDeclaration(new Identifier("y"))), new ArithmeticBinaryExpression(MULTIPLY, new GenericLiteral("BIGINT", "2"), new SymbolReference("x")))))), 2, ImmutableSet.of());
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
                    TYPE_ANALYZER.getTypes(TEST_SESSION, TypeProvider.copyOf(symbolTypes), expression),
                    sourceLayout,
                    PLANNER_CONTEXT.getMetadata(),
                    PLANNER_CONTEXT.getFunctionManager(),
                    PLANNER_CONTEXT.getTypeManager(),
                    TEST_SESSION,
                    true);
        }
    }
}
