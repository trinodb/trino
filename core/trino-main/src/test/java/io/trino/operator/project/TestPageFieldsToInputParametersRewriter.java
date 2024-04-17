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
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.transaction.TestingTransactionManager;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

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
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.plannerContextBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageFieldsToInputParametersRewriter
{
    private static final TestingTransactionManager TRANSACTION_MANAGER = new TestingTransactionManager();
    private static final PlannerContext PLANNER_CONTEXT = plannerContextBuilder()
            .withTransactionManager(TRANSACTION_MANAGER)
            .build();

    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction CEIL = FUNCTIONS.resolveFunction("ceil", fromTypes(BIGINT));
    private static final ResolvedFunction ROUND = FUNCTIONS.resolveFunction("round", fromTypes(BIGINT));
    private static final ResolvedFunction TRANSFORM = FUNCTIONS.resolveFunction("transform", fromTypes(new ArrayType(BIGINT), new FunctionType(ImmutableList.of(BIGINT), INTEGER)));
    private static final ResolvedFunction ZIP_WITH = FUNCTIONS.resolveFunction("zip_with", fromTypes(new ArrayType(BIGINT), new ArrayType(BIGINT), new FunctionType(ImmutableList.of(BIGINT, BIGINT), BIGINT)));
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction DIVIDE_BIGINT = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction MULTIPLY_INTEGER = FUNCTIONS.resolveOperator(OperatorType.MULTIPLY, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction MODULUS_BIGINT = FUNCTIONS.resolveOperator(OperatorType.MODULUS, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction NEGATION_BIGINT = FUNCTIONS.resolveOperator(OperatorType.NEGATION, ImmutableList.of(BIGINT));

    @Test
    public void testEagerLoading()
    {
        RowExpressionBuilder builder = RowExpressionBuilder.create()
                .addSymbol("bigint0", BIGINT)
                .addSymbol("bigint1", BIGINT);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 5L)))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Cast(new Call(MULTIPLY_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 10L))), INTEGER)), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Coalesce(new Call(MODULUS_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 2L))), new Reference(BIGINT, "bigint0"))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new In(new Reference(BIGINT, "bigint0"), ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 2L), new Constant(BIGINT, 3L)))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 0L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Comparison(EQUAL, new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 1L))), new Constant(BIGINT, 0L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Between(new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 1L), new Constant(BIGINT, 10L))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 0L)), new Reference(BIGINT, "bigint0"))), new Constant(BIGINT, null))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Switch(new Reference(BIGINT, "bigint0"), ImmutableList.of(new WhenClause(new Constant(BIGINT, 1L), new Constant(BIGINT, 1L))), new Call(NEGATION_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"))))), 1);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Call(ADD_BIGINT, ImmutableList.of(new Coalesce(new Constant(BIGINT, 0L), new Reference(BIGINT, "bigint0")), new Reference(BIGINT, "bigint0")))), 1);

        verifyEagerlyLoadedColumns(builder.buildExpression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Constant(BIGINT, 2L), new Reference(BIGINT, "bigint1")))))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new NullIf(new Reference(BIGINT, "bigint0"), new Reference(BIGINT, "bigint1"))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Coalesce(new Call(CEIL, ImmutableList.of(new Call(DIVIDE_BIGINT, ImmutableList.of(new Reference(BIGINT, "bigint0"), new Reference(BIGINT, "bigint1"))))), new Constant(BIGINT, 0L))), 2);
        verifyEagerlyLoadedColumns(builder.buildExpression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint0"), new Reference(BIGINT, "bigint1")), new Constant(INTEGER, 1L))), new Constant(INTEGER, 0L))), 2);
        verifyEagerlyLoadedColumns(
                builder.buildExpression(new Case(ImmutableList.of(new WhenClause(new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 0L)), new Reference(BIGINT, "bigint1"))), new Constant(BIGINT, 0L))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new Coalesce(new Call(ROUND, ImmutableList.of(new Reference(BIGINT, "bigint0"))), new Reference(BIGINT, "bigint1"))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 0L)), new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint1"), new Constant(BIGINT, 0L))))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 0L)), new Comparison(GREATER_THAN, new Reference(BIGINT, "bigint1"), new Constant(BIGINT, 0L))))), 2, ImmutableSet.of(0));
        verifyEagerlyLoadedColumns(builder.buildExpression(new Between(new Reference(BIGINT, "bigint0"), new Constant(BIGINT, 0L), new Reference(BIGINT, "bigint1"))), 2, ImmutableSet.of(0));

        builder = RowExpressionBuilder.create()
                .addSymbol("array_bigint0", new ArrayType(BIGINT))
                .addSymbol("array_bigint1", new ArrayType(BIGINT));
        verifyEagerlyLoadedColumns(builder.buildExpression(new Call(TRANSFORM, ImmutableList.of(new Reference(new ArrayType(BIGINT), "array_bigint0"), new Lambda(ImmutableList.of(new Symbol(BIGINT, "x")), new Constant(INTEGER, 1L))))), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression(new Call(TRANSFORM, ImmutableList.of(new Reference(new ArrayType(BIGINT), "array_bigint0"), new Lambda(ImmutableList.of(new Symbol(BIGINT, "x")), new Call(MULTIPLY_INTEGER, ImmutableList.of(new Constant(INTEGER, 2L), new Reference(INTEGER, "x"))))))), 1, ImmutableSet.of());
        verifyEagerlyLoadedColumns(builder.buildExpression(new Call(ZIP_WITH, ImmutableList.of(new Reference(new ArrayType(BIGINT), "array_bigint0"), new Reference(new ArrayType(BIGINT), "array_bigint1"), new Lambda(ImmutableList.of(new Symbol(BIGINT, "x"), new Symbol(BIGINT, "y")), new Call(MULTIPLY_BIGINT, ImmutableList.of(new Constant(BIGINT, 2L), new Reference(BIGINT, "x"))))))), 2, ImmutableSet.of());
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
        private final Map<Symbol, Integer> sourceLayout = new HashMap<>();
        private final List<Type> types = new LinkedList<>();

        private static RowExpressionBuilder create()
        {
            return new RowExpressionBuilder();
        }

        private RowExpressionBuilder addSymbol(String name, Type type)
        {
            Symbol symbol = new Symbol(type, name);
            sourceLayout.put(symbol, types.size());
            types.add(type);
            return this;
        }

        private RowExpression buildExpression(Expression expression)
        {
            return SqlToRowExpressionTranslator.translate(
                    expression,
                    sourceLayout,
                    PLANNER_CONTEXT.getMetadata(),
                    PLANNER_CONTEXT.getTypeManager());
        }
    }
}
