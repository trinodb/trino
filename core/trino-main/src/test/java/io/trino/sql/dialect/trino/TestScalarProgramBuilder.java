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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Context.RowField;
import io.trino.sql.dialect.trino.ProgramBuilder.ValueNameAllocator;
import io.trino.sql.dialect.trino.operation.Array;
import io.trino.sql.dialect.trino.operation.Between;
import io.trino.sql.dialect.trino.operation.Bind;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Case;
import io.trino.sql.dialect.trino.operation.Cast;
import io.trino.sql.dialect.trino.operation.Coalesce;
import io.trino.sql.dialect.trino.operation.Comparison;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.In;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.NullIf;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Switch;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.planner.Symbol;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.trino.Attributes.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.dialect.trino.Attributes.ComparisonOperator.LESS_THAN;
import static io.trino.sql.dialect.trino.Attributes.LogicalOperator.AND;
import static io.trino.sql.dialect.trino.Attributes.LogicalOperator.OR;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestScalarProgramBuilder
{
    private static final Block.Parameter INPUT_ROW_PARAMETER = new Block.Parameter(
            "%input_row",
            irType(anonymousRow(BIGINT, BOOLEAN)));

    private static final Map<Symbol, RowField> SYMBOL_MAPPING = ImmutableMap.of(
            new Symbol(BIGINT, "a"), new RowField(INPUT_ROW_PARAMETER, 0),
            new Symbol(BOOLEAN, "b"), new RowField(INPUT_ROW_PARAMETER, 1));

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testArray()
    {
        io.trino.sql.ir.Array arrayExpression = new io.trino.sql.ir.Array(
                BOOLEAN,
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, false)));

        Constant constantOperation1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Constant constantOperation3 = new Constant("%2", BOOLEAN, false);
        Array arrayOperation = new Array(
                "%3",
                BOOLEAN,
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                arrayExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        arrayOperation),
                new ArrayType(BOOLEAN));
    }

    @Test
    public void testBetween()
    {
        io.trino.sql.ir.Between betweenExpression = new io.trino.sql.ir.Between(
                new io.trino.sql.ir.Constant(BIGINT, 0L),
                new io.trino.sql.ir.Constant(BIGINT, 1L),
                new io.trino.sql.ir.Constant(BIGINT, 2L));

        Constant constantOperationValue = new Constant("%0", BIGINT, 0L);
        Constant constantOperationMin = new Constant("%1", BIGINT, 1L);
        Constant constantOperationMax = new Constant("%2", BIGINT, 2L);
        Between betweenOperation = new Between(
                "%3",
                constantOperationValue.result(),
                constantOperationMin.result(),
                constantOperationMax.result(),
                ImmutableList.of(constantOperationValue.attributes(), constantOperationMin.attributes(), constantOperationMax.attributes()));

        assertProgram(
                betweenExpression,
                ImmutableList.of(
                        constantOperationValue,
                        constantOperationMin,
                        constantOperationMax,
                        betweenOperation),
                BOOLEAN);
    }

    @Test
    public void testBind()
    {
        io.trino.sql.ir.Bind bindExpression = new io.trino.sql.ir.Bind(
                ImmutableList.of(new io.trino.sql.ir.Reference(BIGINT, "a")),
                new io.trino.sql.ir.Lambda(
                        ImmutableList.of(new Symbol(BIGINT, "x")),
                        new io.trino.sql.ir.Comparison(
                                io.trino.sql.ir.Comparison.Operator.LESS_THAN,
                                new io.trino.sql.ir.Reference(BIGINT, "x"),
                                new io.trino.sql.ir.Constant(BIGINT, 0L))));

        FieldReference fieldReferenceOperationA = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, ImmutableMap.of());
        Block.Parameter lambdaArgument = new Block.Parameter("%2", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperationX = new FieldReference("%3", lambdaArgument, 0, ImmutableMap.of());
        Constant constantOperation = new Constant("%4", BIGINT, 0L);
        Comparison comparisonOperation = new Comparison(
                "%5",
                fieldReferenceOperationX.result(),
                constantOperation.result(),
                LESS_THAN,
                ImmutableList.of(fieldReferenceOperationX.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%6", comparisonOperation.result(), comparisonOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%1",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperationX,
                                constantOperation,
                                comparisonOperation,
                                returnOperation)));
        Bind bindOperation = new Bind(
                "%7",
                ImmutableList.of(fieldReferenceOperationA.result()),
                lambdaOperation.result(),
                ImmutableList.of(fieldReferenceOperationA.attributes(), lambdaOperation.attributes()));

        assertProgram(
                bindExpression,
                ImmutableList.of(
                        fieldReferenceOperationA,
                        lambdaOperation,
                        bindOperation),
                new FunctionType(ImmutableList.of(), BOOLEAN));
    }

    @Test
    public void testCallWithoutArguments()
    {
        ResolvedFunction randomFunction = FUNCTION_RESOLUTION.resolveFunction("random", fromTypes());

        io.trino.sql.ir.Call callExpression = new io.trino.sql.ir.Call(randomFunction, ImmutableList.of());

        Call callOperation = new Call("%0", ImmutableList.of(), randomFunction, ImmutableList.of());

        assertProgram(callExpression, ImmutableList.of(callOperation), DOUBLE);
    }

    @Test
    public void testCallWithArguments()
    {
        ResolvedFunction addOperator = FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));

        io.trino.sql.ir.Call callExpression = new io.trino.sql.ir.Call(
                addOperator,
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BIGINT, 1L),
                        new io.trino.sql.ir.Constant(BIGINT, 2L)));

        Constant constantOperation1 = new Constant("%0", BIGINT, 1L);
        Constant constantOperation2 = new Constant("%1", BIGINT, 2L);
        Call callOperation = new Call(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                addOperator,
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));

        assertProgram(
                callExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        callOperation),
                BIGINT);
    }

    @Test
    public void testCase()
    {
        io.trino.sql.ir.Case caseExpression = new io.trino.sql.ir.Case(
                ImmutableList.of(
                        new WhenClause(new io.trino.sql.ir.Constant(BOOLEAN, true), new io.trino.sql.ir.Constant(BIGINT, 0L)),
                        new WhenClause(new io.trino.sql.ir.Constant(BOOLEAN, false), new io.trino.sql.ir.Constant(BIGINT, 1L))),
                new io.trino.sql.ir.Constant(BIGINT, 2L));

        Constant constantOperationWhen1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperationWhen2 = new Constant("%1", BOOLEAN, false);
        Constant constantOperationThen1 = new Constant("%2", BIGINT, 0L);
        Constant constantOperationThen2 = new Constant("%3", BIGINT, 1L);
        Constant constantOperationDefault = new Constant("%4", BIGINT, 2L);
        Case caseOperation = new Case(
                "%5",
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result()),
                ImmutableList.of(constantOperationThen1.result(), constantOperationThen2.result()),
                constantOperationDefault.result(),
                ImmutableList.of(
                        constantOperationWhen1.attributes(),
                        constantOperationWhen2.attributes(),
                        constantOperationThen1.attributes(),
                        constantOperationThen2.attributes(),
                        constantOperationDefault.attributes()));

        assertProgram(
                caseExpression,
                ImmutableList.of(
                        constantOperationWhen1,
                        constantOperationWhen2,
                        constantOperationThen1,
                        constantOperationThen2,
                        constantOperationDefault,
                        caseOperation),
                BIGINT);
    }

    @Test
    public void testCast()
    {
        io.trino.sql.ir.Cast castExpression = new io.trino.sql.ir.Cast(new io.trino.sql.ir.Constant(SMALLINT, 1L), BIGINT);

        Constant constantOperation = new Constant("%0", SMALLINT, 1L);
        Cast castOperation = new Cast(
                "%1",
                constantOperation.result(),
                BIGINT,
                constantOperation.attributes());

        assertProgram(
                castExpression,
                ImmutableList.of(
                        constantOperation,
                        castOperation),
                BIGINT);
    }

    @Test
    public void testCoalesce()
    {
        io.trino.sql.ir.Coalesce coalesceExpression = new io.trino.sql.ir.Coalesce(
                new io.trino.sql.ir.Constant(BIGINT, null),
                new io.trino.sql.ir.Constant(BIGINT, null),
                new io.trino.sql.ir.Constant(BIGINT, 1L));

        Constant constantOperation1 = new Constant("%0", BIGINT, null);
        Constant constantOperation2 = new Constant("%1", BIGINT, null);
        Constant constantOperation3 = new Constant("%2", BIGINT, 1L);
        Coalesce coalesceOperation = new Coalesce(
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                coalesceExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        coalesceOperation),
                BIGINT);
    }

    @Test
    public void testComparison()
    {
        io.trino.sql.ir.Comparison comparisonExpression = new io.trino.sql.ir.Comparison(
                io.trino.sql.ir.Comparison.Operator.GREATER_THAN,
                new io.trino.sql.ir.Constant(BIGINT, 0L),
                new io.trino.sql.ir.Constant(BIGINT, 1L));

        Constant constantOperationLeft = new Constant("%0", BIGINT, 0L);
        Constant constantOperationRight = new Constant("%1", BIGINT, 1L);
        Comparison comparisonOperation = new Comparison(
                "%2",
                constantOperationLeft.result(),
                constantOperationRight.result(),
                GREATER_THAN,
                ImmutableList.of(constantOperationLeft.attributes(), constantOperationRight.attributes()));

        assertProgram(
                comparisonExpression,
                ImmutableList.of(
                        constantOperationLeft,
                        constantOperationRight,
                        comparisonOperation),
                BOOLEAN);
    }

    @Test
    public void testConstant()
    {
        io.trino.sql.ir.Constant constantExpression = new io.trino.sql.ir.Constant(BOOLEAN, true);

        Constant constantOperation = new Constant("%0", BOOLEAN, true);

        assertProgram(constantExpression, ImmutableList.of(constantOperation), BOOLEAN);
    }

    @Test
    public void testConstantNull()
    {
        io.trino.sql.ir.Constant constantExpression = new io.trino.sql.ir.Constant(BOOLEAN, null);

        Constant constantOperation = new Constant("%0", BOOLEAN, null);

        assertProgram(constantExpression, ImmutableList.of(constantOperation), BOOLEAN);
    }

    @Test
    public void testFieldReference()
    {
        io.trino.sql.ir.FieldReference fieldReferenceExpression = new io.trino.sql.ir.FieldReference(
                new io.trino.sql.ir.Row(
                        ImmutableList.of(
                                new io.trino.sql.ir.Constant(BIGINT, 0L),
                                new io.trino.sql.ir.Constant(BOOLEAN, true))),
                0);

        Constant constantOperation1 = new Constant("%0", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));
        FieldReference fieldReferenceOperation = new FieldReference(
                "%3",
                rowOperation.result(),
                0,
                rowOperation.attributes());

        assertProgram(
                fieldReferenceExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        rowOperation,
                        fieldReferenceOperation),
                BIGINT);
    }

    @Test
    public void testIn()
    {
        io.trino.sql.ir.In inExpression = new io.trino.sql.ir.In(
                new io.trino.sql.ir.Constant(BIGINT, 1L),
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BIGINT, 0L),
                        new io.trino.sql.ir.Constant(BIGINT, 1L),
                        new io.trino.sql.ir.Constant(BIGINT, 2L)));

        Constant constantOperationValue = new Constant("%0", BIGINT, 1L);
        Constant constantOperation1 = new Constant("%1", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%2", BIGINT, 1L);
        Constant constantOperation3 = new Constant("%3", BIGINT, 2L);
        In inOperation = new In(
                "%4",
                constantOperationValue.result(),
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperationValue.attributes(), constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                inExpression,
                ImmutableList.of(
                        constantOperationValue,
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        inOperation),
                BOOLEAN);
    }

    @Test
    public void testIsNull()
    {
        io.trino.sql.ir.IsNull isNullExpression = new io.trino.sql.ir.IsNull(new io.trino.sql.ir.Constant(BIGINT, null));

        Constant constantOperation = new Constant("%0", BIGINT, null);
        IsNull isNullOperation = new IsNull("%1", constantOperation.result(), constantOperation.attributes());

        assertProgram(
                isNullExpression,
                ImmutableList.of(
                        constantOperation,
                        isNullOperation),
                BOOLEAN);
    }

    @Test
    public void testLambdaWithoutArguments()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(),
                new io.trino.sql.ir.Constant(BIGINT, 5L));

        Constant constantOperation = new Constant("%2", BIGINT, 5L);
        Return returnOperation = new Return("%3", constantOperation.result(), constantOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(new Block.Parameter("%1", irType(EMPTY_ROW))),
                        ImmutableList.of(
                                constantOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(), BIGINT));
    }

    @Test
    public void testSimpleLambda()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(new Symbol(BIGINT, "x")),
                new io.trino.sql.ir.Comparison(
                        io.trino.sql.ir.Comparison.Operator.LESS_THAN,
                        new io.trino.sql.ir.Reference(BIGINT, "x"),
                        new io.trino.sql.ir.Constant(BIGINT, 0L)));

        Block.Parameter lambdaArgument = new Block.Parameter("%1", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperationX = new FieldReference("%2", lambdaArgument, 0, ImmutableMap.of());
        Constant constantOperation = new Constant("%3", BIGINT, 0L);
        Comparison comparisonOperation = new Comparison(
                "%4",
                fieldReferenceOperationX.result(),
                constantOperation.result(),
                LESS_THAN,
                ImmutableList.of(fieldReferenceOperationX.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%5", comparisonOperation.result(), comparisonOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperationX,
                                constantOperation,
                                comparisonOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(BIGINT), BOOLEAN));
    }

    @Test
    public void testCorrelatedLambda()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(
                        new Symbol(BOOLEAN, "x"),
                        new Symbol(BIGINT, "y")),
                new io.trino.sql.ir.Logical(
                        io.trino.sql.ir.Logical.Operator.OR,
                        ImmutableList.of(
                                new io.trino.sql.ir.Reference(BOOLEAN, "b"), // correlated symbol
                                new io.trino.sql.ir.Reference(BOOLEAN, "x"), // lambda argument
                                new io.trino.sql.ir.Comparison(
                                        io.trino.sql.ir.Comparison.Operator.LESS_THAN,
                                        new io.trino.sql.ir.Reference(BIGINT, "a"), // correlated symbol
                                        new io.trino.sql.ir.Reference(BIGINT, "y"))))); // lambda argument

        Block.Parameter lambdaArgument = new Block.Parameter("%1", irType(anonymousRow(BOOLEAN, BIGINT)));
        FieldReference fieldReferenceOperationB = new FieldReference("%2", INPUT_ROW_PARAMETER, 1, ImmutableMap.of());
        FieldReference fieldReferenceOperationX = new FieldReference("%3", lambdaArgument, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationA = new FieldReference("%4", INPUT_ROW_PARAMETER, 0, ImmutableMap.of());
        FieldReference fieldReferenceOperationY = new FieldReference("%5", lambdaArgument, 1, ImmutableMap.of());
        Comparison comparisonOperation = new Comparison(
                "%6",
                fieldReferenceOperationA.result(),
                fieldReferenceOperationY.result(),
                LESS_THAN,
                ImmutableList.of(fieldReferenceOperationA.attributes(), fieldReferenceOperationY.attributes()));
        Logical logicalOperation = new Logical(
                "%7",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationX.result(), comparisonOperation.result()),
                OR,
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationX.attributes(), comparisonOperation.attributes()));
        Return returnOperation = new Return("%8", logicalOperation.result(), logicalOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperationB,
                                fieldReferenceOperationX,
                                fieldReferenceOperationA,
                                fieldReferenceOperationY,
                                comparisonOperation,
                                logicalOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(BOOLEAN, BIGINT), BOOLEAN));
    }

    @Test
    public void testLambdaDuplicateArguments()
    {
        io.trino.sql.ir.Lambda lambdaExpression = new io.trino.sql.ir.Lambda(
                ImmutableList.of(
                        new Symbol(BOOLEAN, "x"),
                        new Symbol(BOOLEAN, "x")),
                new io.trino.sql.ir.Reference(BOOLEAN, "x"));

        Block.Parameter lambdaArgument = new Block.Parameter("%1", irType(anonymousRow(BOOLEAN, BOOLEAN)));
        FieldReference fieldReferenceOperation = new FieldReference("%2", lambdaArgument, 0, ImmutableMap.of());
        Return returnOperation = new Return("%3", fieldReferenceOperation.result(), fieldReferenceOperation.attributes());
        Lambda lambdaOperation = new Lambda(
                "%0",
                new Block(
                        Optional.of("^lambda"),
                        ImmutableList.of(lambdaArgument),
                        ImmutableList.of(
                                fieldReferenceOperation,
                                returnOperation)));

        assertProgram(lambdaExpression, ImmutableList.of(lambdaOperation), new FunctionType(ImmutableList.of(BOOLEAN, BOOLEAN), BOOLEAN));
    }

    @Test
    public void testLogical()
    {
        io.trino.sql.ir.Logical logicalExpression = new io.trino.sql.ir.Logical(
                io.trino.sql.ir.Logical.Operator.AND,
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, true),
                        new io.trino.sql.ir.Constant(BOOLEAN, false)));

        Constant constantOperation1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Constant constantOperation3 = new Constant("%2", BOOLEAN, false);
        Logical logicalOperation = new Logical(
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                AND,
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        assertProgram(
                logicalExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        constantOperation3,
                        logicalOperation),
                BOOLEAN);
    }

    @Test
    public void testNullIf()
    {
        io.trino.sql.ir.NullIf nullIfExpression = new io.trino.sql.ir.NullIf(
                new io.trino.sql.ir.Constant(BIGINT, 0L),
                new io.trino.sql.ir.Constant(SMALLINT, 1L));

        Constant constantOperationFirst = new Constant("%0", BIGINT, 0L);
        Constant constantOperationSecond = new Constant("%1", SMALLINT, 1L);
        NullIf nullIfOperation = new NullIf(
                "%2",
                constantOperationFirst.result(),
                constantOperationSecond.result(),
                ImmutableList.of(constantOperationFirst.attributes(), constantOperationSecond.attributes()));

        assertProgram(
                nullIfExpression,
                ImmutableList.of(
                        constantOperationFirst,
                        constantOperationSecond,
                        nullIfOperation),
                BIGINT);
    }

    @Test
    public void testReference()
    {
        io.trino.sql.ir.Reference referenceExpression = new io.trino.sql.ir.Reference(BIGINT, "a");

        FieldReference fieldReferenceOperation = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, ImmutableMap.of());

        assertProgram(referenceExpression, ImmutableList.of(fieldReferenceOperation), BIGINT);
    }

    @Test
    public void testRow()
    {
        io.trino.sql.ir.Row rowExpression = new io.trino.sql.ir.Row(
                ImmutableList.of(
                        new io.trino.sql.ir.Constant(BIGINT, 0L),
                        new io.trino.sql.ir.Constant(BOOLEAN, true)));

        Constant constantOperation1 = new Constant("%0", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));

        assertProgram(
                rowExpression,
                ImmutableList.of(
                        constantOperation1,
                        constantOperation2,
                        rowOperation),
                anonymousRow(BIGINT, BOOLEAN));
    }

    @Test
    public void testSwitch()
    {
        io.trino.sql.ir.Switch switchExpression = new io.trino.sql.ir.Switch(
                new io.trino.sql.ir.Constant(BIGINT, 0L),
                ImmutableList.of(
                        new WhenClause(new io.trino.sql.ir.Constant(BIGINT, 1L), new io.trino.sql.ir.Constant(BOOLEAN, true)),
                        new WhenClause(new io.trino.sql.ir.Constant(BIGINT, 2L), new io.trino.sql.ir.Constant(BOOLEAN, false))),
                new io.trino.sql.ir.Constant(BOOLEAN, null));

        Constant constantOperationOperand = new Constant("%0", BIGINT, 0L);
        Constant constantOperationWhen1 = new Constant("%1", BIGINT, 1L);
        Constant constantOperationWhen2 = new Constant("%2", BIGINT, 2L);
        Constant constantOperationThen1 = new Constant("%3", BOOLEAN, true);
        Constant constantOperationThen2 = new Constant("%4", BOOLEAN, false);
        Constant constantOperationDefault = new Constant("%5", BOOLEAN, null);
        Switch switchOperation = new Switch(
                "%6",
                constantOperationOperand.result(),
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result()),
                ImmutableList.of(constantOperationThen1.result(), constantOperationThen2.result()),
                constantOperationDefault.result(),
                ImmutableList.of(
                        constantOperationOperand.attributes(),
                        constantOperationWhen1.attributes(),
                        constantOperationWhen2.attributes(),
                        constantOperationThen1.attributes(),
                        constantOperationThen2.attributes(),
                        constantOperationDefault.attributes()));

        assertProgram(
                switchExpression,
                ImmutableList.of(
                        constantOperationOperand,
                        constantOperationWhen1,
                        constantOperationWhen2,
                        constantOperationThen1,
                        constantOperationThen2,
                        constantOperationDefault,
                        switchOperation),
                BOOLEAN);
    }

    @Test
    public void testNoMappingForSymbol()
    {
        io.trino.sql.ir.Reference referenceExpression = new io.trino.sql.ir.Reference(BIGINT, "A");
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));

        // SYMBOL_MAPPING has entries for symbols "a" and "b", but not for "A"
        assertThatThrownBy(() -> referenceExpression.accept(scalarProgramBuilder, new Context(blockBuilder, SYMBOL_MAPPING)))
                .hasMessage("no mapping for symbol A");
    }

    @Test
    public void testAddReturnOperation()
    {
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));

        io.trino.sql.ir.Constant constantExpression = new io.trino.sql.ir.Constant(BOOLEAN, true);
        constantExpression.accept(scalarProgramBuilder, new Context(blockBuilder, SYMBOL_MAPPING));
        scalarProgramBuilder.addReturnOperation(blockBuilder);

        Constant constantOperation = new Constant("%0", BOOLEAN, true);
        Return returnOperation = new Return("%1", constantOperation.result(), constantOperation.attributes());

        assertThat(blockBuilder.build().operations()).isEqualTo(ImmutableList.of(constantOperation, returnOperation));
    }

    @Test
    public void testAddReturnOperationEmptyBlock()
    {
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));

        assertThatThrownBy(() -> scalarProgramBuilder.addReturnOperation(blockBuilder))
                .hasMessage("no operations added yet");
    }

    private void assertProgram(Expression expression, List<Operation> expected, Type expectedType)
    {
        ScalarProgramBuilder scalarProgramBuilder = new ScalarProgramBuilder(new ValueNameAllocator(), ImmutableMap.builder());
        Block.Builder blockBuilder = new Block.Builder(Optional.empty(), ImmutableList.of(INPUT_ROW_PARAMETER));
        expression.accept(scalarProgramBuilder, new Context(blockBuilder, SYMBOL_MAPPING));
        // add a terminal Return operation. It is required to build the Block
        scalarProgramBuilder.addReturnOperation(blockBuilder);
        Block block = blockBuilder.build();

        // remove the Return operation
        List<Operation> actual = block.operations().subList(0, block.operations().size() - 1);
        assertThat(actual).isEqualTo(expected);

        assertThat(expression.type()).isEqualTo(expectedType);
        assertThat(trinoType(block.getReturnedType())).isEqualTo(expectedType);
    }
}
