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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operation.Comparison;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldSelection;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.ir.Reference;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.dialect.trino.Attributes.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TestingFormatOptions.TESTING_FORMAT_OPTIONS;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static org.assertj.core.api.Assertions.assertThat;

final class TestValueMap
{
    @Test
    public void testValueMap()
    {
        OutputNode plan =
                new OutputNode(
                        new PlanNodeId("output"),
                        new FilterNode(
                                new PlanNodeId("filter"),
                                new ValuesNode(
                                        new PlanNodeId("values"),
                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                        ImmutableList.of(
                                                new io.trino.sql.ir.Row(ImmutableList.of(
                                                        new io.trino.sql.ir.Constant(BIGINT, 1L),
                                                        new io.trino.sql.ir.Constant(BOOLEAN, true))),
                                                new io.trino.sql.ir.Row(ImmutableList.of(
                                                        new io.trino.sql.ir.Constant(BIGINT, 2L),
                                                        new io.trino.sql.ir.Constant(BOOLEAN, false))))),
                                new io.trino.sql.ir.Comparison(
                                        io.trino.sql.ir.Comparison.Operator.GREATER_THAN,
                                        new Reference(BIGINT, "a"),
                                        new io.trino.sql.ir.Constant(BIGINT, 5L))),
                        ImmutableList.of("col_b", "col_a"),
                        ImmutableList.of(new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "a")));

        // operations of the program
        Constant constantOperation1Row1 = new Constant("%2", BIGINT, 1L);
        Constant constantOperation2Row1 = new Constant("%3", BOOLEAN, true);
        Row rowOperation1 = new Row(
                "%4",
                ImmutableList.of(constantOperation1Row1.result(), constantOperation2Row1.result()),
                ImmutableList.of(constantOperation1Row1.attributes(), constantOperation2Row1.attributes()));
        Return returnOperationRow1 = new Return("%5", rowOperation1.result(), rowOperation1.attributes());
        Constant constantOperation1Row2 = new Constant("%6", BIGINT, 2L);
        Constant constantOperation2Row2 = new Constant("%7", BOOLEAN, false);
        Row rowOperation2 = new Row(
                "%8",
                ImmutableList.of(constantOperation1Row2.result(), constantOperation2Row2.result()),
                ImmutableList.of(constantOperation1Row2.attributes(), constantOperation2Row2.attributes()));
        Return returnOperationRow2 = new Return("%9", rowOperation2.result(), rowOperation2.attributes());

        Values valuesOperation = new Values(
                "%1",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row1,
                                        constantOperation2Row1,
                                        rowOperation1,
                                        returnOperationRow1)),
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row2,
                                        constantOperation2Row2,
                                        rowOperation2,
                                        returnOperationRow2))));

        Block.Parameter predicateParameter = new Block.Parameter(
                "%11",
                irType(relationRowType(trinoType(valuesOperation.result().type()))));
        FieldSelection fieldSelectionOperationPredicate = new FieldSelection("%12", predicateParameter, "f_1", ImmutableMap.of());
        Constant constantOperationPredicate = new Constant("%13", BIGINT, 5L);
        Comparison comparisonOperationPredicate = new Comparison(
                "%14",
                fieldSelectionOperationPredicate.result(),
                constantOperationPredicate.result(),
                GREATER_THAN,
                ImmutableList.of(fieldSelectionOperationPredicate.attributes(), constantOperationPredicate.attributes()));
        Return returnOperationPredicate = new Return("%15", comparisonOperationPredicate.result(), comparisonOperationPredicate.attributes());
        Block predicateBlock = new Block(
                Optional.of("^predicate"),
                ImmutableList.of(predicateParameter),
                ImmutableList.of(
                        fieldSelectionOperationPredicate,
                        constantOperationPredicate,
                        comparisonOperationPredicate,
                        returnOperationPredicate));

        Filter filterOperation = new Filter(
                "%10",
                valuesOperation.result(),
                predicateBlock,
                valuesOperation.attributes());

        Block.Parameter fieldSelectionParameter = new Block.Parameter(
                "%17",
                irType(relationRowType(trinoType(filterOperation.result().type()))));
        FieldSelection fieldSelectionOperationB = new FieldSelection("%18", fieldSelectionParameter, "f_2", ImmutableMap.of());
        FieldSelection fieldSelectionOperationA = new FieldSelection("%19", fieldSelectionParameter, "f_1", ImmutableMap.of());
        Row rowOperationFieldSelector = new Row(
                "%20",
                ImmutableList.of(fieldSelectionOperationB.result(), fieldSelectionOperationA.result()),
                ImmutableList.of(fieldSelectionOperationB.attributes(), fieldSelectionOperationA.attributes()));
        Return returnOperationFieldSelector = new Return("%21", rowOperationFieldSelector.result(), rowOperationFieldSelector.attributes());
        Block fieldSelectorBlock = new Block(
                Optional.of("^outputFieldSelector"),
                ImmutableList.of(fieldSelectionParameter),
                ImmutableList.of(
                        fieldSelectionOperationB,
                        fieldSelectionOperationA,
                        rowOperationFieldSelector,
                        returnOperationFieldSelector));

        Output outputOperation = new Output(
                "%16",
                filterOperation.result(),
                fieldSelectorBlock,
                ImmutableList.of("col_b", "col_a"));

        Query queryOperation = new Query(
                "%0",
                new Block(
                        Optional.of("^query"),
                        ImmutableList.of(),
                        ImmutableList.of(
                                valuesOperation,
                                filterOperation,
                                outputOperation)));

        Map<Value, SourceNode> expectedValueMap = ImmutableMap.<Value, SourceNode>builder()
                .put(new Operation.Result("%0", irType(BOOLEAN)), queryOperation)
                .put(new Operation.Result("%1", irType(new MultisetType(rowType("f_1", BIGINT, "f_2", BOOLEAN)))), valuesOperation)
                .put(new Operation.Result("%2", irType(BIGINT)), constantOperation1Row1)
                .put(new Operation.Result("%3", irType(BOOLEAN)), constantOperation2Row1)
                .put(new Operation.Result("%4", irType(anonymousRow(BIGINT, BOOLEAN))), rowOperation1)
                .put(new Operation.Result("%5", irType(anonymousRow(BIGINT, BOOLEAN))), returnOperationRow1)
                .put(new Operation.Result("%6", irType(BIGINT)), constantOperation1Row2)
                .put(new Operation.Result("%7", irType(BOOLEAN)), constantOperation2Row2)
                .put(new Operation.Result("%8", irType(anonymousRow(BIGINT, BOOLEAN))), rowOperation2)
                .put(new Operation.Result("%9", irType(anonymousRow(BIGINT, BOOLEAN))), returnOperationRow2)
                .put(new Operation.Result("%10", irType(new MultisetType(rowType("f_1", BIGINT, "f_2", BOOLEAN)))), filterOperation)
                .put(new Block.Parameter("%11", irType(rowType("f_1", BIGINT, "f_2", BOOLEAN))), predicateBlock)
                .put(new Operation.Result("%12", irType(BIGINT)), fieldSelectionOperationPredicate)
                .put(new Operation.Result("%13", irType(BIGINT)), constantOperationPredicate)
                .put(new Operation.Result("%14", irType(BOOLEAN)), comparisonOperationPredicate)
                .put(new Operation.Result("%15", irType(BOOLEAN)), returnOperationPredicate)
                .put(new Operation.Result("%16", irType(BOOLEAN)), outputOperation)
                .put(new Block.Parameter("%17", irType(rowType("f_1", BIGINT, "f_2", BOOLEAN))), fieldSelectorBlock)
                .put(new Operation.Result("%18", irType(BOOLEAN)), fieldSelectionOperationB)
                .put(new Operation.Result("%19", irType(BIGINT)), fieldSelectionOperationA)
                .put(new Operation.Result("%20", irType(anonymousRow(BOOLEAN, BIGINT))), rowOperationFieldSelector)
                .put(new Operation.Result("%21", irType(anonymousRow(BOOLEAN, BIGINT))), returnOperationFieldSelector)
                .buildOrThrow();

        Program expectedProgram = new Program(queryOperation, expectedValueMap);

        Program actualProgram = ProgramBuilder.buildProgram(plan);

        assertThat(actualProgram.getRoot()).isEqualTo(expectedProgram.getRoot());
        assertThat(actualProgram.getValueMap()).isEqualTo(expectedProgram.getValueMap());

        assertThat(actualProgram.print(1, TESTING_FORMAT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(""f_1"" bigint,""f_2"" boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":1}"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant_result = "{""type"":""boolean"",""value"":true}"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^row
                                        %6 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":2}"}
                                        %7 = constant() : () -> "boolean" ()
                                            {constant_result = "{""type"":""boolean"",""value"":false}"}
                                        %8 = row(%6, %7) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %9 = return(%8) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {cardinality = "2"}
                                %10 = filter(%1) : ("multiset(row(""f_1"" bigint,""f_2"" boolean))") -> "multiset(row(""f_1"" bigint,""f_2"" boolean))" ({
                                    ^predicate (%11 : "row(""f_1"" bigint,""f_2"" boolean)")
                                        %12 = field_selection(%11) : ("row(""f_1"" bigint,""f_2"" boolean)") -> "bigint" ()
                                            {field_name = "f_1"}
                                        %13 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":5}"}
                                        %14 = comparison(%12, %13) : ("bigint", "bigint") -> "boolean" ()
                                            {comparison_operator = "GREATER_THAN"}
                                        %15 = return(%14) : ("boolean") -> "boolean" ()
                                            {ir.terminal = "true"}
                                    })
                                %16 = output(%10) : ("multiset(row(""f_1"" bigint,""f_2"" boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%17 : "row(""f_1"" bigint,""f_2"" boolean)")
                                        %18 = field_selection(%17) : ("row(""f_1"" bigint,""f_2"" boolean)") -> "boolean" ()
                                            {field_name = "f_2"}
                                        %19 = field_selection(%17) : ("row(""f_1"" bigint,""f_2"" boolean)") -> "bigint" ()
                                            {field_name = "f_1"}
                                        %20 = row(%18, %19) : ("boolean", "bigint") -> "row(boolean,bigint)" ()
                                        %21 = return(%20) : ("row(boolean,bigint)") -> "row(boolean,bigint)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {output_names = "[""col_b"",""col_a""]", ir.terminal = "true"}
                            })
                            {ir.terminal = "true"}
                        """);
    }

    private static RowType rowType(String name1, Type type1, String name2, Type type2)
    {
        return RowType.rowType(new RowType.Field(Optional.of(name1), type1), new RowType.Field(Optional.of(name2), type2));
    }
}
