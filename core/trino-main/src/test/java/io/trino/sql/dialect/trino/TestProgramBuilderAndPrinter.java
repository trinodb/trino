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
import com.google.common.collect.ImmutableSet;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.TestingColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.GroupingSetDescriptor;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Identifier;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.trino.TestingFormatOptions.TESTING_FORMAT_OPTIONS;
import static io.trino.sql.ir.Logical.Operator.AND;
import static org.assertj.core.api.Assertions.assertThat;

final class TestProgramBuilderAndPrinter
{
    @Test
    public void testSimplePlan()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new FilterNode(
                                        new PlanNodeId("101"),
                                        new ValuesNode(
                                                new PlanNodeId("102"),
                                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                ImmutableList.of(
                                                        new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 3L), new io.trino.sql.ir.Constant(BOOLEAN, true))),
                                                        new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 5L), new io.trino.sql.ir.Constant(BOOLEAN, false))))),
                                        new io.trino.sql.ir.Constant(BOOLEAN, true)),
                                ImmutableList.of("col_a"),
                                ImmutableList.of(new Symbol(BIGINT, "a"))))
                .print(1, TESTING_FORMAT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":3}"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant_result = "{""type"":""boolean"",""value"":true}"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^row
                                        %6 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":5}"}
                                        %7 = constant() : () -> "boolean" ()
                                            {constant_result = "{""type"":""boolean"",""value"":false}"}
                                        %8 = row(%6, %7) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %9 = return(%8) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {cardinality = "2"}
                                %10 = filter(%1) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean))" ({
                                    ^predicate (%11 : "row(bigint,boolean)")
                                        %12 = constant() : () -> "boolean" ()
                                            {constant_result = "{""type"":""boolean"",""value"":true}"}
                                        %13 = return(%12) : ("boolean") -> "boolean" ()
                                            {ir.terminal = "true"}
                                    })
                                %14 = output(%10) : ("multiset(row(bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%15 : "row(bigint,boolean)")
                                        %16 = field_reference(%15) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %17 = row(%16) : ("bigint") -> "row(bigint)" ()
                                        %18 = return(%17) : ("row(bigint)") -> "row(bigint)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {output_names = "[""col_a""]", ir.terminal = "true"}
                            })
                            {ir.terminal = "true"}
                        """);
    }

    @Test
    public void testMultiLevelCorrelation()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new CorrelatedJoinNode(
                                        new PlanNodeId("101"),
                                        new ValuesNode(
                                                new PlanNodeId("102"),
                                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                ImmutableList.of(
                                                        new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 1L), new io.trino.sql.ir.Constant(BOOLEAN, true))),
                                                        new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 2L), new io.trino.sql.ir.Constant(BOOLEAN, false))))),
                                        new CorrelatedJoinNode(
                                                new PlanNodeId("103"),
                                                new ValuesNode(
                                                        new PlanNodeId("104"),
                                                        ImmutableList.of(new Symbol(BIGINT, "c"), new Symbol(BOOLEAN, "d")),
                                                        ImmutableList.of(
                                                                new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 3L), new io.trino.sql.ir.Constant(BOOLEAN, true))),
                                                                new io.trino.sql.ir.Row(ImmutableList.of(new io.trino.sql.ir.Constant(BIGINT, 4L), new io.trino.sql.ir.Constant(BOOLEAN, false))))),
                                                new FilterNode(
                                                        new PlanNodeId("105"),
                                                        new ValuesNode(
                                                                new PlanNodeId("106"),
                                                                ImmutableList.of(new Symbol(BIGINT, "e"), new Symbol(BOOLEAN, "f")),
                                                                ImmutableList.of(
                                                                        new io.trino.sql.ir.Row(ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BOOLEAN, "b"))), // correlated level 1
                                                                        new Row(ImmutableList.of(new Reference(BIGINT, "c"), new Reference(BOOLEAN, "d"))))), // correlated level 2
                                                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "b"), new Reference(BOOLEAN, "d")))), // correlated on 2 levels
                                                ImmutableList.of(new Symbol(BIGINT, "c"), new Symbol(BOOLEAN, "d")),
                                                JoinType.INNER,
                                                new Reference(BOOLEAN, "b"),
                                                new Identifier("bla")), // origin subquery, whatever
                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                        JoinType.INNER,
                                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "b"), new Reference(BOOLEAN, "d"), new Reference(BOOLEAN, "f"))), // filter using input and subquery symbols
                                        new Identifier("bla")), // origin subquery, whatever
                                ImmutableList.of("col_1", "col_2", "col_3", "col_4", "col_5", "col_6"),
                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "c"), new Symbol(BOOLEAN, "d"), new Symbol(BIGINT, "e"), new Symbol(BOOLEAN, "f"))))
                .print(1, TESTING_FORMAT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
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
                                %10 = correlated_join(%1) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean,bigint,boolean,bigint,boolean))" ({
                                    ^correlationSelector (%11 : "row(bigint,boolean)")
                                        %12 = field_reference(%11) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %13 = field_reference(%11) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %14 = row(%12, %13) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %15 = return(%14) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^subquery (%16 : "row(bigint,boolean)")
                                        %17 = values() : () -> "multiset(row(bigint,boolean))" ({
                                            ^row
                                                %18 = constant() : () -> "bigint" ()
                                                    {constant_result = "{""type"":""bigint"",""value"":3}"}
                                                %19 = constant() : () -> "boolean" ()
                                                    {constant_result = "{""type"":""boolean"",""value"":true}"}
                                                %20 = row(%18, %19) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                %21 = return(%20) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^row
                                                %22 = constant() : () -> "bigint" ()
                                                    {constant_result = "{""type"":""bigint"",""value"":4}"}
                                                %23 = constant() : () -> "boolean" ()
                                                    {constant_result = "{""type"":""boolean"",""value"":false}"}
                                                %24 = row(%22, %23) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                %25 = return(%24) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                    {ir.terminal = "true"}
                                            })
                                            {cardinality = "2"}
                                        %26 = correlated_join(%17) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean,bigint,boolean))" ({
                                            ^correlationSelector (%27 : "row(bigint,boolean)")
                                                %28 = field_reference(%27) : ("row(bigint,boolean)") -> "bigint" ()
                                                    {field_index = "0"}
                                                %29 = field_reference(%27) : ("row(bigint,boolean)") -> "boolean" ()
                                                    {field_index = "1"}
                                                %30 = row(%28, %29) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                %31 = return(%30) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^subquery (%32 : "row(bigint,boolean)")
                                                %33 = values() : () -> "multiset(row(bigint,boolean))" ({
                                                    ^row
                                                        %34 = field_reference(%16) : ("row(bigint,boolean)") -> "bigint" ()
                                                            {field_index = "0"}
                                                        %35 = field_reference(%16) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_index = "1"}
                                                        %36 = row(%34, %35) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                        %37 = return(%36) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                            {ir.terminal = "true"}
                                                    }, {
                                                    ^row
                                                        %38 = field_reference(%32) : ("row(bigint,boolean)") -> "bigint" ()
                                                            {field_index = "0"}
                                                        %39 = field_reference(%32) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_index = "1"}
                                                        %40 = row(%38, %39) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                        %41 = return(%40) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                            {ir.terminal = "true"}
                                                    })
                                                    {cardinality = "2"}
                                                %42 = filter(%33) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean))" ({
                                                    ^predicate (%43 : "row(bigint,boolean)")
                                                        %44 = field_reference(%16) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_index = "1"}
                                                        %45 = field_reference(%32) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_index = "1"}
                                                        %46 = logical(%44, %45) : ("boolean", "boolean") -> "boolean" ()
                                                            {logical_operator = "AND"}
                                                        %47 = return(%46) : ("boolean") -> "boolean" ()
                                                            {ir.terminal = "true"}
                                                    })
                                                %48 = return(%42) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean))" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^filter (%49 : "row(bigint,boolean)", %50 : "row(bigint,boolean)")
                                                %51 = field_reference(%16) : ("row(bigint,boolean)") -> "boolean" ()
                                                    {field_index = "1"}
                                                %52 = return(%51) : ("boolean") -> "boolean" ()
                                                    {ir.terminal = "true"}
                                            })
                                            {join_type = "INNER"}
                                        %53 = return(%26) : ("multiset(row(bigint,boolean,bigint,boolean))") -> "multiset(row(bigint,boolean,bigint,boolean))" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^filter (%54 : "row(bigint,boolean)", %55 : "row(bigint,boolean,bigint,boolean)")
                                        %56 = field_reference(%54) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %57 = field_reference(%55) : ("row(bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %58 = field_reference(%55) : ("row(bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_index = "3"}
                                        %59 = logical(%56, %57, %58) : ("boolean", "boolean", "boolean") -> "boolean" ()
                                            {logical_operator = "AND"}
                                        %60 = return(%59) : ("boolean") -> "boolean" ()
                                            {ir.terminal = "true"}
                                    })
                                    {join_type = "INNER"}
                                %61 = output(%10) : ("multiset(row(bigint,boolean,bigint,boolean,bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%62 : "row(bigint,boolean,bigint,boolean,bigint,boolean)")
                                        %63 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %64 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %65 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "bigint" ()
                                            {field_index = "2"}
                                        %66 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_index = "3"}
                                        %67 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "bigint" ()
                                            {field_index = "4"}
                                        %68 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_index = "5"}
                                        %69 = row(%63, %64, %65, %66, %67, %68) : ("bigint", "boolean", "bigint", "boolean", "bigint", "boolean") -> "row(bigint,boolean,bigint,boolean,bigint,boolean)" ()
                                        %70 = return(%69) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "row(bigint,boolean,bigint,boolean,bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {output_names = "[""col_1"",""col_2"",""col_3"",""col_4"",""col_5"",""col_6""]", ir.terminal = "true"}
                            })
                            {ir.terminal = "true"}
                        """);
    }

    @Test
    public void testProjectAndAggregation()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        ResolvedFunction countFunction = functionResolution.resolveFunction("count", ImmutableList.of());
        ResolvedFunction sumFunction = functionResolution.resolveFunction("sum", fromTypes(BIGINT));

        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new AggregationNode(
                                        new PlanNodeId("101"),
                                        new ProjectNode(
                                                new PlanNodeId("102"),
                                                new ValuesNode(
                                                        new PlanNodeId("103"),
                                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                        ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 3L), new Constant(BOOLEAN, true))))),
                                                Assignments.builder()
                                                        .putIdentities(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")))
                                                        .put(new Symbol(BIGINT, "c"), new Constant(BIGINT, 5L))
                                                        .build()),
                                        ImmutableMap.of(
                                                new Symbol(BIGINT, "count"),
                                                new AggregationNode.Aggregation(
                                                        countFunction,
                                                        ImmutableList.of(),
                                                        true,
                                                        Optional.of(new Symbol(BOOLEAN, "b")),
                                                        Optional.empty(),
                                                        Optional.empty()),
                                                new Symbol(BIGINT, "sum"),
                                                new AggregationNode.Aggregation(
                                                        sumFunction,
                                                        ImmutableList.of(new Reference(BIGINT, "c")),
                                                        false,
                                                        Optional.empty(),
                                                        Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a")), ImmutableMap.of(new Symbol(BIGINT, "a"), ASC_NULLS_LAST))),
                                                        Optional.of(new Symbol(BOOLEAN, "b")))),
                                        new GroupingSetDescriptor(ImmutableList.of(new Symbol(BIGINT, "c")), 2, ImmutableSet.of(1)),
                                        ImmutableList.of(new Symbol(BIGINT, "c")),
                                        AggregationNode.Step.SINGLE,
                                        Optional.empty(),
                                        Optional.empty()),
                                ImmutableList.of("key_c", "count", "sum"),
                                ImmutableList.of(new Symbol(BIGINT, "c"), new Symbol(BIGINT, "count"), new Symbol(BIGINT, "sum"))))
                .print(1, TESTING_FORMAT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":3}"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant_result = "{""type"":""boolean"",""value"":true}"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {cardinality = "1"}
                                %6 = project(%1) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean,bigint))" ({
                                    ^assignments (%7 : "row(bigint,boolean)")
                                        %8 = field_reference(%7) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %9 = field_reference(%7) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %10 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":5}"}
                                        %11 = row(%8, %9, %10) : ("bigint", "boolean", "bigint") -> "row(bigint,boolean,bigint)" ()
                                        %12 = return(%11) : ("row(bigint,boolean,bigint)") -> "row(bigint,boolean,bigint)" ()
                                            {ir.terminal = "true"}
                                    })
                                %13 = aggregation(%6) : ("multiset(row(bigint,boolean,bigint))") -> "multiset(row(bigint,bigint,bigint))" ({
                                    ^aggregates (%14 : "multiset(row(bigint,boolean,bigint))")
                                        %15 = aggregate_call(%14) : ("multiset(row(bigint,boolean,bigint))") -> "bigint" ({
                                            ^arguments (%16 : "row(bigint,boolean,bigint)")
                                                %17 = constant() : () -> "empty row" ()
                                                    {constant_result = "{""type"":""empty row""}"}
                                                %18 = return(%17) : ("empty row") -> "empty row" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^filterSelector (%19 : "row(bigint,boolean,bigint)")
                                                %20 = field_reference(%19) : ("row(bigint,boolean,bigint)") -> "boolean" ()
                                                    {field_index = "1"}
                                                %21 = row(%20) : ("boolean") -> "row(boolean)" ()
                                                %22 = return(%21) : ("row(boolean)") -> "row(boolean)" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^maskSelector (%23 : "row(bigint,boolean,bigint)")
                                                %24 = constant() : () -> "empty row" ()
                                                    {constant_result = "{""type"":""empty row""}"}
                                                %25 = return(%24) : ("empty row") -> "empty row" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^orderingSelector (%26 : "row(bigint,boolean,bigint)")
                                                %27 = constant() : () -> "empty row" ()
                                                    {constant_result = "{""type"":""empty row""}"}
                                                %28 = return(%27) : ("empty row") -> "empty row" ()
                                                    {ir.terminal = "true"}
                                            })
                                            {resolved_function = "{""signature"":{""name"":{""catalogName"":""system"",""schemaName"":""builtin"",""functionName"":""count""},""returnType"":""bigint"",""argumentTypes"":[]},""catalogHandle"":""system:normal:system"",""functionId"":""count():bigint"",""functionKind"":""AGGREGATE"",""deterministic"":true,""functionNullability"":{""returnNullable"":true,""argumentNullable"":[]},""typeDependencies"":{},""functionDependencies"":[]}", distinct = "true"}
                                        %29 = aggregate_call(%14) : ("multiset(row(bigint,boolean,bigint))") -> "bigint" ({
                                            ^arguments (%30 : "row(bigint,boolean,bigint)")
                                                %31 = field_reference(%30) : ("row(bigint,boolean,bigint)") -> "bigint" ()
                                                    {field_index = "2"}
                                                %32 = row(%31) : ("bigint") -> "row(bigint)" ()
                                                %33 = return(%32) : ("row(bigint)") -> "row(bigint)" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^filterSelector (%34 : "row(bigint,boolean,bigint)")
                                                %35 = constant() : () -> "empty row" ()
                                                    {constant_result = "{""type"":""empty row""}"}
                                                %36 = return(%35) : ("empty row") -> "empty row" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^maskSelector (%37 : "row(bigint,boolean,bigint)")
                                                %38 = field_reference(%37) : ("row(bigint,boolean,bigint)") -> "boolean" ()
                                                    {field_index = "1"}
                                                %39 = row(%38) : ("boolean") -> "row(boolean)" ()
                                                %40 = return(%39) : ("row(boolean)") -> "row(boolean)" ()
                                                    {ir.terminal = "true"}
                                            }, {
                                            ^orderingSelector (%41 : "row(bigint,boolean,bigint)")
                                                %42 = field_reference(%41) : ("row(bigint,boolean,bigint)") -> "bigint" ()
                                                    {field_index = "0"}
                                                %43 = row(%42) : ("bigint") -> "row(bigint)" ()
                                                %44 = return(%43) : ("row(bigint)") -> "row(bigint)" ()
                                                    {ir.terminal = "true"}
                                            })
                                            {sort_orders = "[""ASC_NULLS_LAST""]", resolved_function = "{""signature"":{""name"":{""catalogName"":""system"",""schemaName"":""builtin"",""functionName"":""sum""},""returnType"":""bigint"",""argumentTypes"":[""bigint""]},""catalogHandle"":""system:normal:system"",""functionId"":""sum(bigint):bigint"",""functionKind"":""AGGREGATE"",""deterministic"":true,""functionNullability"":{""returnNullable"":true,""argumentNullable"":[false]},""typeDependencies"":{},""functionDependencies"":[]}", distinct = "false"}
                                        %45 = row(%15, %29) : ("bigint", "bigint") -> "row(bigint,bigint)" ()
                                        %46 = return(%45) : ("row(bigint,bigint)") -> "row(bigint,bigint)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^groupingKeysSelector (%47 : "row(bigint,boolean,bigint)")
                                        %48 = field_reference(%47) : ("row(bigint,boolean,bigint)") -> "bigint" ()
                                            {field_index = "2"}
                                        %49 = row(%48) : ("bigint") -> "row(bigint)" ()
                                        %50 = return(%49) : ("row(bigint)") -> "row(bigint)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^hashSelector (%51 : "row(bigint,boolean,bigint)")
                                        %52 = constant() : () -> "empty row" ()
                                            {constant_result = "{""type"":""empty row""}"}
                                        %53 = return(%52) : ("empty row") -> "empty row" ()
                                            {ir.terminal = "true"}
                                    })
                                    {grouping_sets_count = "2", global_grouping_sets = "[1]", pre_grouped_indexes = "[0]", aggregation_step = "SINGLE", input_reducing = "false"}
                                %54 = output(%13) : ("multiset(row(bigint,bigint,bigint))") -> "boolean" ({
                                    ^outputFieldSelector (%55 : "row(bigint,bigint,bigint)")
                                        %56 = field_reference(%55) : ("row(bigint,bigint,bigint)") -> "bigint" ()
                                            {field_index = "0"}
                                        %57 = field_reference(%55) : ("row(bigint,bigint,bigint)") -> "bigint" ()
                                            {field_index = "1"}
                                        %58 = field_reference(%55) : ("row(bigint,bigint,bigint)") -> "bigint" ()
                                            {field_index = "2"}
                                        %59 = row(%56, %57, %58) : ("bigint", "bigint", "bigint") -> "row(bigint,bigint,bigint)" ()
                                        %60 = return(%59) : ("row(bigint,bigint,bigint)") -> "row(bigint,bigint,bigint)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {output_names = "[""key_c"",""count"",""sum""]", ir.terminal = "true"}
                            })
                            {ir.terminal = "true"}
                        """);
    }

    @Test
    public void testTableScan()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new TableScanNode(
                                        new PlanNodeId("101"),
                                        new TableHandle(
                                                CatalogHandle.fromId("bla:normal:1"),
                                                TestingConnectorTableHandle.INSTANCE,
                                                TestingTransactionHandle.create()),
                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                        ImmutableMap.of(
                                                new Symbol(BOOLEAN, "b"),
                                                new TestingColumnHandle("b_handle"),
                                                new Symbol(BIGINT, "a"),
                                                new TestingColumnHandle("a_handle")),
                                        TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                                        Optional.of(PlanNodeStatsEstimate.unknown()),
                                        false,
                                        Optional.of(Boolean.TRUE)),
                                ImmutableList.of("col_a", "col_b"),
                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))))
                .print(1, TESTING_FORMAT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = table_scan() : () -> "multiset(row(bigint,boolean))" ()
                                    {table_handle = "[test: table_handle attribute]", column_handles = "[test: column_handles attribute]", constraint = "[test: constraint attribute]", statistics = "{""outputRowCount"":""NaN"",""fieldStatistics"":{}}", update_target = "false", use_connector_node_partitioning = "true"}
                                %2 = output(%1) : ("multiset(row(bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%3 : "row(bigint,boolean)")
                                        %4 = field_reference(%3) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %5 = field_reference(%3) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %6 = row(%4, %5) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %7 = return(%6) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {output_names = "[""col_a"",""col_b""]", ir.terminal = "true"}
                            })
                            {ir.terminal = "true"}
                        """);
    }

    @Test
    public void testExchange()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new ExchangeNode(
                                        new PlanNodeId("101"),
                                        ExchangeNode.Type.GATHER,
                                        ExchangeNode.Scope.REMOTE,
                                        new PartitioningScheme(
                                                Partitioning.create(
                                                        new PartitioningHandle(
                                                                Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                                                Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                                                new ConnectorPartitioningHandle() {}),
                                                        ImmutableList.of(new Symbol(BIGINT, "f"))),
                                                ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g")),
                                                Optional.of(new Symbol(BOOLEAN, "g")),
                                                false,
                                                Optional.of(new int[] {5, 6, 7}),
                                                Optional.empty()),
                                        ImmutableList.of(
                                                new ValuesNode(
                                                        new PlanNodeId("102"),
                                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                        ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 3L), new Constant(BOOLEAN, true))))),
                                                new ValuesNode(
                                                        new PlanNodeId("103"),
                                                        ImmutableList.of(new Symbol(SMALLINT, "c"), new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e")),
                                                        ImmutableList.of())),
                                        ImmutableList.of(
                                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e"))),
                                        Optional.empty()
                                ),
                                ImmutableList.of("col_a", "col_b"),
                                ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g"))))
                .print(1, TESTING_FORMAT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant_result = "{""type"":""bigint"",""value"":3}"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant_result = "{""type"":""boolean"",""value"":true}"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {cardinality = "1"}
                                %6 = values() : () -> "multiset(row(smallint,bigint,boolean))" ()
                                    {cardinality = "0"}
                                %7 = exchange(%1, %6) : ("multiset(row(bigint,boolean))", "multiset(row(smallint,bigint,boolean))") -> "multiset(row(bigint,boolean))" ({
                                    ^inputSelector (%8 : "row(bigint,boolean)")
                                        %9 = field_reference(%8) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %10 = field_reference(%8) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %11 = row(%9, %10) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %12 = return(%11) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^inputSelector (%13 : "row(smallint,bigint,boolean)")
                                        %14 = field_reference(%13) : ("row(smallint,bigint,boolean)") -> "bigint" ()
                                            {field_index = "1"}
                                        %15 = field_reference(%13) : ("row(smallint,bigint,boolean)") -> "boolean" ()
                                            {field_index = "2"}
                                        %16 = row(%14, %15) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %17 = return(%16) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^boundArguments (%18 : "row(bigint,boolean)")
                                        %19 = field_reference(%18) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %20 = row(%19) : ("bigint") -> "row(bigint)" ()
                                        %21 = return(%20) : ("row(bigint)") -> "row(bigint)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^hashSelector (%22 : "row(bigint,boolean)")
                                        %23 = field_reference(%22) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %24 = row(%23) : ("boolean") -> "row(boolean)" ()
                                        %25 = return(%24) : ("row(boolean)") -> "row(boolean)" ()
                                            {ir.terminal = "true"}
                                    }, {
                                    ^orderingSelector (%26 : "row(bigint,boolean)")
                                        %27 = constant() : () -> "empty row" ()
                                            {constant_result = "{""type"":""empty row""}"}
                                        %28 = return(%27) : ("empty row") -> "empty row" ()
                                            {ir.terminal = "true"}
                                    })
                                    {exchange_type = "GATHER", exchange_scope = "REMOTE", partitioning_handle = "[test: partitioning_handle attribute]", nullable_values = "[test: nullable_values attribute]", replicate_nulls_and_any = "false", bucket_to_partition = "[5,6,7]"}
                                %29 = output(%7) : ("multiset(row(bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%30 : "row(bigint,boolean)")
                                        %31 = field_reference(%30) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_index = "0"}
                                        %32 = field_reference(%30) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_index = "1"}
                                        %33 = row(%31, %32) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                        %34 = return(%33) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.terminal = "true"}
                                    })
                                    {output_names = "[""col_a"",""col_b""]", ir.terminal = "true"}
                            })
                            {ir.terminal = "true"}
                        """);
    }

    private enum TestingConnectorTableHandle
            implements ConnectorTableHandle
    {
        INSTANCE
    }
}
