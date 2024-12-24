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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Identifier;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static org.assertj.core.api.Assertions.assertThat;

class TestProgramBuilder
{
    @Test
    public void testProgramBuilderAndPrinter()
    {
        assertThat(ProgramBuilder
                .buildProgram(
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
                .print())
                .isEqualTo("whatever, give me the printout in the error message");
    }

    @Test
    public void testCorrelation()
    {
        assertThat(ProgramBuilder
                .buildProgram(
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
                .print())
                .isEqualTo("whatever, give me the printout in the error message");
    }

    @Test
    public void testLambda() // note: for this test to print the plan, must implement `getFlatFixedSize()` and `isFlatVariableWidth()` in FunctionType.
    {
        assertThat(ProgramBuilder
                .buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new ValuesNode(
                                        new PlanNodeId("101"),
                                        ImmutableList.of(
                                                new Symbol(new FunctionType(ImmutableList.of(), BOOLEAN), "a"),
                                                new Symbol(new FunctionType(ImmutableList.of(BOOLEAN), BOOLEAN), "b"),
                                                new Symbol(new FunctionType(ImmutableList.of(BOOLEAN, BOOLEAN), BOOLEAN), "c")),
                                        ImmutableList.of(new Row(
                                                ImmutableList.of(
                                                        new Lambda(ImmutableList.of(), new Constant(BOOLEAN, true)),
                                                        new Lambda(ImmutableList.of(new Symbol(BOOLEAN, "x")), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "x"), new Constant(BOOLEAN, false)))),
                                                        new Lambda(ImmutableList.of(new Symbol(BOOLEAN, "y"), new Symbol(BOOLEAN, "z")), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "y"), new Reference(BOOLEAN, "z")))))))),
                                ImmutableList.of("col_a", "col_b", "col_c"),
                                ImmutableList.of(
                                        new Symbol(new FunctionType(ImmutableList.of(), BOOLEAN), "a"),
                                        new Symbol(new FunctionType(ImmutableList.of(BOOLEAN), BOOLEAN), "b"),
                                        new Symbol(new FunctionType(ImmutableList.of(BOOLEAN, BOOLEAN), BOOLEAN), "c"))))
                .print())
                .isEqualTo("whatever, give me the printout in the error message");
    }

    @Test
    public void testCorrelatedLambda() // note: for this test to print the plan, must implement `getFlatFixedSize()` and `isFlatVariableWidth()` in FunctionType.
    {
        assertThat(ProgramBuilder
                .buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new CorrelatedJoinNode(
                                        new PlanNodeId("101"),
                                        new ValuesNode(
                                                new PlanNodeId("102"),
                                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 5L), new Constant(BOOLEAN, true))))
                                        ),
                                        new ValuesNode(
                                                new PlanNodeId("103"),
                                                ImmutableList.of(new Symbol(new FunctionType(ImmutableList.of(BOOLEAN, BOOLEAN), BOOLEAN), "c")),
                                                ImmutableList.of(new Row(ImmutableList.of(
                                                        new Lambda(
                                                                ImmutableList.of(new Symbol(BOOLEAN, "x"), new Symbol(BOOLEAN, "y")),
                                                                new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "x"), new Reference(BOOLEAN, "b"), new Reference(BOOLEAN, "y")))))))), // correlated lambda body
                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                        JoinType.INNER,
                                        new Constant(BOOLEAN, true),
                                        new Identifier("whatever")), // origin subquery
                                ImmutableList.of("col_a"),
                                ImmutableList.of(new Symbol(new FunctionType(ImmutableList.of(BOOLEAN, BOOLEAN), BOOLEAN), "c"))))
                .print())
                .isEqualTo("whatever, give me the printout in the error message");
    }
}
