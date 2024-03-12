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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.IrUtils.extractPredicates;
import static io.trino.sql.ir.IrUtils.logicalExpression;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.dataType;
import static io.trino.sql.planner.iterative.rule.SimplifyExpressions.rewrite;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE;
import static io.trino.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.tree.LogicalExpression.Operator.AND;
import static io.trino.sql.tree.LogicalExpression.Operator.OR;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyExpressions
{
    @Test
    public void testPushesDownNegations()
    {
        assertSimplifies(
                new NotExpression(new SymbolReference("X")),
                new NotExpression(new SymbolReference("X")),
                ImmutableMap.of("X", BOOLEAN));
        assertSimplifies(
                new NotExpression(new NotExpression(new SymbolReference("X"))),
                new SymbolReference("X"),
                ImmutableMap.of("X", BOOLEAN));
        assertSimplifies(
                new NotExpression(new NotExpression(new NotExpression(new SymbolReference("X")))),
                new NotExpression(new SymbolReference("X")),
                ImmutableMap.of("X", BOOLEAN));
        assertSimplifies(
                new NotExpression(new NotExpression(new NotExpression(new SymbolReference("X")))),
                new NotExpression(new SymbolReference("X")),
                ImmutableMap.of("X", BOOLEAN));

        assertSimplifies(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("X"), new SymbolReference("Y"))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("X"), new SymbolReference("Y")),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("X"), new NotExpression(new NotExpression(new SymbolReference("Y"))))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("X"), new SymbolReference("Y")),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN));
        assertSimplifies(
                new ComparisonExpression(GREATER_THAN, new SymbolReference("X"), new NotExpression(new NotExpression(new SymbolReference("Y")))),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("X"), new SymbolReference("Y")),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN));
        assertSimplifies(
                new NotExpression(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Z"), new SymbolReference("V"))))))),
                new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference("X")), new NotExpression(new SymbolReference("Y")), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Z"), new SymbolReference("V"))))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN, "Z", BOOLEAN, "V", BOOLEAN));
        assertSimplifies(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Z"), new SymbolReference("V"))))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new SymbolReference("X")), new NotExpression(new SymbolReference("Y")), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Z"), new SymbolReference("V"))))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN, "Z", BOOLEAN, "V", BOOLEAN));
        assertSimplifies(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Z"), new SymbolReference("V")))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new SymbolReference("X")), new NotExpression(new SymbolReference("Y")), new LogicalExpression(AND, ImmutableList.of(new NotExpression(new SymbolReference("Z")), new NotExpression(new SymbolReference("V")))))), ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN, "Z", BOOLEAN, "V", BOOLEAN));

        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                ImmutableMap.of("X", BIGINT, "Y", BIGINT));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                ImmutableMap.of("X", DOUBLE, "Y", DOUBLE));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("X"), new SymbolReference("Y"))),
                ImmutableMap.of("X", VARCHAR, "Y", VARCHAR));
    }

    @Test
    public void testExtractCommonPredicates()
    {
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Y"), new SymbolReference("X"))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("X"))),
                new SymbolReference("X"),
                ImmutableMap.of("X", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("X"))),
                new SymbolReference("X"),
                ImmutableMap.of("X", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("V"))), new SymbolReference("V"))),
                new SymbolReference("V"),
                ImmutableMap.of("A", BOOLEAN, "V", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("V"))), new SymbolReference("V"))),
                new SymbolReference("V"),
                ImmutableMap.of("A", BOOLEAN, "V", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"), new SymbolReference("C"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"), new SymbolReference("C"))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new SymbolReference("I"), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"), new SymbolReference("C")))))),
                new ComparisonExpression(EQUAL, new SymbolReference("I"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B")))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "I", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Z"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Z"))))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN, "Z", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new SymbolReference("V"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new SymbolReference("Z"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("V"), new SymbolReference("Z"))))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN, "Z", BOOLEAN, "V", BOOLEAN));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new SymbolReference("V"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new SymbolReference("Z"))))), new SymbolReference("I")),
                new ComparisonExpression(EQUAL, new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("V"), new SymbolReference("Z"))))), new SymbolReference("I")),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN, "Z", BOOLEAN, "V", BOOLEAN, "I", BOOLEAN));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))), new SymbolReference("V"))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))), new SymbolReference("V"))))),
                new SymbolReference("V"),
                ImmutableMap.of("X", BOOLEAN, "V", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))), new SymbolReference("X"))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))), new SymbolReference("V"))))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))),
                ImmutableMap.of("X", BOOLEAN, "V", BOOLEAN));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))), new SymbolReference("Z"))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))), new SymbolReference("V"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("X"), new SymbolReference("V"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Z"), new SymbolReference("V"))))),
                ImmutableMap.of("X", BOOLEAN, "V", BOOLEAN, "Z", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("Y"), new SymbolReference("Z"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("Y"), new SymbolReference("V"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("Y"), new SymbolReference("X"))))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("X"), new SymbolReference("Y"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("Z"), new SymbolReference("V"), new SymbolReference("X"))))),
                ImmutableMap.of("X", BOOLEAN, "Y", BOOLEAN, "Z", BOOLEAN, "V", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"), new SymbolReference("C"), new SymbolReference("D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"), new SymbolReference("E"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("F"))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("B"), new SymbolReference("C"), new SymbolReference("D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("B"), new SymbolReference("E"))), new SymbolReference("F"))))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "D", BOOLEAN, "E", BOOLEAN, "F", BOOLEAN));

        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("C"))))), new SymbolReference("D"))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B"), new SymbolReference("C"))), new SymbolReference("D"))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "D", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("C"))))), new SymbolReference("D"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"), new SymbolReference("D"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("C"), new SymbolReference("D"))))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "D", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("C"))))), new SymbolReference("D"))), new SymbolReference("E"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("E"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B"), new SymbolReference("C"), new SymbolReference("E"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("D"), new SymbolReference("E"))))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "D", BOOLEAN, "E", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("C"))))), new SymbolReference("D"))), new SymbolReference("E"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("B"), new SymbolReference("C"))), new SymbolReference("D"))), new SymbolReference("E"))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "D", BOOLEAN, "E", BOOLEAN));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("C"), new SymbolReference("D"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("C"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("A"), new SymbolReference("D"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B"), new SymbolReference("C"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B"), new SymbolReference("D"))))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "D", BOOLEAN));
        // No distribution since it would add too many new terms
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("C"), new SymbolReference("D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("E"), new SymbolReference("F"))))),
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A"), new SymbolReference("B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("C"), new SymbolReference("D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("E"), new SymbolReference("F"))))),
                ImmutableMap.of("A", BOOLEAN, "B", BOOLEAN, "C", BOOLEAN, "D", BOOLEAN, "E", BOOLEAN, "F", BOOLEAN));

        // Test overflow handling for large disjunct expressions
        Map<String, Type> symbolTypes = IntStream.range(1, 61)
                .mapToObj(i -> "A" + i)
                .collect(toImmutableMap(Function.identity(), x -> BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A1"), new SymbolReference("A2"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A3"), new SymbolReference("A4"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A5"), new SymbolReference("A6"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A7"), new SymbolReference("A8"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A9"), new SymbolReference("A10"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A11"), new SymbolReference("A12"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A13"), new SymbolReference("A14"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A15"), new SymbolReference("A16"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A17"), new SymbolReference("A18"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A19"), new SymbolReference("A20"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A21"), new SymbolReference("A22"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A23"), new SymbolReference("A24"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A25"), new SymbolReference("A26"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A27"), new SymbolReference("A28"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A29"), new SymbolReference("A30"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A31"), new SymbolReference("A32"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A33"), new SymbolReference("A34"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A35"), new SymbolReference("A36"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A37"), new SymbolReference("A38"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A39"), new SymbolReference("A40"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A41"), new SymbolReference("A42"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A43"), new SymbolReference("A44"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A45"), new SymbolReference("A46"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A47"), new SymbolReference("A48"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A49"), new SymbolReference("A50"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A51"), new SymbolReference("A52"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A53"), new SymbolReference("A54"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A55"), new SymbolReference("A56"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A57"), new SymbolReference("A58"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A59"), new SymbolReference("A60"))))),
                new LogicalExpression(OR, ImmutableList.of(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A1"), new SymbolReference("A2"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A3"), new SymbolReference("A4"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A5"), new SymbolReference("A6"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A7"), new SymbolReference("A8"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A9"), new SymbolReference("A10"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A11"), new SymbolReference("A12"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A13"), new SymbolReference("A14"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A15"), new SymbolReference("A16"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A17"), new SymbolReference("A18"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A19"), new SymbolReference("A20"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A21"), new SymbolReference("A22"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A23"), new SymbolReference("A24"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A25"), new SymbolReference("A26"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A27"), new SymbolReference("A28"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A29"), new SymbolReference("A30"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A31"), new SymbolReference("A32"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A33"), new SymbolReference("A34"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A35"), new SymbolReference("A36"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A37"), new SymbolReference("A38"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A39"), new SymbolReference("A40"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A41"), new SymbolReference("A42"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A43"), new SymbolReference("A44"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A45"), new SymbolReference("A46"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A47"), new SymbolReference("A48"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A49"), new SymbolReference("A50"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A51"), new SymbolReference("A52"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A53"), new SymbolReference("A54"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A55"), new SymbolReference("A56"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A57"), new SymbolReference("A58"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference("A59"), new SymbolReference("A60"))))),
                symbolTypes);
    }

    @Test
    public void testMultipleNulls()
    {
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new NullLiteral(), new NullLiteral(), new NullLiteral(), FALSE_LITERAL)),
                FALSE_LITERAL);
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new NullLiteral(), new NullLiteral(), new NullLiteral(), new SymbolReference("B1"))),
                new LogicalExpression(AND, ImmutableList.of(new NullLiteral(), new SymbolReference("B1"))),
                ImmutableMap.of("B1", BOOLEAN));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new NullLiteral(), new NullLiteral(), new NullLiteral(), TRUE_LITERAL)),
                TRUE_LITERAL);
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new NullLiteral(), new NullLiteral(), new NullLiteral(), new SymbolReference("B1"))),
                new LogicalExpression(OR, ImmutableList.of(new NullLiteral(), new SymbolReference("B1"))),
                ImmutableMap.of("B1", BOOLEAN));
    }

    @Test
    public void testCastBigintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new LongLiteral("12300000000"), dataType("varchar(11)")),
                new StringLiteral("12300000000"));
        assertSimplifies(
                new Cast(new LongLiteral("-12300000000"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-12300000000"), dataType("varchar(50)")));

        // cast from bigint to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new LongLiteral("12300000000"), dataType("varchar(3)")),
                new Cast(new LongLiteral("12300000000"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new LongLiteral("-12300000000"), dataType("varchar(3)")),
                new Cast(new LongLiteral("-12300000000"), dataType("varchar(3)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new LongLiteral("12300000000"), dataType("varchar(3)")), new StringLiteral("12300000000")),
                new ComparisonExpression(EQUAL, new Cast(new LongLiteral("12300000000"), dataType("varchar(3)")), new StringLiteral("12300000000")));
    }

    @Test
    public void testCastIntegerToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new LongLiteral("1234"), dataType("varchar(4)")),
                new StringLiteral("1234"));
        assertSimplifies(
                new Cast(new LongLiteral("-1234"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-1234"), dataType("varchar(50)")));

        // cast from integer to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new LongLiteral("1234"), dataType("varchar(3)")),
                new Cast(new LongLiteral("1234"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new LongLiteral("-1234"), dataType("varchar(3)")),
                new Cast(new LongLiteral("-1234"), dataType("varchar(3)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new LongLiteral("1234"), dataType("varchar(3)")), new StringLiteral("1234")),
                new ComparisonExpression(EQUAL, new Cast(new LongLiteral("1234"), dataType("varchar(3)")), new StringLiteral("1234")));
    }

    @Test
    public void testCastSmallintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new GenericLiteral("SMALLINT", "1234"), dataType("varchar(4)")),
                new StringLiteral("1234"));
        assertSimplifies(
                new Cast(new GenericLiteral("SMALLINT", "-1234"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-1234"), dataType("varchar(50)")));

        // cast from smallint to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new GenericLiteral("SMALLINT", "1234"), dataType("varchar(3)")),
                new Cast(new GenericLiteral("SMALLINT", "1234"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new GenericLiteral("SMALLINT", "-1234"), dataType("varchar(3)")),
                new Cast(new GenericLiteral("SMALLINT", "-1234"), dataType("varchar(3)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("SMALLINT", "1234"), dataType("varchar(3)")), new StringLiteral("1234")),
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("SMALLINT", "1234"), dataType("varchar(3)")), new StringLiteral("1234")));
    }

    @Test
    public void testCastTinyintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new GenericLiteral("TINYINT", "123"), dataType("varchar(3)")),
                new StringLiteral("123"));
        assertSimplifies(
                new Cast(new GenericLiteral("TINYINT", "-123"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-123"), dataType("varchar(50)")));

        // cast from smallint to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new GenericLiteral("TINYINT", "123"), dataType("varchar(2)")),
                new Cast(new GenericLiteral("TINYINT", "123"), dataType("varchar(2)")));
        assertSimplifies(
                new Cast(new GenericLiteral("TINYINT", "-123"), dataType("varchar(2)")),
                new Cast(new GenericLiteral("TINYINT", "-123"), dataType("varchar(2)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("TINYINT", "123"), dataType("varchar(2)")), new StringLiteral("123")),
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("TINYINT", "123"), dataType("varchar(2)")), new StringLiteral("123")));
    }

    @Test
    public void testCastShortDecimalToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new DecimalLiteral("12.4"), dataType("varchar(4)")),
                new StringLiteral("12.4"));
        assertSimplifies(
                new Cast(new DecimalLiteral("-12.4"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-12.4"), dataType("varchar(50)")));

        // cast from short decimal to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new DecimalLiteral("12.4"), dataType("varchar(3)")),
                new Cast(new DecimalLiteral("12.4"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new DecimalLiteral("-12.4"), dataType("varchar(3)")),
                new Cast(new DecimalLiteral("-12.4"), dataType("varchar(3)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new DecimalLiteral("12.4"), dataType("varchar(3)")), new StringLiteral("12.4")),
                new ComparisonExpression(EQUAL, new Cast(new DecimalLiteral("12.4"), dataType("varchar(3)")), new StringLiteral("12.4")));
    }

    @Test
    public void testCastLongDecimalToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new DecimalLiteral("100000000000000000.1"), dataType("varchar(20)")),
                new StringLiteral("100000000000000000.1"));
        assertSimplifies(
                new Cast(new DecimalLiteral("-100000000000000000.1"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-100000000000000000.1"), dataType("varchar(50)")));

        // cast from long decimal to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new DecimalLiteral("100000000000000000.1"), dataType("varchar(3)")),
                new Cast(new DecimalLiteral("100000000000000000.1"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new DecimalLiteral("-100000000000000000.1"), dataType("varchar(3)")),
                new Cast(new DecimalLiteral("-100000000000000000.1"), dataType("varchar(3)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new DecimalLiteral("100000000000000000.1"), dataType("varchar(3)")), new StringLiteral("100000000000000000.1")),
                new ComparisonExpression(EQUAL, new Cast(new DecimalLiteral("100000000000000000.1"), dataType("varchar(3)")), new StringLiteral("100000000000000000.1")));
    }

    @Test
    public void testCastDoubleToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new DoubleLiteral("0.0"), dataType("varchar(3)")),
                new StringLiteral("0E0"));
        assertSimplifies(
                new Cast(new DoubleLiteral("-0.0"), dataType("varchar(4)")),
                new StringLiteral("-0E0"));
        assertSimplifies(
                new Cast(new ArithmeticBinaryExpression(DIVIDE, new DoubleLiteral("0.0"), new DoubleLiteral("0.0")), dataType("varchar(3)")),
                new StringLiteral("NaN"));
        assertSimplifies(
                new Cast(new GenericLiteral("DOUBLE", "Infinity"), dataType("varchar(8)")),
                new StringLiteral("Infinity"));
        assertSimplifies(
                new Cast(new DoubleLiteral("1200.0"), dataType("varchar(5)")),
                new StringLiteral("1.2E3"));
        assertSimplifies(
                new Cast(new DoubleLiteral("-1200.0"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-1.2E3"), dataType("varchar(50)")));

        // cast from double to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new DoubleLiteral("1200.0"), dataType("varchar(3)")),
                new Cast(new DoubleLiteral("1200.0"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new DoubleLiteral("-1200.0"), dataType("varchar(3)")),
                new Cast(new DoubleLiteral("-1200.0"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new DoubleLiteral("NaN"), dataType("varchar(2)")),
                new Cast(new DoubleLiteral("NaN"), dataType("varchar(2)")));
        assertSimplifies(
                new Cast(new GenericLiteral("DOUBLE", "Infinity"), dataType("varchar(7)")),
                new Cast(new GenericLiteral("DOUBLE", "Infinity"), dataType("varchar(7)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new DoubleLiteral("1200.0"), dataType("varchar(3)")), new StringLiteral("1200.0")),
                new ComparisonExpression(EQUAL, new Cast(new DoubleLiteral("1200.0"), dataType("varchar(3)")), new StringLiteral("1200.0")));
    }

    @Test
    public void testCastRealToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "0e0"), dataType("varchar(3)")),
                new StringLiteral("0E0"));
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "-0e0"), dataType("varchar(4)")),
                new StringLiteral("-0E0"));
        assertSimplifies(
                new Cast(new ArithmeticBinaryExpression(DIVIDE, new GenericLiteral("REAL", "0e0"), new GenericLiteral("REAL", "0e0")), dataType("varchar(3)")),
                new StringLiteral("NaN"));
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "Infinity"), dataType("varchar(8)")),
                new StringLiteral("Infinity"));
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "12e2"), dataType("varchar(5)")),
                new StringLiteral("1.2E3"));
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "-12e2"), dataType("varchar(50)")),
                new Cast(new StringLiteral("-1.2E3"), dataType("varchar(50)")));

        // cast from real to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "12e2"), dataType("varchar(3)")),
                new Cast(new GenericLiteral("REAL", "12e2"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "-12e2"), dataType("varchar(3)")),
                new Cast(new GenericLiteral("REAL", "-12e2"), dataType("varchar(3)")));
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "NaN"), dataType("varchar(2)")),
                new Cast(new GenericLiteral("REAL", "NaN"), dataType("varchar(2)")));
        assertSimplifies(
                new Cast(new GenericLiteral("REAL", "Infinity"), dataType("varchar(7)")),
                new Cast(new GenericLiteral("REAL", "Infinity"), dataType("varchar(7)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("REAL", "12e2"), dataType("varchar(3)")), new StringLiteral("1200.0")),
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("REAL", "12e2"), dataType("varchar(3)")), new StringLiteral("1200.0")));
    }

    @Test
    public void testCastDateToBoundedVarchar()
    {
        // the varchar type length is enough to contain the date's representation
        assertSimplifies(
                new Cast(new GenericLiteral("DATE", "2013-02-02"), dataType("varchar(10)")),
                new StringLiteral("2013-02-02"));
        assertSimplifies(
                new Cast(new GenericLiteral("DATE", "2013-02-02"), dataType("varchar(50)")),
                new Cast(new StringLiteral("2013-02-02"), dataType("varchar(50)")));

        // cast from date to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new GenericLiteral("DATE", "2013-02-02"), dataType("varchar(3)")),
                new Cast(new GenericLiteral("DATE", "2013-02-02"), dataType("varchar(3)")));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("DATE", "2013-02-02"), dataType("varchar(3)")), new StringLiteral("2013-02-02")),
                new ComparisonExpression(EQUAL, new Cast(new GenericLiteral("DATE", "2013-02-02"), dataType("varchar(3)")), new StringLiteral("2013-02-02")));
    }

    private static void assertSimplifies(Expression expression, Expression expected)
    {
        assertSimplifies(expression, expected, ImmutableMap.of());
    }

    private static void assertSimplifies(Expression expression, Expression expected, Map<String, Type> symbolTypes)
    {
        Map<Symbol, Type> symbols = symbolTypes.entrySet().stream().collect(toImmutableMap(symbolTypeEntry -> new Symbol(symbolTypeEntry.getKey()), Map.Entry::getValue));
        Expression simplified = normalize(rewrite(expression, TEST_SESSION, new SymbolAllocator(symbols), PLANNER_CONTEXT, new IrTypeAnalyzer(PLANNER_CONTEXT)));
        assertThat(simplified).isEqualTo(normalize(expected));
    }

    @Test
    public void testPushesDownNegationsNumericTypes()
    {
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("I1"), new SymbolReference("I2"))),
                new ComparisonExpression(NOT_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("I1"), new SymbolReference("I2"))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new ComparisonExpression(GREATER_THAN, new SymbolReference("I3"), new SymbolReference("I4"))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("I3"), new SymbolReference("I4")))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("I1"), new SymbolReference("I2")))), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("I3"), new SymbolReference("I4")))))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new ComparisonExpression(GREATER_THAN, new SymbolReference("I3"), new SymbolReference("I4")))));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("B1"), new SymbolReference("B2"))), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B3"), new SymbolReference("B4"))))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference("B1")), new NotExpression(new SymbolReference("B2")))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B3"), new SymbolReference("B4"))))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("I1"), new SymbolReference("I2"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("I1"), new SymbolReference("I2"))));

        /*
         Restricted rewrite for types having NaN
         */
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))),
                new ComparisonExpression(NOT_EQUAL, new SymbolReference("D1"), new SymbolReference("D2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(NOT_EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))),
                new ComparisonExpression(EQUAL, new SymbolReference("D1"), new SymbolReference("D2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference("R1"), new SymbolReference("R2"))),
                new ComparisonExpression(NOT_EQUAL, new SymbolReference("R1"), new SymbolReference("R2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(NOT_EQUAL, new SymbolReference("R1"), new SymbolReference("R2"))),
                new ComparisonExpression(EQUAL, new SymbolReference("R1"), new SymbolReference("R2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("D1"), new SymbolReference("D2"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("D1"), new SymbolReference("D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("R1"), new SymbolReference("R2"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference("R1"), new SymbolReference("R2"))));

        // DOUBLE: no negation pushdown for inequalities
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("D1"), new SymbolReference("D2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("D1"), new SymbolReference("D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))));

        // REAL: no negation pushdown for inequalities
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("R1"), new SymbolReference("R2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("R1"), new SymbolReference("R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("R1"), new SymbolReference("R2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("R1"), new SymbolReference("R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("R1"), new SymbolReference("R2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("R1"), new SymbolReference("R2"))));

        // Multiple negations
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))))),
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2")))))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference("D1"), new SymbolReference("D2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2"))))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2")))))),
                new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2")));

        // Nested comparisons
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2"))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2"))))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("R1"), new SymbolReference("R2")))), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("I1"), new SymbolReference("I2")))))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("R1"), new SymbolReference("R2"))), new ComparisonExpression(GREATER_THAN, new SymbolReference("I1"), new SymbolReference("I2")))));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2")), new LogicalExpression(AND, ImmutableList.of(new SymbolReference("B1"), new SymbolReference("B2"))), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B3"), new SymbolReference("B4"))))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2"))), new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference("B1")), new NotExpression(new SymbolReference("B2")))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference("B3"), new SymbolReference("B4"))))));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2")), new ComparisonExpression(LESS_THAN, new SymbolReference("I1"), new SymbolReference("I2")))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference("B1"), new SymbolReference("B2"))), new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2"))))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new SymbolReference("D2"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")))), new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference("B1")), new NotExpression(new SymbolReference("B2")))), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new SymbolReference("R2"))))))));
        assertSimplifiesNumericTypes(
                new IfExpression(new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("I1"), new SymbolReference("I2"))), new SymbolReference("D1"), new SymbolReference("D2")),
                new IfExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference("I1"), new SymbolReference("I2")), new SymbolReference("D1"), new SymbolReference("D2")));

        // Symbol of type having NaN on either side of comparison
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new LongLiteral("1"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("D1"), new LongLiteral("1"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new LongLiteral("1"), new SymbolReference("D2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new LongLiteral("1"), new SymbolReference("D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new LongLiteral("1"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference("R1"), new LongLiteral("1"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new LongLiteral("1"), new SymbolReference("R2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new LongLiteral("1"), new SymbolReference("R2"))));
    }

    private static void assertSimplifiesNumericTypes(Expression expression, Expression expected)
    {
        Expression rewritten = rewrite(expression, TEST_SESSION, new SymbolAllocator(numericAndBooleanSymbolTypeMapFor(expression)), PLANNER_CONTEXT, new IrTypeAnalyzer(PLANNER_CONTEXT));
        assertThat(normalize(rewritten)).isEqualTo(normalize(expected));
    }

    private static Map<Symbol, Type> numericAndBooleanSymbolTypeMapFor(Expression expression)
    {
        return SymbolsExtractor.extractUnique(expression).stream()
                .collect(Collectors.toMap(
                        symbol -> symbol,
                        symbol -> {
                            switch (symbol.getName().charAt(0)) {
                                case 'I':
                                    return INTEGER;
                                case 'D':
                                    return DOUBLE;
                                case 'R':
                                    return REAL;
                                case 'B':
                                    return BOOLEAN;
                                default:
                                    return BIGINT;
                            }
                        }));
    }

    private static Expression normalize(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new NormalizeExpressionRewriter(), expression);
    }

    private static class NormalizeExpressionRewriter
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteLogicalExpression(LogicalExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            List<Expression> predicates = extractPredicates(node.getOperator(), node).stream()
                    .map(p -> treeRewriter.rewrite(p, context))
                    .sorted(Comparator.comparing(Expression::toString))
                    .collect(toList());
            return logicalExpression(node.getOperator(), predicates);
        }

        @Override
        public Expression rewriteCast(Cast node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            // the `expected` Cast expression comes out of the AstBuilder with the `typeOnly` flag set to false.
            // always set the `typeOnly` flag to false so that it does not break the comparison.
            return new Cast(node.getExpression(), node.getType(), node.isSafe());
        }
    }
}
