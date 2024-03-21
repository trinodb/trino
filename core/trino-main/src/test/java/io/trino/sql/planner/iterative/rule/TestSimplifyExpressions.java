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
import io.airlift.slice.Slices;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Decimals;
import io.trino.sql.ir.ArithmeticBinaryExpression;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.ComparisonExpression;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.LogicalExpression;
import io.trino.sql.ir.NotExpression;
import io.trino.sql.ir.SymbolReference;
import io.trino.type.Reals;
import io.trino.type.UnknownType;
import io.trino.util.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.ArithmeticBinaryExpression.Operator.DIVIDE;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN;
import static io.trino.sql.ir.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonExpression.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrUtils.extractPredicates;
import static io.trino.sql.ir.IrUtils.logicalExpression;
import static io.trino.sql.ir.LogicalExpression.Operator.AND;
import static io.trino.sql.ir.LogicalExpression.Operator.OR;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.iterative.rule.SimplifyExpressions.rewrite;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSimplifyExpressions
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction DIVIDE_DOUBLE = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(DOUBLE, DOUBLE));
    private static final ResolvedFunction DIVIDE_REAL = FUNCTIONS.resolveOperator(OperatorType.DIVIDE, ImmutableList.of(REAL, REAL));

    @Test
    public void testPushesDownNegations()
    {
        assertSimplifies(
                new NotExpression(new SymbolReference(BOOLEAN, "X")),
                new NotExpression(new SymbolReference(BOOLEAN, "X")));
        assertSimplifies(
                new NotExpression(new NotExpression(new SymbolReference(BOOLEAN, "X"))),
                new SymbolReference(BOOLEAN, "X"));
        assertSimplifies(
                new NotExpression(new NotExpression(new NotExpression(new SymbolReference(BOOLEAN, "X")))),
                new NotExpression(new SymbolReference(BOOLEAN, "X")));
        assertSimplifies(
                new NotExpression(new NotExpression(new NotExpression(new SymbolReference(BOOLEAN, "X")))),
                new NotExpression(new SymbolReference(BOOLEAN, "X")));

        assertSimplifies(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y")));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(BOOLEAN, "X"), new NotExpression(new NotExpression(new SymbolReference(BOOLEAN, "Y"))))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y")));
        assertSimplifies(
                new ComparisonExpression(GREATER_THAN, new SymbolReference(BOOLEAN, "X"), new NotExpression(new NotExpression(new SymbolReference(BOOLEAN, "Y")))),
                new ComparisonExpression(GREATER_THAN, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y")));
        assertSimplifies(
                new NotExpression(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Z"), new SymbolReference(BOOLEAN, "V"))))))),
                new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference(BOOLEAN, "X")), new NotExpression(new SymbolReference(BOOLEAN, "Y")), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Z"), new SymbolReference(BOOLEAN, "V"))))));
        assertSimplifies(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Z"), new SymbolReference(BOOLEAN, "V"))))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new SymbolReference(BOOLEAN, "X")), new NotExpression(new SymbolReference(BOOLEAN, "Y")), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Z"), new SymbolReference(BOOLEAN, "V"))))));
        assertSimplifies(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Z"), new SymbolReference(BOOLEAN, "V")))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new SymbolReference(BOOLEAN, "X")), new NotExpression(new SymbolReference(BOOLEAN, "Y")), new LogicalExpression(AND, ImmutableList.of(new NotExpression(new SymbolReference(BOOLEAN, "Z")), new NotExpression(new SymbolReference(BOOLEAN, "V")))))));

        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))));
        assertSimplifies(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))));
    }

    @Test
    public void testExtractCommonPredicates()
    {
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "X"))));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "X"))),
                new SymbolReference(BOOLEAN, "X"));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "X"))),
                new SymbolReference(BOOLEAN, "X"));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "V"))),
                new SymbolReference(BOOLEAN, "V"));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "V"))),
                new SymbolReference(BOOLEAN, "V"));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new SymbolReference(BOOLEAN, "I"), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C")))))),
                new ComparisonExpression(EQUAL, new SymbolReference(BOOLEAN, "I"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B")))));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Z"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Z"))))));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "V"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "Z"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "V"), new SymbolReference(BOOLEAN, "Z"))))));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "V"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "Z"))))), new SymbolReference(BOOLEAN, "I")),
                new ComparisonExpression(EQUAL, new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "V"), new SymbolReference(BOOLEAN, "Z"))))), new SymbolReference(BOOLEAN, "I")));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "V"))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "V"))))),
                new SymbolReference(BOOLEAN, "V"));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "X"))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "V"))))),
                new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "Z"))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))), new SymbolReference(BOOLEAN, "V"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "V"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Z"), new SymbolReference(BOOLEAN, "V"))))));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "Z"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "V"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "Y"), new SymbolReference(BOOLEAN, "X"))))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "X"), new SymbolReference(BOOLEAN, "Y"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "Z"), new SymbolReference(BOOLEAN, "V"), new SymbolReference(BOOLEAN, "X"))))));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"), new SymbolReference(BOOLEAN, "D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "E"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "F"))))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"), new SymbolReference(BOOLEAN, "D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "E"))), new SymbolReference(BOOLEAN, "F"))))));

        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "C"))))), new SymbolReference(BOOLEAN, "D"))),
                new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"))), new SymbolReference(BOOLEAN, "D"))));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "C"))))), new SymbolReference(BOOLEAN, "D"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "D"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "C"), new SymbolReference(BOOLEAN, "D"))))));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "C"))))), new SymbolReference(BOOLEAN, "D"))), new SymbolReference(BOOLEAN, "E"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "E"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"), new SymbolReference(BOOLEAN, "E"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "D"), new SymbolReference(BOOLEAN, "E"))))));
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "C"))))), new SymbolReference(BOOLEAN, "D"))), new SymbolReference(BOOLEAN, "E"))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"))), new SymbolReference(BOOLEAN, "D"))), new SymbolReference(BOOLEAN, "E"))));

        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "C"), new SymbolReference(BOOLEAN, "D"))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "C"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "D"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "C"))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B"), new SymbolReference(BOOLEAN, "D"))))));
        // No distribution since it would add too many new terms
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "C"), new SymbolReference(BOOLEAN, "D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "E"), new SymbolReference(BOOLEAN, "F"))))),
                new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A"), new SymbolReference(BOOLEAN, "B"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "C"), new SymbolReference(BOOLEAN, "D"))), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "E"), new SymbolReference(BOOLEAN, "F"))))));

        // Test overflow handling for large disjunct expressions
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A1"), new SymbolReference(BOOLEAN, "A2"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A3"), new SymbolReference(BOOLEAN, "A4"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A5"), new SymbolReference(BOOLEAN, "A6"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A7"), new SymbolReference(BOOLEAN, "A8"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A9"), new SymbolReference(BOOLEAN, "A10"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A11"), new SymbolReference(BOOLEAN, "A12"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A13"), new SymbolReference(BOOLEAN, "A14"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A15"), new SymbolReference(BOOLEAN, "A16"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A17"), new SymbolReference(BOOLEAN, "A18"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A19"), new SymbolReference(BOOLEAN, "A20"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A21"), new SymbolReference(BOOLEAN, "A22"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A23"), new SymbolReference(BOOLEAN, "A24"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A25"), new SymbolReference(BOOLEAN, "A26"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A27"), new SymbolReference(BOOLEAN, "A28"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A29"), new SymbolReference(BOOLEAN, "A30"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A31"), new SymbolReference(BOOLEAN, "A32"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A33"), new SymbolReference(BOOLEAN, "A34"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A35"), new SymbolReference(BOOLEAN, "A36"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A37"), new SymbolReference(BOOLEAN, "A38"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A39"), new SymbolReference(BOOLEAN, "A40"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A41"), new SymbolReference(BOOLEAN, "A42"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A43"), new SymbolReference(BOOLEAN, "A44"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A45"), new SymbolReference(BOOLEAN, "A46"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A47"), new SymbolReference(BOOLEAN, "A48"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A49"), new SymbolReference(BOOLEAN, "A50"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A51"), new SymbolReference(BOOLEAN, "A52"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A53"), new SymbolReference(BOOLEAN, "A54"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A55"), new SymbolReference(BOOLEAN, "A56"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A57"), new SymbolReference(BOOLEAN, "A58"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A59"), new SymbolReference(BOOLEAN, "A60"))))),
                new LogicalExpression(OR, ImmutableList.of(
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A1"), new SymbolReference(BOOLEAN, "A2"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A3"), new SymbolReference(BOOLEAN, "A4"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A5"), new SymbolReference(BOOLEAN, "A6"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A7"), new SymbolReference(BOOLEAN, "A8"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A9"), new SymbolReference(BOOLEAN, "A10"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A11"), new SymbolReference(BOOLEAN, "A12"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A13"), new SymbolReference(BOOLEAN, "A14"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A15"), new SymbolReference(BOOLEAN, "A16"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A17"), new SymbolReference(BOOLEAN, "A18"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A19"), new SymbolReference(BOOLEAN, "A20"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A21"), new SymbolReference(BOOLEAN, "A22"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A23"), new SymbolReference(BOOLEAN, "A24"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A25"), new SymbolReference(BOOLEAN, "A26"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A27"), new SymbolReference(BOOLEAN, "A28"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A29"), new SymbolReference(BOOLEAN, "A30"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A31"), new SymbolReference(BOOLEAN, "A32"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A33"), new SymbolReference(BOOLEAN, "A34"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A35"), new SymbolReference(BOOLEAN, "A36"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A37"), new SymbolReference(BOOLEAN, "A38"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A39"), new SymbolReference(BOOLEAN, "A40"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A41"), new SymbolReference(BOOLEAN, "A42"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A43"), new SymbolReference(BOOLEAN, "A44"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A45"), new SymbolReference(BOOLEAN, "A46"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A47"), new SymbolReference(BOOLEAN, "A48"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A49"), new SymbolReference(BOOLEAN, "A50"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A51"), new SymbolReference(BOOLEAN, "A52"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A53"), new SymbolReference(BOOLEAN, "A54"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A55"), new SymbolReference(BOOLEAN, "A56"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A57"), new SymbolReference(BOOLEAN, "A58"))),
                        new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "A59"), new SymbolReference(BOOLEAN, "A60"))))));
    }

    @Test
    public void testMultipleNulls()
    {
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), FALSE_LITERAL)),
                FALSE_LITERAL);
        assertSimplifies(
                new LogicalExpression(AND, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), new SymbolReference(BOOLEAN, "B1"))),
                new LogicalExpression(AND, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new SymbolReference(BOOLEAN, "B1"))));
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), TRUE_LITERAL)),
                TRUE_LITERAL);
        assertSimplifies(
                new LogicalExpression(OR, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), new Constant(UnknownType.UNKNOWN, null), new SymbolReference(BOOLEAN, "B1"))),
                new LogicalExpression(OR, ImmutableList.of(new Constant(UnknownType.UNKNOWN, null), new SymbolReference(BOOLEAN, "B1"))));
    }

    @Test
    public void testCastBigintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(BIGINT, 12300000000L), createVarcharType(11)),
                new Constant(createVarcharType(11), Slices.utf8Slice("12300000000")));
        assertSimplifies(
                new Cast(new Constant(BIGINT, -12300000000L), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-12300000000")));

        // cast from bigint to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(BIGINT, 12300000000L), createVarcharType(3)),
                new Cast(new Constant(BIGINT, 12300000000L), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(BIGINT, -12300000000L), createVarcharType(3)),
                new Cast(new Constant(BIGINT, -12300000000L), createVarcharType(3)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(BIGINT, 12300000000L), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("12300000000"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(BIGINT, 12300000000L), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("12300000000"))));
    }

    @Test
    public void testCastIntegerToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(INTEGER, 1234L), createVarcharType(4)),
                new Constant(createVarcharType(4), Slices.utf8Slice("1234")));
        assertSimplifies(
                new Cast(new Constant(INTEGER, -1234L), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-1234")));

        // cast from integer to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)),
                new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)),
                new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1234"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1234"))));
    }

    @Test
    public void testCastSmallintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(SMALLINT, 1234L), createVarcharType(4)),
                new Constant(createVarcharType(4), Slices.utf8Slice("1234")));
        assertSimplifies(
                new Cast(new Constant(SMALLINT, -1234L), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-1234")));

        // cast from smallint to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(SMALLINT, 1234L), createVarcharType(3)),
                new Cast(new Constant(SMALLINT, 1234L), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(SMALLINT, -1234L), createVarcharType(3)),
                new Cast(new Constant(SMALLINT, -1234L), createVarcharType(3)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(SMALLINT, 1234L), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1234"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(SMALLINT, 1234L), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1234"))));
    }

    @Test
    public void testCastTinyintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(TINYINT, 123L), createVarcharType(3)),
                new Constant(createVarcharType(3), Slices.utf8Slice("123")));
        assertSimplifies(
                new Cast(new Constant(TINYINT, -123L), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-123")));

        // cast from smallint to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(TINYINT, 123L), createVarcharType(2)),
                new Cast(new Constant(TINYINT, 123L), createVarcharType(2)));
        assertSimplifies(
                new Cast(new Constant(TINYINT, -123L), createVarcharType(2)),
                new Cast(new Constant(TINYINT, -123L), createVarcharType(2)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(TINYINT, 123L), createVarcharType(2)), new Constant(VARCHAR, Slices.utf8Slice("123"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(TINYINT, 123L), createVarcharType(2)), new Constant(VARCHAR, Slices.utf8Slice("123"))));
    }

    @Test
    public void testCastShortDecimalToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("12.4"))), createVarcharType(4)),
                new Constant(createVarcharType(4), Slices.utf8Slice("12.4")));
        assertSimplifies(
                new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("-12.4"))), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-12.4")));

        // cast from short decimal to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("12.4"))), createVarcharType(3)),
                new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("12.4"))), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("-12.4"))), createVarcharType(3)),
                new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("-12.4"))), createVarcharType(3)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("12.4"))), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("12.4"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("12.4"))), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("12.4"))));
    }

    @Test
    public void testCastLongDecimalToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("100000000000000000.1"))), createVarcharType(20)),
                new Constant(createVarcharType(20), Slices.utf8Slice("100000000000000000.1")));
        assertSimplifies(
                new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("-100000000000000000.1"))), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-100000000000000000.1")));

        // cast from long decimal to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("100000000000000000.1"))), createVarcharType(3)),
                new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("100000000000000000.1"))), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("-100000000000000000.1"))), createVarcharType(3)),
                new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("-100000000000000000.1"))), createVarcharType(3)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("100000000000000000.1"))), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("100000000000000000.1"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("100000000000000000.1"))), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("100000000000000000.1"))));
    }

    @Test
    public void testCastDoubleToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(DOUBLE, 0.0), createVarcharType(3)),
                new Constant(createVarcharType(3), Slices.utf8Slice("0E0")));
        assertSimplifies(
                new Cast(new Constant(DOUBLE, -0.0), createVarcharType(4)),
                new Constant(createVarcharType(4), Slices.utf8Slice("-0E0")));
        assertSimplifies(
                new Cast(new ArithmeticBinaryExpression(DIVIDE_DOUBLE, DIVIDE, new Constant(DOUBLE, 0.0), new Constant(DOUBLE, 0.0)), createVarcharType(3)),
                new Constant(createVarcharType(3), Slices.utf8Slice("NaN")));
        assertSimplifies(
                new Cast(new Constant(DOUBLE, Double.POSITIVE_INFINITY), createVarcharType(8)),
                new Constant(createVarcharType(8), Slices.utf8Slice("Infinity")));
        assertSimplifies(
                new Cast(new Constant(DOUBLE, 1200.0), createVarcharType(5)),
                new Constant(createVarcharType(5), Slices.utf8Slice("1.2E3")));
        assertSimplifies(
                new Cast(new Constant(DOUBLE, -1200.0), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-1.2E3")));

        // cast from double to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(DOUBLE, 1200.0), createVarcharType(3)),
                new Cast(new Constant(DOUBLE, 1200.0), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(DOUBLE, -1200.0), createVarcharType(3)),
                new Cast(new Constant(DOUBLE, -1200.0), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(DOUBLE, Double.NaN), createVarcharType(2)),
                new Cast(new Constant(DOUBLE, Double.NaN), createVarcharType(2)));
        assertSimplifies(
                new Cast(new Constant(DOUBLE, Double.POSITIVE_INFINITY), createVarcharType(7)),
                new Cast(new Constant(DOUBLE, Double.POSITIVE_INFINITY), createVarcharType(7)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(DOUBLE, 1200.0), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1200.0"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(DOUBLE, 1200.0), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1200.0"))));
    }

    @Test
    public void testCastRealToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(0.0f)), createVarcharType(3)),
                new Constant(createVarcharType(3), Slices.utf8Slice("0E0")));
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(-0.0f)), createVarcharType(4)),
                new Constant(createVarcharType(4), Slices.utf8Slice("-0E0")));
        assertSimplifies(
                new Cast(new ArithmeticBinaryExpression(DIVIDE_REAL, DIVIDE, new Constant(REAL, Reals.toReal(0.0f)), new Constant(REAL, Reals.toReal(0.0f))), createVarcharType(3)),
                new Constant(createVarcharType(3), Slices.utf8Slice("NaN")));
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(Float.POSITIVE_INFINITY)), createVarcharType(8)),
                new Constant(createVarcharType(8), Slices.utf8Slice("Infinity")));
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(12e2f)), createVarcharType(5)),
                new Constant(createVarcharType(5), Slices.utf8Slice("1.2E3")));
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(-12e2f)), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("-1.2E3")));

        // cast from real to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(12e2f)), createVarcharType(3)),
                new Cast(new Constant(REAL, Reals.toReal(12e2f)), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(-12e2f)), createVarcharType(3)),
                new Cast(new Constant(REAL, Reals.toReal(-12e2f)), createVarcharType(3)));
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(Float.NaN)), createVarcharType(2)),
                new Cast(new Constant(REAL, Reals.toReal(Float.NaN)), createVarcharType(2)));
        assertSimplifies(
                new Cast(new Constant(REAL, Reals.toReal(Float.POSITIVE_INFINITY)), createVarcharType(7)),
                new Cast(new Constant(REAL, Reals.toReal(Float.POSITIVE_INFINITY)), createVarcharType(7)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(REAL, Reals.toReal(12e2f)), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1200.0"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(REAL, Reals.toReal(12e2f)), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("1200.0"))));
    }

    @Test
    public void testCastDateToBoundedVarchar()
    {
        // the varchar type length is enough to contain the date's representation
        assertSimplifies(
                new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(10)),
                new Constant(createVarcharType(10), Slices.utf8Slice("2013-02-02")));
        assertSimplifies(
                new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(50)),
                new Constant(createVarcharType(50), Slices.utf8Slice("2013-02-02")));

        // cast from date to varchar fails, so the expression is not modified
        assertSimplifies(
                new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(3)),
                new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(3)));
        assertSimplifies(
                new ComparisonExpression(EQUAL, new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("2013-02-02"))),
                new ComparisonExpression(EQUAL, new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(3)), new Constant(VARCHAR, Slices.utf8Slice("2013-02-02"))));
    }

    private static void assertSimplifies(Expression expression, Expression expected)
    {
        Expression simplified = normalize(rewrite(expression, TEST_SESSION, PLANNER_CONTEXT));
        assertThat(simplified).isEqualTo(normalize(expected));
    }

    @Test
    public void testPushesDownNegationsNumericTypes()
    {
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2"))),
                new ComparisonExpression(NOT_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2"))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "I3"), new SymbolReference(INTEGER, "I4"))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(INTEGER, "I3"), new SymbolReference(INTEGER, "I4")))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")))), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "I3"), new SymbolReference(INTEGER, "I4")))))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "I3"), new SymbolReference(INTEGER, "I4")))));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "B1"), new SymbolReference(BOOLEAN, "B2"))), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B3"), new SymbolReference(BOOLEAN, "B4"))))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference(BOOLEAN, "B1")), new NotExpression(new SymbolReference(BOOLEAN, "B2")))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B3"), new SymbolReference(BOOLEAN, "B4"))))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2"))));

        /*
         Restricted rewrite for types having NaN
         */
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))),
                new ComparisonExpression(NOT_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(NOT_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))),
                new ComparisonExpression(EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))),
                new ComparisonExpression(NOT_EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(NOT_EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))),
                new ComparisonExpression(EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))),
                new NotExpression(new ComparisonExpression(IS_DISTINCT_FROM, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))));

        // DOUBLE: no negation pushdown for inequalities
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))));

        // REAL: no negation pushdown for inequalities
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))),
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))));

        // Multiple negations
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))))),
                new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")))))),
                new ComparisonExpression(LESS_THAN_OR_EQUAL, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2")))))),
                new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2")));

        // Nested comparisons
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))))),
                new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(NOT_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))))));
        assertSimplifiesNumericTypes(
                new NotExpression(new NotExpression(new NotExpression(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2")))), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")))))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))), new ComparisonExpression(GREATER_THAN, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")))));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")), new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "B1"), new SymbolReference(BOOLEAN, "B2"))), new NotExpression(new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B3"), new SymbolReference(BOOLEAN, "B4"))))))),
                new LogicalExpression(AND, ImmutableList.of(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))), new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference(BOOLEAN, "B1")), new NotExpression(new SymbolReference(BOOLEAN, "B2")))), new LogicalExpression(OR, ImmutableList.of(new SymbolReference(BOOLEAN, "B3"), new SymbolReference(BOOLEAN, "B4"))))));
        assertSimplifiesNumericTypes(
                new NotExpression(new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")), new ComparisonExpression(LESS_THAN, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")))), new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(AND, ImmutableList.of(new SymbolReference(BOOLEAN, "B1"), new SymbolReference(BOOLEAN, "B2"))), new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))))))),
                new LogicalExpression(AND, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2"))), new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")))), new LogicalExpression(OR, ImmutableList.of(new LogicalExpression(OR, ImmutableList.of(new NotExpression(new SymbolReference(BOOLEAN, "B1")), new NotExpression(new SymbolReference(BOOLEAN, "B2")))), new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new SymbolReference(REAL, "R2"))))))));
        assertSimplifiesNumericTypes(
                ifExpression(new NotExpression(new ComparisonExpression(LESS_THAN, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2"))), new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")),
                ifExpression(new ComparisonExpression(GREATER_THAN_OR_EQUAL, new SymbolReference(INTEGER, "I1"), new SymbolReference(INTEGER, "I2")), new SymbolReference(DOUBLE, "D1"), new SymbolReference(DOUBLE, "D2")));

        // Symbol of type having NaN on either side of comparison
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new Constant(DOUBLE, 1.0))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(DOUBLE, "D1"), new Constant(DOUBLE, 1.0))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new Constant(DOUBLE, 1.0), new SymbolReference(DOUBLE, "D2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new Constant(DOUBLE, 1.0), new SymbolReference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new Constant(REAL, Reals.toReal(1L)))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new SymbolReference(REAL, "R1"), new Constant(REAL, Reals.toReal(1L)))));
        assertSimplifiesNumericTypes(
                new NotExpression(new ComparisonExpression(GREATER_THAN, new Constant(REAL, Reals.toReal(1)), new SymbolReference(REAL, "R2"))),
                new NotExpression(new ComparisonExpression(GREATER_THAN, new Constant(REAL, Reals.toReal(1)), new SymbolReference(REAL, "R2"))));
    }

    private static void assertSimplifiesNumericTypes(Expression expression, Expression expected)
    {
        Expression rewritten = rewrite(expression, TEST_SESSION, PLANNER_CONTEXT);
        assertThat(normalize(rewritten)).isEqualTo(normalize(expected));
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
