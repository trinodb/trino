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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.ExpressionRewriter;
import io.trino.sql.ir.ExpressionTreeRewriter;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.type.Reals;
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
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.IS_DISTINCT_FROM;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrUtils.extractPredicates;
import static io.trino.sql.ir.IrUtils.logicalExpression;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
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
                new Not(new Reference(BOOLEAN, "X")),
                new Not(new Reference(BOOLEAN, "X")));
        assertSimplifies(
                new Not(new Not(new Reference(BOOLEAN, "X"))),
                new Reference(BOOLEAN, "X"));
        assertSimplifies(
                new Not(new Not(new Not(new Reference(BOOLEAN, "X")))),
                new Not(new Reference(BOOLEAN, "X")));
        assertSimplifies(
                new Not(new Not(new Not(new Reference(BOOLEAN, "X")))),
                new Not(new Reference(BOOLEAN, "X")));

        assertSimplifies(
                new Not(new Comparison(GREATER_THAN, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))),
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y")));
        assertSimplifies(
                new Not(new Comparison(GREATER_THAN, new Reference(BOOLEAN, "X"), new Not(new Not(new Reference(BOOLEAN, "Y"))))),
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y")));
        assertSimplifies(
                new Comparison(GREATER_THAN, new Reference(BOOLEAN, "X"), new Not(new Not(new Reference(BOOLEAN, "Y")))),
                new Comparison(GREATER_THAN, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y")));
        assertSimplifies(
                new Not(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Not(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Z"), new Reference(BOOLEAN, "V"))))))),
                new Logical(OR, ImmutableList.of(new Not(new Reference(BOOLEAN, "X")), new Not(new Reference(BOOLEAN, "Y")), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Z"), new Reference(BOOLEAN, "V"))))));
        assertSimplifies(
                new Not(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Not(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Z"), new Reference(BOOLEAN, "V"))))))),
                new Logical(AND, ImmutableList.of(new Not(new Reference(BOOLEAN, "X")), new Not(new Reference(BOOLEAN, "Y")), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Z"), new Reference(BOOLEAN, "V"))))));
        assertSimplifies(
                new Not(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Z"), new Reference(BOOLEAN, "V")))))),
                new Logical(AND, ImmutableList.of(new Not(new Reference(BOOLEAN, "X")), new Not(new Reference(BOOLEAN, "Y")), new Logical(AND, ImmutableList.of(new Not(new Reference(BOOLEAN, "Z")), new Not(new Reference(BOOLEAN, "V")))))));

        assertSimplifies(
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))),
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))));
        assertSimplifies(
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))),
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))));
        assertSimplifies(
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))),
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))));
        assertSimplifies(
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))),
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))));
    }

    @Test
    public void testExtractCommonPredicates()
    {
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))),
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))),
                new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "X"))));
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "X"))),
                new Reference(BOOLEAN, "X"));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "X"))),
                new Reference(BOOLEAN, "X"));
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))))),
                new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))));

        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "V"))),
                new Reference(BOOLEAN, "V"));
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "V"))),
                new Reference(BOOLEAN, "V"));
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))))),
                new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"))))),
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))));
        assertSimplifies(
                new Comparison(EQUAL, new Reference(BOOLEAN, "I"), new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C")))))),
                new Comparison(EQUAL, new Reference(BOOLEAN, "I"), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B")))));
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Z"))))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Z"))))));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "V"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "Z"))))),
                new Logical(AND, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "V"), new Reference(BOOLEAN, "Z"))))));
        assertSimplifies(
                new Comparison(EQUAL, new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "V"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "Z"))))), new Reference(BOOLEAN, "I")),
                new Comparison(EQUAL, new Logical(OR, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "V"), new Reference(BOOLEAN, "Z"))))), new Reference(BOOLEAN, "I")));

        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "V"))), new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "V"))))),
                new Reference(BOOLEAN, "V"));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "X"))), new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "V"))))),
                new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))));

        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "Z"))), new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))), new Reference(BOOLEAN, "V"))))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "V"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Z"), new Reference(BOOLEAN, "V"))))));
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "Z"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "V"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "Y"), new Reference(BOOLEAN, "X"))))))),
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "X"), new Reference(BOOLEAN, "Y"), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "Z"), new Reference(BOOLEAN, "V"), new Reference(BOOLEAN, "X"))))));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"), new Reference(BOOLEAN, "D"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "E"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "F"))))),
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"), new Reference(BOOLEAN, "D"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "E"))), new Reference(BOOLEAN, "F"))))));

        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "C"))))), new Reference(BOOLEAN, "D"))),
                new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"))), new Reference(BOOLEAN, "D"))));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "C"))))), new Reference(BOOLEAN, "D"))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "D"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "C"), new Reference(BOOLEAN, "D"))))));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "C"))))), new Reference(BOOLEAN, "D"))), new Reference(BOOLEAN, "E"))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "E"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"), new Reference(BOOLEAN, "E"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "D"), new Reference(BOOLEAN, "E"))))));
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "C"))))), new Reference(BOOLEAN, "D"))), new Reference(BOOLEAN, "E"))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"))), new Reference(BOOLEAN, "D"))), new Reference(BOOLEAN, "E"))));

        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "C"), new Reference(BOOLEAN, "D"))))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "C"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "D"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "C"))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B"), new Reference(BOOLEAN, "D"))))));
        // No distribution since it would add too many new terms
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "C"), new Reference(BOOLEAN, "D"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "E"), new Reference(BOOLEAN, "F"))))),
                new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A"), new Reference(BOOLEAN, "B"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "C"), new Reference(BOOLEAN, "D"))), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "E"), new Reference(BOOLEAN, "F"))))));

        // Test overflow handling for large disjunct expressions
        assertSimplifies(
                new Logical(OR, ImmutableList.of(
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A1"), new Reference(BOOLEAN, "A2"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A3"), new Reference(BOOLEAN, "A4"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A5"), new Reference(BOOLEAN, "A6"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A7"), new Reference(BOOLEAN, "A8"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A9"), new Reference(BOOLEAN, "A10"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A11"), new Reference(BOOLEAN, "A12"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A13"), new Reference(BOOLEAN, "A14"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A15"), new Reference(BOOLEAN, "A16"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A17"), new Reference(BOOLEAN, "A18"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A19"), new Reference(BOOLEAN, "A20"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A21"), new Reference(BOOLEAN, "A22"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A23"), new Reference(BOOLEAN, "A24"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A25"), new Reference(BOOLEAN, "A26"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A27"), new Reference(BOOLEAN, "A28"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A29"), new Reference(BOOLEAN, "A30"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A31"), new Reference(BOOLEAN, "A32"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A33"), new Reference(BOOLEAN, "A34"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A35"), new Reference(BOOLEAN, "A36"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A37"), new Reference(BOOLEAN, "A38"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A39"), new Reference(BOOLEAN, "A40"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A41"), new Reference(BOOLEAN, "A42"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A43"), new Reference(BOOLEAN, "A44"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A45"), new Reference(BOOLEAN, "A46"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A47"), new Reference(BOOLEAN, "A48"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A49"), new Reference(BOOLEAN, "A50"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A51"), new Reference(BOOLEAN, "A52"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A53"), new Reference(BOOLEAN, "A54"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A55"), new Reference(BOOLEAN, "A56"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A57"), new Reference(BOOLEAN, "A58"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A59"), new Reference(BOOLEAN, "A60"))))),
                new Logical(OR, ImmutableList.of(
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A1"), new Reference(BOOLEAN, "A2"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A3"), new Reference(BOOLEAN, "A4"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A5"), new Reference(BOOLEAN, "A6"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A7"), new Reference(BOOLEAN, "A8"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A9"), new Reference(BOOLEAN, "A10"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A11"), new Reference(BOOLEAN, "A12"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A13"), new Reference(BOOLEAN, "A14"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A15"), new Reference(BOOLEAN, "A16"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A17"), new Reference(BOOLEAN, "A18"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A19"), new Reference(BOOLEAN, "A20"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A21"), new Reference(BOOLEAN, "A22"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A23"), new Reference(BOOLEAN, "A24"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A25"), new Reference(BOOLEAN, "A26"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A27"), new Reference(BOOLEAN, "A28"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A29"), new Reference(BOOLEAN, "A30"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A31"), new Reference(BOOLEAN, "A32"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A33"), new Reference(BOOLEAN, "A34"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A35"), new Reference(BOOLEAN, "A36"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A37"), new Reference(BOOLEAN, "A38"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A39"), new Reference(BOOLEAN, "A40"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A41"), new Reference(BOOLEAN, "A42"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A43"), new Reference(BOOLEAN, "A44"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A45"), new Reference(BOOLEAN, "A46"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A47"), new Reference(BOOLEAN, "A48"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A49"), new Reference(BOOLEAN, "A50"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A51"), new Reference(BOOLEAN, "A52"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A53"), new Reference(BOOLEAN, "A54"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A55"), new Reference(BOOLEAN, "A56"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A57"), new Reference(BOOLEAN, "A58"))),
                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "A59"), new Reference(BOOLEAN, "A60"))))));
    }

    @Test
    public void testMultipleNulls()
    {
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), FALSE)),
                FALSE);
        assertSimplifies(
                new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), new Reference(BOOLEAN, "B1"))),
                new Logical(AND, ImmutableList.of(new Constant(BOOLEAN, null), new Reference(BOOLEAN, "B1"))));
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), TRUE)),
                TRUE);
        assertSimplifies(
                new Logical(OR, ImmutableList.of(new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), new Constant(BOOLEAN, null), new Reference(BOOLEAN, "B1"))),
                new Logical(OR, ImmutableList.of(new Constant(BOOLEAN, null), new Reference(BOOLEAN, "B1"))));
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
                new Comparison(EQUAL, new Cast(new Constant(BIGINT, 12300000000L), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("12300000000"))),
                new Comparison(EQUAL, new Cast(new Constant(BIGINT, 12300000000L), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("12300000000"))));
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
                new Comparison(EQUAL, new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1234"))),
                new Comparison(EQUAL, new Cast(new Constant(INTEGER, 1234L), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1234"))));
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
                new Comparison(EQUAL, new Cast(new Constant(SMALLINT, 1234L), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1234"))),
                new Comparison(EQUAL, new Cast(new Constant(SMALLINT, 1234L), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1234"))));
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
                new Comparison(EQUAL, new Cast(new Constant(TINYINT, 123L), createVarcharType(2)), new Constant(createVarcharType(2), Slices.utf8Slice("12"))),
                new Comparison(EQUAL, new Cast(new Constant(TINYINT, 123L), createVarcharType(2)), new Constant(createVarcharType(2), Slices.utf8Slice("12"))));
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
                new Comparison(EQUAL, new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("12.4"))), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("12.4"))),
                new Comparison(EQUAL, new Cast(new Constant(createDecimalType(3, 1), Decimals.valueOfShort(new BigDecimal("12.4"))), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("12.4"))));
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
                new Comparison(EQUAL, new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("100000000000000000.1"))), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("100000000000000000.1"))),
                new Comparison(EQUAL, new Cast(new Constant(createDecimalType(19, 1), Decimals.valueOf(new BigDecimal("100000000000000000.1"))), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("100000000000000000.1"))));
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
                new Cast(new Call(DIVIDE_DOUBLE, ImmutableList.of(new Constant(DOUBLE, 0.0), new Constant(DOUBLE, 0.0))), createVarcharType(3)),
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
                new Comparison(EQUAL, new Cast(new Constant(DOUBLE, 1200.0), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1200.0"))),
                new Comparison(EQUAL, new Cast(new Constant(DOUBLE, 1200.0), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1200.0"))));
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
                new Cast(new Call(DIVIDE_REAL, ImmutableList.of(new Constant(REAL, Reals.toReal(0.0f)), new Constant(REAL, Reals.toReal(0.0f)))), createVarcharType(3)),
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
                new Comparison(EQUAL, new Cast(new Constant(REAL, Reals.toReal(12e2f)), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1200.0"))),
                new Comparison(EQUAL, new Cast(new Constant(REAL, Reals.toReal(12e2f)), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("1200.0"))));
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
                new Comparison(EQUAL, new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("2013-02-02"))),
                new Comparison(EQUAL, new Cast(new Constant(DATE, (long) DateTimeUtils.parseDate("2013-02-02")), createVarcharType(3)), new Constant(createVarcharType(3), Slices.utf8Slice("2013-02-02"))));
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
                new Not(new Comparison(EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2"))),
                new Comparison(NOT_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2"))),
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")));
        assertSimplifiesNumericTypes(
                new Not(new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Comparison(GREATER_THAN, new Reference(INTEGER, "I3"), new Reference(INTEGER, "I4"))))),
                new Logical(AND, ImmutableList.of(new Comparison(NOT_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Comparison(LESS_THAN_OR_EQUAL, new Reference(INTEGER, "I3"), new Reference(INTEGER, "I4")))));
        assertSimplifiesNumericTypes(
                new Not(new Not(new Not(new Logical(OR, ImmutableList.of(new Not(new Not(new Comparison(EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")))), new Not(new Comparison(GREATER_THAN, new Reference(INTEGER, "I3"), new Reference(INTEGER, "I4")))))))),
                new Logical(AND, ImmutableList.of(new Comparison(NOT_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Comparison(GREATER_THAN, new Reference(INTEGER, "I3"), new Reference(INTEGER, "I4")))));
        assertSimplifiesNumericTypes(
                new Not(new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "B1"), new Reference(BOOLEAN, "B2"))), new Not(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B3"), new Reference(BOOLEAN, "B4"))))))),
                new Logical(AND, ImmutableList.of(new Comparison(NOT_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Logical(OR, ImmutableList.of(new Not(new Reference(BOOLEAN, "B1")), new Not(new Reference(BOOLEAN, "B2")))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B3"), new Reference(BOOLEAN, "B4"))))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2"))),
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2"))));

        /*
         Restricted rewrite for types having NaN
         */
        assertSimplifiesNumericTypes(
                new Not(new Comparison(EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))),
                new Comparison(NOT_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(NOT_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))),
                new Comparison(EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2"))),
                new Comparison(NOT_EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2")));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(NOT_EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2"))),
                new Comparison(EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2")));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))),
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(REAL, "R1"), new Reference(REAL, "R2"))),
                new Not(new Comparison(IS_DISTINCT_FROM, new Reference(REAL, "R1"), new Reference(REAL, "R2"))));

        // DOUBLE: no negation pushdown for inequalities
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))),
                new Not(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))),
                new Not(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(LESS_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))),
                new Not(new Comparison(LESS_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))),
                new Not(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))));

        // REAL: no negation pushdown for inequalities
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))),
                new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2"))),
                new Not(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(LESS_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))),
                new Not(new Comparison(LESS_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(LESS_THAN_OR_EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2"))),
                new Not(new Comparison(LESS_THAN_OR_EQUAL, new Reference(REAL, "R1"), new Reference(REAL, "R2"))));

        // Multiple negations
        assertSimplifiesNumericTypes(
                new Not(new Not(new Not(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))))),
                new Not(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new Not(new Not(new Not(new Not(new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")))))),
                new Comparison(LESS_THAN_OR_EQUAL, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")));
        assertSimplifiesNumericTypes(
                new Not(new Not(new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))))),
                new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))));
        assertSimplifiesNumericTypes(
                new Not(new Not(new Not(new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2")))))),
                new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2")));

        // Nested comparisons
        assertSimplifiesNumericTypes(
                new Not(new Logical(OR, ImmutableList.of(new Comparison(EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))))),
                new Logical(AND, ImmutableList.of(new Comparison(NOT_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Not(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))))));
        assertSimplifiesNumericTypes(
                new Not(new Not(new Not(new Logical(OR, ImmutableList.of(new Not(new Not(new Comparison(LESS_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2")))), new Not(new Comparison(GREATER_THAN, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")))))))),
                new Logical(AND, ImmutableList.of(new Not(new Comparison(LESS_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))), new Comparison(GREATER_THAN, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")))));
        assertSimplifiesNumericTypes(
                new Not(new Logical(OR, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")), new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "B1"), new Reference(BOOLEAN, "B2"))), new Not(new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B3"), new Reference(BOOLEAN, "B4"))))))),
                new Logical(AND, ImmutableList.of(new Not(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))), new Logical(OR, ImmutableList.of(new Not(new Reference(BOOLEAN, "B1")), new Not(new Reference(BOOLEAN, "B2")))), new Logical(OR, ImmutableList.of(new Reference(BOOLEAN, "B3"), new Reference(BOOLEAN, "B4"))))));
        assertSimplifiesNumericTypes(
                new Not(new Logical(OR, ImmutableList.of(new Logical(AND, ImmutableList.of(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")), new Comparison(LESS_THAN, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")))), new Logical(AND, ImmutableList.of(new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "B1"), new Reference(BOOLEAN, "B2"))), new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))))))),
                new Logical(AND, ImmutableList.of(new Logical(OR, ImmutableList.of(new Not(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2"))), new Comparison(GREATER_THAN_OR_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")))), new Logical(OR, ImmutableList.of(new Logical(OR, ImmutableList.of(new Not(new Reference(BOOLEAN, "B1")), new Not(new Reference(BOOLEAN, "B2")))), new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Reference(REAL, "R2"))))))));
        assertSimplifiesNumericTypes(
                ifExpression(new Not(new Comparison(LESS_THAN, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2"))), new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")),
                ifExpression(new Comparison(GREATER_THAN_OR_EQUAL, new Reference(INTEGER, "I1"), new Reference(INTEGER, "I2")), new Reference(DOUBLE, "D1"), new Reference(DOUBLE, "D2")));

        // Symbol of type having NaN on either side of comparison
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Constant(DOUBLE, 1.0))),
                new Not(new Comparison(GREATER_THAN, new Reference(DOUBLE, "D1"), new Constant(DOUBLE, 1.0))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN, new Constant(DOUBLE, 1.0), new Reference(DOUBLE, "D2"))),
                new Not(new Comparison(GREATER_THAN, new Constant(DOUBLE, 1.0), new Reference(DOUBLE, "D2"))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Constant(REAL, Reals.toReal(1L)))),
                new Not(new Comparison(GREATER_THAN, new Reference(REAL, "R1"), new Constant(REAL, Reals.toReal(1L)))));
        assertSimplifiesNumericTypes(
                new Not(new Comparison(GREATER_THAN, new Constant(REAL, Reals.toReal(1)), new Reference(REAL, "R2"))),
                new Not(new Comparison(GREATER_THAN, new Constant(REAL, Reals.toReal(1)), new Reference(REAL, "R2"))));
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
        public Expression rewriteLogical(Logical node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            List<Expression> predicates = extractPredicates(node.operator(), node).stream()
                    .map(p -> treeRewriter.rewrite(p, context))
                    .sorted(Comparator.comparing(Expression::toString))
                    .collect(toList());
            return logicalExpression(node.operator(), predicates);
        }

        @Override
        public Expression rewriteCast(Cast node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            // the `expected` Cast expression comes out of the AstBuilder with the `typeOnly` flag set to false.
            // always set the `typeOnly` flag to false so that it does not break the comparison.
            return new Cast(node.expression(), node.type(), node.safe());
        }
    }
}
