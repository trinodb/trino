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

import io.trino.spi.type.Type;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.LogicalExpression;
import org.testng.annotations.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.ExpressionUtils.extractPredicates;
import static io.trino.sql.ExpressionUtils.logicalExpression;
import static io.trino.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static io.trino.sql.planner.iterative.rule.SimplifyExpressions.rewrite;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestSimplifyExpressions
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testPushesDownNegations()
    {
        assertSimplifies("NOT X", "NOT X");
        assertSimplifies("NOT NOT X", "X");
        assertSimplifies("NOT NOT NOT X", "NOT X");
        assertSimplifies("NOT NOT NOT X", "NOT X");

        assertSimplifies("NOT (X > Y)", "X <= Y");
        assertSimplifies("NOT (X > (NOT NOT Y))", "X <= Y");
        assertSimplifies("X > (NOT NOT Y)", "X > Y");
        assertSimplifies("NOT (X AND Y AND (NOT (Z OR V)))", "(NOT X) OR (NOT Y) OR (Z OR V)");
        assertSimplifies("NOT (X OR Y OR (NOT (Z OR V)))", "(NOT X) AND (NOT Y) AND (Z OR V)");
        assertSimplifies("NOT (X OR Y OR (Z OR V))", "(NOT X) AND (NOT Y) AND ((NOT Z) AND (NOT V))");

        assertSimplifies("NOT (X IS DISTINCT FROM Y)", "NOT (X IS DISTINCT FROM Y)");
    }

    @Test
    public void testExtractCommonPredicates()
    {
        assertSimplifies("X AND Y", "X AND Y");
        assertSimplifies("X OR Y", "X OR Y");
        assertSimplifies("X AND X", "X");
        assertSimplifies("X OR X", "X");
        assertSimplifies("(X OR Y) AND (X OR Y)", "X OR Y");

        assertSimplifies("(A AND V) OR V", "V");
        assertSimplifies("(A OR V) AND V", "V");
        assertSimplifies("(A OR B OR C) AND (A OR B)", "A OR B");
        assertSimplifies("(A AND B) OR (A AND B AND C)", "A AND B");
        assertSimplifies("I = ((A OR B) AND (A OR B OR C))", "I = (A OR B)");
        assertSimplifies("(X OR Y) AND (X OR Z)", "(X OR Y) AND (X OR Z)");
        assertSimplifies("(X AND Y AND V) OR (X AND Y AND Z)", "(X AND Y) AND (V OR Z)");
        assertSimplifies("((X OR Y OR V) AND (X OR Y OR Z)) = I", "((X OR Y) OR (V AND Z)) = I");

        assertSimplifies("((X OR V) AND V) OR ((X OR V) AND V)", "V");
        assertSimplifies("((X OR V) AND X) OR ((X OR V) AND V)", "X OR V");

        assertSimplifies("((X OR V) AND Z) OR ((X OR V) AND V)", "(X OR V) AND (Z OR V)");
        assertSimplifies("X AND ((Y AND Z) OR (Y AND V) OR (Y AND X))", "X AND Y AND (Z OR V OR X)");
        assertSimplifies("(A AND B AND C AND D) OR (A AND B AND E) OR (A AND F)", "A AND ((B AND C AND D) OR (B AND E) OR F)");

        assertSimplifies("((A AND B) OR (A AND C)) AND D", "A AND (B OR C) AND D");
        assertSimplifies("((A OR B) AND (A OR C)) OR D", "(A OR B OR D) AND (A OR C OR D)");
        assertSimplifies("(((A AND B) OR (A AND C)) AND D) OR E", "(A OR E) AND (B OR C OR E) AND (D OR E)");
        assertSimplifies("(((A OR B) AND (A OR C)) OR D) AND E", "(A OR (B AND C) OR D) AND E");

        assertSimplifies("(A AND B) OR (C AND D)", "(A OR C) AND (A OR D) AND (B OR C) AND (B OR D)");
        // No distribution since it would add too many new terms
        assertSimplifies("(A AND B) OR (C AND D) OR (E AND F)", "(A AND B) OR (C AND D) OR (E AND F)");

        // Test overflow handling for large disjunct expressions
        assertSimplifies("(A1 AND A2) OR (A3 AND A4) OR (A5 AND A6) OR (A7 AND A8) OR (A9 AND A10)" +
                        " OR (A11 AND A12) OR (A13 AND A14) OR (A15 AND A16) OR (A17 AND A18) OR (A19 AND A20)" +
                        " OR (A21 AND A22) OR (A23 AND A24) OR (A25 AND A26) OR (A27 AND A28) OR (A29 AND A30)" +
                        " OR (A31 AND A32) OR (A33 AND A34) OR (A35 AND A36) OR (A37 AND A38) OR (A39 AND A40)" +
                        " OR (A41 AND A42) OR (A43 AND A44) OR (A45 AND A46) OR (A47 AND A48) OR (A49 AND A50)" +
                        " OR (A51 AND A52) OR (A53 AND A54) OR (A55 AND A56) OR (A57 AND A58) OR (A59 AND A60)",
                "(A1 AND A2) OR (A3 AND A4) OR (A5 AND A6) OR (A7 AND A8) OR (A9 AND A10)" +
                        " OR (A11 AND A12) OR (A13 AND A14) OR (A15 AND A16) OR (A17 AND A18) OR (A19 AND A20)" +
                        " OR (A21 AND A22) OR (A23 AND A24) OR (A25 AND A26) OR (A27 AND A28) OR (A29 AND A30)" +
                        " OR (A31 AND A32) OR (A33 AND A34) OR (A35 AND A36) OR (A37 AND A38) OR (A39 AND A40)" +
                        " OR (A41 AND A42) OR (A43 AND A44) OR (A45 AND A46) OR (A47 AND A48) OR (A49 AND A50)" +
                        " OR (A51 AND A52) OR (A53 AND A54) OR (A55 AND A56) OR (A57 AND A58) OR (A59 AND A60)");
    }

    @Test
    public void testMultipleNulls()
    {
        assertSimplifies("null AND null AND null AND false", "false");
        assertSimplifies("null AND null AND null AND B1", "null AND B1");
        assertSimplifies("null OR null OR null OR true", "true");
        assertSimplifies("null OR null OR null OR B1", "null OR B1");
    }

    @Test
    public void testCastBigintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(12300000000 AS varchar(11))", "'12300000000'");
        assertSimplifies("CAST(-12300000000 AS varchar(50))", "CAST('-12300000000' AS varchar(50))");

        // cast from bigint to varchar fails, so the expression is not modified
        assertSimplifies("CAST(12300000000 AS varchar(3))", "CAST(12300000000 AS varchar(3))");
        assertSimplifies("CAST(-12300000000 AS varchar(3))", "CAST(-12300000000 AS varchar(3))");
        assertSimplifies("CAST(12300000000 AS varchar(3)) = '12300000000'", "CAST(12300000000 AS varchar(3)) = '12300000000'");
    }

    @Test
    public void testCastIntegerToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(1234 AS varchar(4))", "'1234'");
        assertSimplifies("CAST(-1234 AS varchar(50))", "CAST('-1234' AS varchar(50))");

        // cast from integer to varchar fails, so the expression is not modified
        assertSimplifies("CAST(1234 AS varchar(3))", "CAST(1234 AS varchar(3))");
        assertSimplifies("CAST(-1234 AS varchar(3))", "CAST(-1234 AS varchar(3))");
        assertSimplifies("CAST(1234 AS varchar(3)) = '1234'", "CAST(1234 AS varchar(3)) = '1234'");
    }

    @Test
    public void testCastSmallintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(SMALLINT '1234' AS varchar(4))", "'1234'");
        assertSimplifies("CAST(SMALLINT '-1234' AS varchar(50))", "CAST('-1234' AS varchar(50))");

        // cast from smallint to varchar fails, so the expression is not modified
        assertSimplifies("CAST(SMALLINT '1234' AS varchar(3))", "CAST(SMALLINT '1234' AS varchar(3))");
        assertSimplifies("CAST(SMALLINT '-1234' AS varchar(3))", "CAST(SMALLINT '-1234' AS varchar(3))");
        assertSimplifies("CAST(SMALLINT '1234' AS varchar(3)) = '1234'", "CAST(SMALLINT '1234' AS varchar(3)) = '1234'");
    }

    @Test
    public void testCastTinyintToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(TINYINT '123' AS varchar(3))", "'123'");
        assertSimplifies("CAST(TINYINT '-123' AS varchar(50))", "CAST('-123' AS varchar(50))");

        // cast from smallint to varchar fails, so the expression is not modified
        assertSimplifies("CAST(TINYINT '123' AS varchar(2))", "CAST(TINYINT '123' AS varchar(2))");
        assertSimplifies("CAST(TINYINT '-123' AS varchar(2))", "CAST(TINYINT '-123' AS varchar(2))");
        assertSimplifies("CAST(TINYINT '123' AS varchar(2)) = '123'", "CAST(TINYINT '123' AS varchar(2)) = '123'");
    }

    @Test
    public void testCastShortDecimalToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(DECIMAL '12.4' AS varchar(4))", "'12.4'");
        assertSimplifies("CAST(DECIMAL '-12.4' AS varchar(50))", "CAST('-12.4' AS varchar(50))");

        // cast from short decimal to varchar fails, so the expression is not modified
        assertSimplifies("CAST(DECIMAL '12.4' AS varchar(3))", "CAST(DECIMAL '12.4' AS varchar(3))");
        assertSimplifies("CAST(DECIMAL '-12.4' AS varchar(3))", "CAST(DECIMAL '-12.4' AS varchar(3))");
        assertSimplifies("CAST(DECIMAL '12.4' AS varchar(3)) = '12.4'", "CAST(DECIMAL '12.4' AS varchar(3)) = '12.4'");
    }

    @Test
    public void testCastLongDecimalToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(DECIMAL '100000000000000000.1' AS varchar(20))", "'100000000000000000.1'");
        assertSimplifies("CAST(DECIMAL '-100000000000000000.1' AS varchar(50))", "CAST('-100000000000000000.1' AS varchar(50))");

        // cast from long decimal to varchar fails, so the expression is not modified
        assertSimplifies("CAST(DECIMAL '100000000000000000.1' AS varchar(3))", "CAST(DECIMAL '100000000000000000.1' AS varchar(3))");
        assertSimplifies("CAST(DECIMAL '-100000000000000000.1' AS varchar(3))", "CAST(DECIMAL '-100000000000000000.1' AS varchar(3))");
        assertSimplifies("CAST(DECIMAL '100000000000000000.1' AS varchar(3)) = '100000000000000000.1'", "CAST(DECIMAL '100000000000000000.1' AS varchar(3)) = '100000000000000000.1'");
    }

    @Test
    public void testCastDoubleToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(0e0 AS varchar(3))", "'0E0'");
        assertSimplifies("CAST(-0e0 AS varchar(4))", "'-0E0'");
        assertSimplifies("CAST(0e0 / 0e0 AS varchar(3))", "'NaN'");
        assertSimplifies("CAST(DOUBLE 'Infinity' AS varchar(8))", "'Infinity'");
        assertSimplifies("CAST(12e2 AS varchar(5))", "'1.2E3'");
        assertSimplifies("CAST(-12e2 AS varchar(50))", "CAST('-1.2E3' AS varchar(50))");

        // cast from double to varchar fails, so the expression is not modified
        assertSimplifies("CAST(12e2 AS varchar(3))", "CAST(12e2 AS varchar(3))");
        assertSimplifies("CAST(-12e2 AS varchar(3))", "CAST(-12e2 AS varchar(3))");
        assertSimplifies("CAST(DOUBLE 'NaN' AS varchar(2))", "CAST(DOUBLE 'NaN' AS varchar(2))");
        assertSimplifies("CAST(DOUBLE 'Infinity' AS varchar(7))", "CAST(DOUBLE 'Infinity' AS varchar(7))");
        assertSimplifies("CAST(12e2 AS varchar(3)) = '1200.0'", "CAST(12e2 AS varchar(3)) = '1200.0'");
    }

    @Test
    public void testCastRealToBoundedVarchar()
    {
        // the varchar type length is enough to contain the number's representation
        assertSimplifies("CAST(REAL '0e0' AS varchar(3))", "'0E0'");
        assertSimplifies("CAST(REAL '-0e0' AS varchar(4))", "'-0E0'");
        assertSimplifies("CAST(REAL '0e0' / REAL '0e0' AS varchar(3))", "'NaN'");
        assertSimplifies("CAST(REAL 'Infinity' AS varchar(8))", "'Infinity'");
        assertSimplifies("CAST(REAL '12e2' AS varchar(5))", "'1.2E3'");
        assertSimplifies("CAST(REAL '-12e2' AS varchar(50))", "CAST('-1.2E3' AS varchar(50))");

        // cast from real to varchar fails, so the expression is not modified
        assertSimplifies("CAST(REAL '12e2' AS varchar(3))", "CAST(REAL '12e2' AS varchar(3))");
        assertSimplifies("CAST(REAL '-12e2' AS varchar(3))", "CAST(REAL '-12e2' AS varchar(3))");
        assertSimplifies("CAST(REAL 'NaN' AS varchar(2))", "CAST(REAL 'NaN' AS varchar(2))");
        assertSimplifies("CAST(REAL 'Infinity' AS varchar(7))", "CAST(REAL 'Infinity' AS varchar(7))");
        assertSimplifies("CAST(REAL '12e2' AS varchar(3)) = '1200.0'", "CAST(REAL '12e2' AS varchar(3)) = '1200.0'");
    }

    private static void assertSimplifies(String expression, String expected)
    {
        ParsingOptions parsingOptions = new ParsingOptions();
        Expression actualExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expression, parsingOptions));
        Expression expectedExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expected, parsingOptions));
        Expression rewritten = rewrite(actualExpression, TEST_SESSION, new SymbolAllocator(booleanSymbolTypeMapFor(actualExpression)), PLANNER_CONTEXT, createTestingTypeAnalyzer(PLANNER_CONTEXT));
        assertEquals(
                normalize(rewritten),
                normalize(expectedExpression));
    }

    private static Map<Symbol, Type> booleanSymbolTypeMapFor(Expression expression)
    {
        return SymbolsExtractor.extractUnique(expression).stream()
                .collect(Collectors.toMap(symbol -> symbol, symbol -> BOOLEAN));
    }

    @Test
    public void testPushesDownNegationsNumericTypes()
    {
        assertSimplifiesNumericTypes("NOT (I1 = I2)", "I1 <> I2");
        assertSimplifiesNumericTypes("NOT (I1 > I2)", "I1 <= I2");
        assertSimplifiesNumericTypes("NOT ((I1 = I2) OR (I3 > I4))", "I1 <> I2 AND I3 <= I4");
        assertSimplifiesNumericTypes("NOT NOT NOT (NOT NOT (I1 = I2) OR NOT(I3 > I4))", "I1 <> I2 AND I3 > I4");
        assertSimplifiesNumericTypes("NOT ((I1 = I2) OR (B1 AND B2) OR NOT (B3 OR B4))", "I1 <> I2 AND (NOT B1 OR NOT B2) AND (B3 OR B4)");
        assertSimplifiesNumericTypes("NOT (I1 IS DISTINCT FROM I2)", "NOT (I1 IS DISTINCT FROM I2)");

        /*
         Restricted rewrite for types having NaN
         */
        assertSimplifiesNumericTypes("NOT (D1 = D2)", "D1 <> D2");
        assertSimplifiesNumericTypes("NOT (D1 <> D2)", "D1 = D2");
        assertSimplifiesNumericTypes("NOT (R1 = R2)", "R1 <> R2");
        assertSimplifiesNumericTypes("NOT (R1 <> R2)", "R1 = R2");
        assertSimplifiesNumericTypes("NOT (D1 IS DISTINCT FROM D2)", "NOT (D1 IS DISTINCT FROM D2)");
        assertSimplifiesNumericTypes("NOT (R1 IS DISTINCT FROM R2)", "NOT (R1 IS DISTINCT FROM R2)");

        // DOUBLE: no negation pushdown for inequalities
        assertSimplifiesNumericTypes("NOT (D1 > D2)", "NOT (D1 > D2)");
        assertSimplifiesNumericTypes("NOT (D1 >= D2)", "NOT (D1 >= D2)");
        assertSimplifiesNumericTypes("NOT (D1 < D2)", "NOT (D1 < D2)");
        assertSimplifiesNumericTypes("NOT (D1 <= D2)", "NOT (D1 <= D2)");

        // REAL: no negation pushdown for inequalities
        assertSimplifiesNumericTypes("NOT (R1 > R2)", "NOT (R1 > R2)");
        assertSimplifiesNumericTypes("NOT (R1 >= R2)", "NOT (R1 >= R2)");
        assertSimplifiesNumericTypes("NOT (R1 < R2)", "NOT (R1 < R2)");
        assertSimplifiesNumericTypes("NOT (R1 <= R2)", "NOT (R1 <= R2)");

        // Multiple negations
        assertSimplifiesNumericTypes("NOT NOT NOT (D1 <= D2)", "NOT (D1 <= D2)");
        assertSimplifiesNumericTypes("NOT NOT NOT NOT (D1 <= D2)", "D1 <= D2");
        assertSimplifiesNumericTypes("NOT NOT NOT (R1 > R2)", "NOT (R1 > R2)");
        assertSimplifiesNumericTypes("NOT NOT NOT NOT (R1 > R2)", "R1 > R2");

        // Nested comparisons
        assertSimplifiesNumericTypes("NOT ((I1 = I2) OR (D1 > D2))", "I1 <> I2 AND NOT (D1 > D2)");
        assertSimplifiesNumericTypes("NOT NOT NOT (NOT NOT (R1 < R2) OR NOT(I1 > I2))", "NOT (R1 < R2) AND I1 > I2");
        assertSimplifiesNumericTypes("NOT ((D1 > D2) OR (B1 AND B2) OR NOT (B3 OR B4))", "NOT (D1 > D2) AND (NOT B1 OR NOT B2) AND (B3 OR B4)");
        assertSimplifiesNumericTypes("NOT (((D1 > D2) AND (I1 < I2)) OR ((B1 AND B2) AND (R1 > R2)))", "(NOT (D1 > D2) OR I1 >= I2) AND ((NOT B1 OR NOT B2) OR NOT (R1 > R2))");
        assertSimplifiesNumericTypes("IF (NOT (I1 < I2), D1, D2)", "IF (I1 >= I2, D1, D2)");

        // Symbol of type having NaN on either side of comparison
        assertSimplifiesNumericTypes("NOT (D1 > 1)", "NOT (D1 > 1)");
        assertSimplifiesNumericTypes("NOT (1 > D2)", "NOT (1 > D2)");
        assertSimplifiesNumericTypes("NOT (R1 > 1)", "NOT (R1 > 1)");
        assertSimplifiesNumericTypes("NOT (1 > R2)", "NOT (1 > R2)");
    }

    @Test
    public void testRewriteOrExpression()
    {
        assertSimplifiesNumericTypes("I1 = 1 OR I1 = 2 ", "I1 IN (1, 2)");
        // TODO: Implement rule for Merging IN expression
        assertSimplifiesNumericTypes("I1 = 1 OR I1 = 2 OR I1 IN (3, 4)", "I1 IN (3, 4) OR I1 IN (1, 2)");
        assertSimplifiesNumericTypes("I1 = 1 OR I1 = 2 OR I1 = I2", "I1 IN (1, 2, I2)");
        assertSimplifiesNumericTypes("I1 = 1 OR I1 = 2 OR I2 = 3 OR I2 = 4", "I1 IN (1, 2) OR I2 IN (3, 4)");
    }

    private static void assertSimplifiesNumericTypes(String expression, String expected)
    {
        ParsingOptions parsingOptions = new ParsingOptions();
        Expression actualExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expression, parsingOptions));
        Expression expectedExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expected, parsingOptions));
        Expression rewritten = rewrite(actualExpression, TEST_SESSION, new SymbolAllocator(numericAndBooleanSymbolTypeMapFor(actualExpression)), PLANNER_CONTEXT, createTestingTypeAnalyzer(PLANNER_CONTEXT));
        assertEquals(
                normalize(rewritten),
                normalize(expectedExpression));
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
            return new Cast(node.getExpression(), node.getType(), node.isSafe(), false);
        }
    }
}
