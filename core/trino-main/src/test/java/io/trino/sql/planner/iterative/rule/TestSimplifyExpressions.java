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

import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.LiteralEncoder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.TypeAnalyzer;
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
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.ExpressionUtils.extractPredicates;
import static io.trino.sql.ExpressionUtils.logicalExpression;
import static io.trino.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static io.trino.sql.planner.iterative.rule.SimplifyExpressions.rewrite;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

public class TestSimplifyExpressions
{
    private static final SqlParser SQL_PARSER = new SqlParser();
    private static final Metadata METADATA = createTestMetadataManager();
    private static final LiteralEncoder LITERAL_ENCODER = new LiteralEncoder(METADATA);

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

    private static void assertSimplifies(String expression, String expected)
    {
        ParsingOptions parsingOptions = new ParsingOptions();
        Expression actualExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expression, parsingOptions));
        Expression expectedExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expected, parsingOptions));
        Expression rewritten = rewrite(actualExpression, TEST_SESSION, new SymbolAllocator(booleanSymbolTypeMapFor(actualExpression)), METADATA, LITERAL_ENCODER, new TypeAnalyzer(SQL_PARSER, METADATA));
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

    private static void assertSimplifiesNumericTypes(String expression, String expected)
    {
        ParsingOptions parsingOptions = new ParsingOptions();
        Expression actualExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expression, parsingOptions));
        Expression expectedExpression = rewriteIdentifiersToSymbolReferences(SQL_PARSER.createExpression(expected, parsingOptions));
        Expression rewritten = rewrite(actualExpression, TEST_SESSION, new SymbolAllocator(numericAndBooleanSymbolTypeMapFor(actualExpression)), METADATA, LITERAL_ENCODER, new TypeAnalyzer(SQL_PARSER, METADATA));
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
    }
}
