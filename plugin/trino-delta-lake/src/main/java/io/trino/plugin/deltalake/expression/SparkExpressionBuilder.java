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
package io.trino.plugin.deltalake.expression;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.Character.isValidCodePoint;
import static java.lang.String.format;
import static java.util.HexFormat.isHexDigit;

public class SparkExpressionBuilder
        extends SparkExpressionBaseBaseVisitor<Object>
{
    private static final char STRING_LITERAL_ESCAPE_CHARACTER = '\\';

    @Override
    public Object visitStandaloneExpression(SparkExpressionBaseParser.StandaloneExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Object visitPredicated(SparkExpressionBaseParser.PredicatedContext context)
    {
        // Handle comparison operator
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        // Handle simple expression like just TRUE
        return visit(context.valueExpression);
    }

    @Override
    public Object visitArithmeticBinary(SparkExpressionBaseParser.ArithmeticBinaryContext context)
    {
        return new ArithmeticBinaryExpression(
                getArithmeticBinaryOperator(context.operator),
                (SparkExpression) visit(context.left),
                (SparkExpression) visit(context.right));
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case SparkExpressionBaseParser.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case SparkExpressionBaseParser.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case SparkExpressionBaseParser.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case SparkExpressionBaseParser.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case SparkExpressionBaseParser.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
            case SparkExpressionBaseParser.AMPERSAND:
                return ArithmeticBinaryExpression.Operator.BITWISE_AND;
            case SparkExpressionBaseParser.CIRCUMFLEX:
                return ArithmeticBinaryExpression.Operator.BITWISE_XOR;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    @Override
    public Object visitComparison(SparkExpressionBaseParser.ComparisonContext context)
    {
        return new ComparisonExpression(
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (SparkExpression) visit(context.value),
                (SparkExpression) visit(context.right));
    }

    @Override
    public SparkExpression visitAnd(SparkExpressionBaseParser.AndContext context)
    {
        verify(context.booleanExpression().size() == 2, "AND operator expects two expressions: " + context.booleanExpression());
        return new LogicalExpression(
                LogicalExpression.Operator.AND,
                visit(context.left, SparkExpression.class),
                visit(context.right, SparkExpression.class));
    }

    @Override
    public Object visitOr(SparkExpressionBaseParser.OrContext context)
    {
        verify(context.booleanExpression().size() == 2, "AND operator expects two expressions: " + context.booleanExpression());
        return new LogicalExpression(
                LogicalExpression.Operator.OR,
                visit(context.left, SparkExpression.class),
                visit(context.right, SparkExpression.class));
    }

    @Override
    public SparkExpression visitBetween(SparkExpressionBaseParser.BetweenContext context)
    {
        BetweenPredicate.Operator operator = BetweenPredicate.Operator.BETWEEN;
        if (context.NOT() != null) {
            operator = BetweenPredicate.Operator.NOT_BETWEEN;
        }
        return new BetweenPredicate(
                operator,
                visit(context.value, SparkExpression.class),
                visit(context.lower, SparkExpression.class),
                visit(context.upper, SparkExpression.class));
    }

    @Override
    public Object visitColumnReference(SparkExpressionBaseParser.ColumnReferenceContext context)
    {
        return visit(context.identifier());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        return switch (symbol.getType()) {
            case SparkExpressionBaseLexer.EQ:
                yield ComparisonExpression.Operator.EQUAL;
            case SparkExpressionBaseLexer.NEQ:
                yield ComparisonExpression.Operator.NOT_EQUAL;
            case SparkExpressionBaseLexer.LT:
                yield ComparisonExpression.Operator.LESS_THAN;
            case SparkExpressionBaseLexer.LTE:
                yield ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case SparkExpressionBaseLexer.GT:
                yield ComparisonExpression.Operator.GREATER_THAN;
            case SparkExpressionBaseLexer.GTE:
                yield ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
        };
    }

    @Override
    public Object visitBooleanLiteral(SparkExpressionBaseParser.BooleanLiteralContext context)
    {
        return new BooleanLiteral(context.getText());
    }

    @Override
    public SparkExpression visitIntegerLiteral(SparkExpressionBaseParser.IntegerLiteralContext context)
    {
        return new LongLiteral(context.getText());
    }

    @Override
    public Object visitUnicodeStringLiteral(SparkExpressionBaseParser.UnicodeStringLiteralContext context)
    {
        return new StringLiteral(decodeUnicodeLiteral(context));
    }

    @Override
    public SparkExpression visitNullLiteral(SparkExpressionBaseParser.NullLiteralContext context)
    {
        return new NullLiteral();
    }

    private static String decodeUnicodeLiteral(SparkExpressionBaseParser.UnicodeStringLiteralContext context)
    {
        String rawContent = unquote(context.getText());
        StringBuilder value = new StringBuilder();
        StringBuilder unicodeEscapeCharacters = new StringBuilder();
        int unicodeEscapeCharactersNeeded = 0;
        UnicodeDecodeState state = UnicodeDecodeState.BASE;
        for (int i = 0; i < rawContent.length(); i++) {
            char ch = rawContent.charAt(i);
            switch (state) {
                case BASE -> {
                    if (ch == STRING_LITERAL_ESCAPE_CHARACTER) {
                        state = UnicodeDecodeState.ESCAPED;
                    }
                    else {
                        value.append(ch);
                    }
                }
                case ESCAPED -> {
                    if (ch == STRING_LITERAL_ESCAPE_CHARACTER) {
                        value.append(STRING_LITERAL_ESCAPE_CHARACTER);
                        state = UnicodeDecodeState.BASE;
                    }
                    else if (ch == 'u') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        unicodeEscapeCharactersNeeded = 4;
                    }
                    else if (ch == 'U') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        unicodeEscapeCharactersNeeded = 8;
                    }
                    else if (isHexDigit(ch)) {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        unicodeEscapeCharacters.append(ch);
                    }
                    else {
                        throw new ParsingException("Invalid hexadecimal digit: " + ch);
                    }
                }
                case UNICODE_SEQUENCE -> {
                    if (!isHexDigit(ch)) {
                        // Will fail check below
                        break;
                    }
                    unicodeEscapeCharacters.append(ch);
                    if (unicodeEscapeCharactersNeeded == unicodeEscapeCharacters.length()) {
                        String currentEscapedCode = unicodeEscapeCharacters.toString();
                        unicodeEscapeCharacters.setLength(0);
                        int codePoint = Integer.parseInt(currentEscapedCode, 16);
                        checkState(isValidCodePoint(codePoint), "Invalid escaped character: %s", currentEscapedCode);
                        value.appendCodePoint(codePoint);
                        state = UnicodeDecodeState.BASE;
                        unicodeEscapeCharactersNeeded = -1;
                    }
                    else {
                        checkState(unicodeEscapeCharactersNeeded > unicodeEscapeCharacters.length(), "Unexpected escape sequence length: %s", unicodeEscapeCharacters.length());
                    }
                }
            }
        }

        if (state != UnicodeDecodeState.BASE) {
            throw new ParsingException(format("Incomplete escape sequence '%s' at the end of %s literal", unicodeEscapeCharacters, context.getText()));
        }
        return value.toString();
    }

    private static String unquote(String value)
    {
        if (value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1)
                    .replace("\"\"", "\"");
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1)
                    .replace("''", "'");
        }
        throw new IllegalArgumentException("Unexpected value: " + value);
    }

    private enum UnicodeDecodeState
    {
        BASE,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    @Override
    public SparkExpression visitUnquotedIdentifier(SparkExpressionBaseParser.UnquotedIdentifierContext context)
    {
        return new Identifier(context.getText());
    }

    @Override
    public Object visitBackQuotedIdentifier(SparkExpressionBaseParser.BackQuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("``", "`");
        return new Identifier(identifier);
    }

    private <T> T visit(ParserRuleContext context, Class<T> expected)
    {
        return expected.cast(super.visit(context));
    }

    // default implementation is error-prone
    @Override
    protected Object aggregateResult(Object aggregate, Object nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }
        if (aggregate == null) {
            return nextResult;
        }
        throw new UnsupportedOperationException(format("Cannot combine %s and %s", aggregate, nextResult));
    }
}
