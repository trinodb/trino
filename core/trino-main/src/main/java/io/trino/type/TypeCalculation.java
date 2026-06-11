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
package io.trino.type;

import io.trino.spi.type.NumericExpression;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.NodeLocation;
import io.trino.type.TypeCalculationParser.ArithmeticBinaryContext;
import io.trino.type.TypeCalculationParser.ArithmeticUnaryContext;
import io.trino.type.TypeCalculationParser.BinaryFunctionContext;
import io.trino.type.TypeCalculationParser.IdentifierContext;
import io.trino.type.TypeCalculationParser.NullLiteralContext;
import io.trino.type.TypeCalculationParser.NumericLiteralContext;
import io.trino.type.TypeCalculationParser.ParenthesizedExpressionContext;
import io.trino.type.TypeCalculationParser.TypeCalculationContext;
import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.math.BigInteger;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.type.TypeCalculationParser.ASTERISK;
import static io.trino.type.TypeCalculationParser.MAX;
import static io.trino.type.TypeCalculationParser.MIN;
import static io.trino.type.TypeCalculationParser.MINUS;
import static io.trino.type.TypeCalculationParser.PLUS;
import static io.trino.type.TypeCalculationParser.SLASH;
import static java.util.Objects.requireNonNull;

public final class TypeCalculation
{
    private static final ANTLRErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine + 1);
        }
    };

    private TypeCalculation() {}

    public static Long calculateLiteralValue(
            String calculation,
            Map<String, Long> inputs)
    {
        try {
            ParserRuleContext tree = parseTypeCalculation(calculation);
            CalculateTypeVisitor visitor = new CalculateTypeVisitor(inputs);
            BigInteger result = visitor.visit(tree);
            return result.longValueExact();
        }
        catch (StackOverflowError e) {
            throw new ParsingException("Type calculation is too large (stack overflow while parsing)", new NodeLocation(1, 1));
        }
    }

    /// Parses a calculated type-parameter expression (e.g. `min(38, x + y)`) into a structured
    /// [NumericExpression], so it can be evaluated without re-parsing the string at every bind.
    public static NumericExpression parseNumericExpression(String calculation)
    {
        try {
            return new BuildNumericExpressionVisitor().visit(parseTypeCalculation(calculation));
        }
        catch (StackOverflowError e) {
            throw new ParsingException("Type calculation is too large (stack overflow while parsing)", new NodeLocation(1, 1));
        }
    }

    private static class BuildNumericExpressionVisitor
            extends TypeCalculationBaseVisitor<NumericExpression>
    {
        @Override
        public NumericExpression visitTypeCalculation(TypeCalculationContext context)
        {
            return visit(context.expression());
        }

        @Override
        public NumericExpression visitArithmeticBinary(ArithmeticBinaryContext context)
        {
            NumericExpression left = visit(context.left);
            NumericExpression right = visit(context.right);
            return switch (context.operator.getType()) {
                case PLUS -> new NumericExpression.Operation(NumericExpression.Operator.ADD, left, right);
                case MINUS -> new NumericExpression.Operation(NumericExpression.Operator.SUBTRACT, left, right);
                case ASTERISK -> new NumericExpression.Operation(NumericExpression.Operator.MULTIPLY, left, right);
                case SLASH -> new NumericExpression.Operation(NumericExpression.Operator.DIVIDE, left, right);
                default -> throw new IllegalArgumentException("Unsupported binary operator " + context.operator.getText());
            };
        }

        @Override
        public NumericExpression visitArithmeticUnary(ArithmeticUnaryContext context)
        {
            NumericExpression value = visit(context.expression());
            return switch (context.operator.getType()) {
                case PLUS -> value;
                case MINUS -> new NumericExpression.Operation(NumericExpression.Operator.SUBTRACT, new NumericExpression.Literal(0), value);
                default -> throw new IllegalArgumentException("Unsupported unary operator " + context.operator.getText());
            };
        }

        @Override
        public NumericExpression visitBinaryFunction(BinaryFunctionContext context)
        {
            NumericExpression left = visit(context.left);
            NumericExpression right = visit(context.right);
            return switch (context.binaryFunctionName().name.getType()) {
                case MIN -> new NumericExpression.Operation(NumericExpression.Operator.MIN, left, right);
                case MAX -> new NumericExpression.Operation(NumericExpression.Operator.MAX, left, right);
                default -> throw new IllegalArgumentException("Unsupported binary function " + context.binaryFunctionName().getText());
            };
        }

        @Override
        public NumericExpression visitNumericLiteral(NumericLiteralContext context)
        {
            return new NumericExpression.Literal(Long.parseLong(context.INTEGER_VALUE().getText()));
        }

        @Override
        public NumericExpression visitNullLiteral(NullLiteralContext context)
        {
            // Mirror the legacy calculator, which treats NULL as 0 in a type calculation.
            return new NumericExpression.Literal(0);
        }

        @Override
        public NumericExpression visitIdentifier(IdentifierContext context)
        {
            return new NumericExpression.Variable(context.getText());
        }

        @Override
        public NumericExpression visitParenthesizedExpression(ParenthesizedExpressionContext context)
        {
            return visit(context.expression());
        }

        @Override
        public NumericExpression visitIfExpression(TypeCalculationParser.IfExpressionContext context)
        {
            NumericExpression.ComparisonOperator operator = switch (context.operator.getText()) {
                case ">" -> NumericExpression.ComparisonOperator.GREATER_THAN;
                case "<" -> NumericExpression.ComparisonOperator.LESS_THAN;
                case ">=" -> NumericExpression.ComparisonOperator.GREATER_THAN_OR_EQUAL;
                case "<=" -> NumericExpression.ComparisonOperator.LESS_THAN_OR_EQUAL;
                case "=" -> NumericExpression.ComparisonOperator.EQUAL;
                case "!=" -> NumericExpression.ComparisonOperator.NOT_EQUAL;
                default -> throw new IllegalArgumentException("Unsupported comparison operator " + context.operator.getText());
            };
            return new NumericExpression.Conditional(
                    new NumericExpression.Comparison(operator, visit(context.left), visit(context.right)),
                    visit(context.ifTrue),
                    visit(context.ifFalse));
        }
    }

    private static ParserRuleContext parseTypeCalculation(String calculation)
    {
        TypeCalculationLexer lexer = new TypeCalculationLexer(CharStreams.fromString(calculation));
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        TypeCalculationParser parser = new TypeCalculationParser(tokenStream);

        lexer.removeErrorListeners();
        lexer.addErrorListener(ERROR_LISTENER);

        parser.removeErrorListeners();

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            parser.setErrorHandler(new BailErrorStrategy());
            tree = parser.typeCalculation();
        }
        catch (ParseCancellationException e) {
            // if we fail, parse with LL mode
            parser.reset();
            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            parser.setErrorHandler(new DefaultErrorStrategy());
            parser.addErrorListener(ERROR_LISTENER);
            tree = parser.typeCalculation();
        }
        return tree;
    }

    private static class CalculateTypeVisitor
            extends TypeCalculationBaseVisitor<BigInteger>
    {
        private final Map<String, Long> inputs;

        public CalculateTypeVisitor(Map<String, Long> inputs)
        {
            this.inputs = requireNonNull(inputs);
        }

        @Override
        public BigInteger visitTypeCalculation(TypeCalculationContext ctx)
        {
            return visit(ctx.expression());
        }

        @Override
        public BigInteger visitIfExpression(TypeCalculationParser.IfExpressionContext ctx)
        {
            BigInteger left = visit(ctx.left);
            BigInteger right = visit(ctx.right);

            boolean condition = switch (ctx.operator.getText()) {
                case ">" -> left.compareTo(right) > 0;
                case "<" -> left.compareTo(right) < 0;
                case ">=" -> left.compareTo(right) >= 0;
                case "<=" -> left.compareTo(right) <= 0;
                case "=" -> left.equals(right);
                case "!=" -> !left.equals(right);
                default -> throw new IllegalStateException("Unsupported if operator " + ctx.operator.getText());
            };

            if (condition) {
                return visit(ctx.ifTrue);
            }

            return visit(ctx.ifFalse);
        }

        @Override
        public BigInteger visitArithmeticBinary(ArithmeticBinaryContext ctx)
        {
            BigInteger left = visit(ctx.left);
            BigInteger right = visit(ctx.right);
            return switch (ctx.operator.getType()) {
                case PLUS -> left.add(right);
                case MINUS -> left.subtract(right);
                case ASTERISK -> left.multiply(right);
                case SLASH -> left.divide(right);
                default -> throw new IllegalStateException("Unsupported binary operator " + ctx.operator.getText());
            };
        }

        @Override
        public BigInteger visitArithmeticUnary(ArithmeticUnaryContext ctx)
        {
            BigInteger value = visit(ctx.expression());
            return switch (ctx.operator.getType()) {
                case PLUS -> value;
                case MINUS -> value.negate();
                default -> throw new IllegalStateException("Unsupported unary operator " + ctx.operator.getText());
            };
        }

        @Override
        public BigInteger visitBinaryFunction(BinaryFunctionContext ctx)
        {
            BigInteger left = visit(ctx.left);
            BigInteger right = visit(ctx.right);
            return switch (ctx.binaryFunctionName().name.getType()) {
                case MIN -> left.min(right);
                case MAX -> left.max(right);
                default -> throw new IllegalArgumentException("Unsupported binary function " + ctx.binaryFunctionName().getText());
            };
        }

        @Override
        public BigInteger visitNumericLiteral(NumericLiteralContext ctx)
        {
            return new BigInteger(ctx.INTEGER_VALUE().getText());
        }

        @Override
        public BigInteger visitNullLiteral(NullLiteralContext ctx)
        {
            return BigInteger.ZERO;
        }

        @Override
        public BigInteger visitIdentifier(IdentifierContext ctx)
        {
            String identifier = ctx.getText();
            Long value = inputs.get(identifier);
            checkState(value != null, "value for variable '%s' is not specified in the inputs", identifier);
            return BigInteger.valueOf(value);
        }

        @Override
        public BigInteger visitParenthesizedExpression(ParenthesizedExpressionContext ctx)
        {
            return visit(ctx.expression());
        }
    }
}
