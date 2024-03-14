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
package io.trino.sql.ir;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static io.trino.sql.SqlFormatter.formatName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class ExpressionFormatter
{
    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression)
    {
        return new Formatter(Optional.empty(), Optional.empty()).process(expression, null);
    }

    public static class Formatter
            extends IrVisitor<String, Void>
    {
        private final Optional<Function<Literal, String>> literalFormatter;
        private final Optional<Function<SymbolReference, String>> symbolReferenceFormatter;

        public Formatter(
                Optional<Function<Literal, String>> literalFormatter,
                Optional<Function<SymbolReference, String>> symbolReferenceFormatter)
        {
            this.literalFormatter = requireNonNull(literalFormatter, "literalFormatter is null");
            this.symbolReferenceFormatter = requireNonNull(symbolReferenceFormatter, "symbolReferenceFormatter is null");
        }

        @Override
        protected String visitRow(Row node, Void context)
        {
            return node.getItems().stream()
                    .map(child -> process(child, context))
                    .collect(joining(", ", "ROW (", ")"));
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: %s.visit%s".formatted(getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> String.valueOf(node.getValue()));
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> formatStringLiteral(node.getValue()));
        }

        @Override
        protected String visitBinaryLiteral(BinaryLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> "X'" + BaseEncoding.base16().encode(node.getValue()) + "'");
        }

        @Override
        protected String visitArray(Array node, Void context)
        {
            return node.getValues().stream()
                    .map(ExpressionFormatter::formatExpression)
                    .collect(joining(",", "ARRAY[", "]"));
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            return formatExpression(node.getBase()) + "[" + formatExpression(node.getIndex()) + "]";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> Long.toString(node.getValue()));
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> doubleFormatter.get().format(node.getValue()));
        }

        @Override
        protected String visitDecimalLiteral(DecimalLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    // TODO return node value without "DECIMAL '..'" when FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
                    .orElseGet(() -> "DECIMAL '" + node.getValue() + "'");
        }

        @Override
        protected String visitGenericLiteral(GenericLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> node.getType() + " " + formatStringLiteral(node.getValue()));
        }

        @Override
        protected String visitNullLiteral(NullLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElse("null");
        }

        @Override
        protected String visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            if (literalFormatter.isPresent()) {
                return literalFormatter.get().apply(node);
            }
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "-" : "";
            StringBuilder builder = new StringBuilder()
                    .append("INTERVAL ")
                    .append(sign)
                    .append("'").append(node.getValue()).append("' ")
                    .append(node.getStartField());

            if (node.getEndField().isPresent()) {
                builder.append(" TO ").append(node.getEndField().get());
            }
            return builder.toString();
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            return formatName(node.getName()) + '(' + joinExpressions(node.getArguments()) + ')';
        }

        @Override
        protected String visitLambdaExpression(LambdaExpression node, Void context)
        {
            return "(" +
                    String.join(", ", node.getArguments()) +
                    ") -> " +
                    process(node.getBody(), context);
        }

        @Override
        protected String visitSymbolReference(SymbolReference node, Void context)
        {
            if (symbolReferenceFormatter.isPresent()) {
                return symbolReferenceFormatter.get().apply(node);
            }
            return node.getName();
        }

        @Override
        protected String visitBindExpression(BindExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("\"$INTERNAL$BIND\"(");
            for (Expression value : node.getValues()) {
                builder.append(process(value, context))
                        .append(", ");
            }
            builder.append(process(node.getFunction(), context))
                    .append(")");
            return builder.toString();
        }

        @Override
        protected String visitLogicalExpression(LogicalExpression node, Void context)
        {
            return "(" +
                    node.getTerms().stream()
                            .map(term -> process(term, context))
                            .collect(joining(" " + node.getOperator().toString() + " ")) +
                    ")";
        }

        @Override
        protected String visitNotExpression(NotExpression node, Void context)
        {
            return "(NOT " + process(node.getValue(), context) + ")";
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IS NULL)";
        }

        @Override
        protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IS NOT NULL)";
        }

        @Override
        protected String visitNullIfExpression(NullIfExpression node, Void context)
        {
            return "NULLIF(" + process(node.getFirst(), context) + ", " + process(node.getSecond(), context) + ')';
        }

        @Override
        protected String visitIfExpression(IfExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("IF(")
                    .append(process(node.getCondition(), context))
                    .append(", ")
                    .append(process(node.getTrueValue(), context));
            if (node.getFalseValue().isPresent()) {
                builder.append(", ")
                        .append(process(node.getFalseValue().get(), context));
            }
            builder.append(")");
            return builder.toString();
        }

        @Override
        protected String visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
        }

        @Override
        protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            String value = process(node.getValue(), context);

            return switch (node.getSign()) {
                // Unary is ambiguous with respect to negative numbers. "-1" parses as a number, but "-(1)" parses as "unaryMinus(number)"
                // The parentheses are needed to ensure the parsing roundtrips properly.
                case MINUS -> "-(" + value + ")";
                case PLUS -> "+" + value;
            };
        }

        @Override
        protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), context) + " AS " + node.getType().getDisplayName() + ")";
        }

        @Override
        protected String visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }

            node.getDefaultValue()
                    .ifPresent(value -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE")
                    .add(process(node.getOperand(), context));

            for (WhenClause whenClause : node.getWhenClauses()) {
                parts.add(process(whenClause, context));
            }

            node.getDefaultValue()
                    .ifPresent(value -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitWhenClause(WhenClause node, Void context)
        {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
        }

        @Override
        protected String visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " BETWEEN " +
                    process(node.getMin(), context) + " AND " + process(node.getMax(), context) + ")";
        }

        @Override
        protected String visitInPredicate(InPredicate node, Void context)
        {
            return "(" + process(node.getValue(), context) + " IN " + joinExpressions(node.getValueList()) + ")";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
        }

        private String joinExpressions(List<Expression> expressions)
        {
            return expressions.stream()
                    .map(e -> process(e, null))
                    .collect(joining(", "));
        }
    }

    static String formatStringLiteral(String s)
    {
        return "'" + s.replace("'", "''") + "'";
    }
}
