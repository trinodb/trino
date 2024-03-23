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
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class ExpressionFormatter
{
    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression)
    {
        return new Formatter(Optional.empty(), Optional.empty()).process(expression, null);
    }

    public static class Formatter
            extends IrVisitor<String, Void>
    {
        private final Optional<Function<Constant, String>> literalFormatter;
        private final Optional<Function<Reference, String>> symbolReferenceFormatter;

        public Formatter(
                Optional<Function<Constant, String>> literalFormatter,
                Optional<Function<Reference, String>> symbolReferenceFormatter)
        {
            this.literalFormatter = requireNonNull(literalFormatter, "literalFormatter is null");
            this.symbolReferenceFormatter = requireNonNull(symbolReferenceFormatter, "symbolReferenceFormatter is null");
        }

        @Override
        protected String visitRow(Row node, Void context)
        {
            return node.items().stream()
                    .map(child -> process(child, context))
                    .collect(joining(", ", "ROW (", ")"));
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: %s.visit%s".formatted(getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected String visitFieldReference(FieldReference node, Void context)
        {
            return formatExpression(node.base()) + "[" + formatExpression(node.index()) + "]";
        }

        @Override
        protected String visitConstant(Constant node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> {
                        if (node.value() == null) {
                            return "null::" + node.type();
                        }
                        else {
                            return node.type() + " '" + node.type().getObjectValue(null, node.getValueAsBlock(), 0) + "'";
                        }
                    });
        }

        @Override
        protected String visitCall(Call node, Void context)
        {
            return node.function().getName().toString() + '(' + joinExpressions(node.arguments()) + ')';
        }

        @Override
        protected String visitLambda(Lambda node, Void context)
        {
            return "(" +
                    node.arguments().stream()
                            .map(Symbol::getName)
                            .collect(joining(", ")) +
                    ") -> " +
                    process(node.body(), context);
        }

        @Override
        protected String visitReference(Reference node, Void context)
        {
            if (symbolReferenceFormatter.isPresent()) {
                return symbolReferenceFormatter.get().apply(node);
            }
            return node.name();
        }

        @Override
        protected String visitBind(Bind node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("\"$bind\"(");
            for (Expression value : node.values()) {
                builder.append(process(value, context))
                        .append(", ");
            }
            builder.append(process(node.function(), context))
                    .append(")");
            return builder.toString();
        }

        @Override
        protected String visitLogical(Logical node, Void context)
        {
            return "(" +
                    node.terms().stream()
                            .map(term -> process(term, context))
                            .collect(joining(" " + node.operator().toString() + " ")) +
                    ")";
        }

        @Override
        protected String visitNot(Not node, Void context)
        {
            return "(NOT " + process(node.value(), context) + ")";
        }

        @Override
        protected String visitComparison(Comparison node, Void context)
        {
            return formatBinaryExpression(node.operator().getValue(), node.left(), node.right());
        }

        @Override
        protected String visitIsNull(IsNull node, Void context)
        {
            return "(" + process(node.value(), context) + " IS NULL)";
        }

        @Override
        protected String visitNullIf(NullIf node, Void context)
        {
            return "NULLIF(" + process(node.first(), context) + ", " + process(node.second(), context) + ')';
        }

        @Override
        protected String visitCoalesce(Coalesce node, Void context)
        {
            return "COALESCE(" + joinExpressions(node.operands()) + ")";
        }

        @Override
        protected String visitNegation(Negation node, Void context)
        {
            return "-(" + process(node.value(), context) + ")";
        }

        @Override
        protected String visitArithmetic(Arithmetic node, Void context)
        {
            return formatBinaryExpression(node.operator().getValue(), node.left(), node.right());
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return (node.safe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.expression(), context) + " AS " + node.type().getDisplayName() + ")";
        }

        @Override
        protected String visitCase(Case node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();
            parts.add("CASE");
            for (WhenClause whenClause : node.whenClauses()) {
                parts.add(format(whenClause, context));
            }

            node.defaultValue()
                    .ifPresent(value -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        @Override
        protected String visitSwitch(Switch node, Void context)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            parts.add("CASE")
                    .add(process(node.operand(), context));

            for (WhenClause whenClause : node.whenClauses()) {
                parts.add(format(whenClause, context));
            }

            node.defaultValue()
                    .ifPresent(value -> parts.add("ELSE").add(process(value, context)));

            parts.add("END");

            return "(" + Joiner.on(' ').join(parts.build()) + ")";
        }

        protected String format(WhenClause node, Void context)
        {
            return "WHEN " + process(node.getOperand(), context) + " THEN " + process(node.getResult(), context);
        }

        @Override
        protected String visitBetween(Between node, Void context)
        {
            return "(" + process(node.value(), context) + " BETWEEN " +
                    process(node.min(), context) + " AND " + process(node.max(), context) + ")";
        }

        @Override
        protected String visitIn(In node, Void context)
        {
            return "(" + process(node.value(), context) + " IN (" + joinExpressions(node.valueList()) + "))";
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
}
