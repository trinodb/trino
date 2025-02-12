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
package io.trino.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonPathInvocation;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.ReservedIdentifiers.reserved;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static io.trino.sql.SqlFormatter.formatName;
import static io.trino.sql.SqlFormatter.formatSql;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public final class ExpressionFormatter
{
    private static final ThreadLocal<DecimalFormat> DOUBLE_FORMATTER = ThreadLocal.withInitial(
            () -> new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

    private ExpressionFormatter() {}

    public static String formatExpression(Expression expression)
    {
        return new Formatter(Optional.empty()).process(expression, null);
    }

    public static class Formatter
            extends AstVisitor<String, Void>
    {
        private final Optional<Function<Literal, String>> literalFormatter;

        public Formatter(Optional<Function<Literal, String>> literalFormatter)
        {
            this.literalFormatter = requireNonNull(literalFormatter, "literalFormatter is null");
        }

        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
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
        protected String visitAtTimeZone(AtTimeZone node, Void context)
        {
            return process(node.getValue(), context) +
                    " AT TIME ZONE " +
                    process(node.getTimeZone(), context);
        }

        @Override
        protected String visitCurrentCatalog(CurrentCatalog node, Void context)
        {
            return "CURRENT_CATALOG";
        }

        @Override
        protected String visitCurrentSchema(CurrentSchema node, Void context)
        {
            return "CURRENT_SCHEMA";
        }

        @Override
        protected String visitCurrentUser(CurrentUser node, Void context)
        {
            return "CURRENT_USER";
        }

        @Override
        protected String visitCurrentPath(CurrentPath node, Void context)
        {
            return "CURRENT_PATH";
        }

        @Override
        protected String visitTrim(Trim node, Void context)
        {
            if (node.getTrimCharacter().isEmpty()) {
                return "trim(%s FROM %s)".formatted(node.getSpecification(), process(node.getTrimSource(), context));
            }

            return "trim(%s %s FROM %s)".formatted(node.getSpecification(), process(node.getTrimCharacter().get(), context), process(node.getTrimSource(), context));
        }

        @Override
        protected String visitFormat(Format node, Void context)
        {
            return "format(" + joinExpressions(node.getArguments()) + ")";
        }

        @Override
        protected String visitCurrentDate(CurrentDate node, Void context)
        {
            return "current_date";
        }

        @Override
        protected String visitCurrentTime(CurrentTime node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("current_time");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        @Override
        protected String visitCurrentTimestamp(CurrentTimestamp node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("current_timestamp");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        @Override
        protected String visitLocalTime(LocalTime node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("localtime");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        @Override
        protected String visitLocalTimestamp(LocalTimestamp node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("localtimestamp");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        @Override
        protected String visitExtract(Extract node, Void context)
        {
            return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
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
                    .orElseGet(() -> "X'" + node.toHexString() + "'");
        }

        @Override
        protected String visitParameter(Parameter node, Void context)
        {
            return "?";
        }

        @Override
        protected String visitAllRows(AllRows node, Void context)
        {
            return "ALL";
        }

        @Override
        protected String visitArray(Array node, Void context)
        {
            return node.getValues().stream()
                    .map(SqlFormatter::formatSql)
                    .collect(joining(",", "ARRAY[", "]"));
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Void context)
        {
            return formatSql(node.getBase()) + "[" + formatSql(node.getIndex()) + "]";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(node::getValue);
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> DOUBLE_FORMATTER.get().format(node.getValue()));
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
        protected String visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            return "(" + formatSql(node.getQuery()) + ")";
        }

        @Override
        protected String visitExists(ExistsPredicate node, Void context)
        {
            return "(EXISTS " + formatSql(node.getSubquery()) + ")";
        }

        @Override
        protected String visitIdentifier(Identifier node, Void context)
        {
            if (node.isDelimited() || reserved(node.getValue())) {
                return '"' + node.getValue().replace("\"", "\"\"") + '"';
            }
            return node.getValue();
        }

        @Override
        protected String visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, Void context)
        {
            return formatExpression(node.getName());
        }

        @Override
        protected String visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            String baseString = process(node.getBase(), context);
            return baseString + "." + node.getField().map(this::process).orElse("*");
        }

        @Override
        public String visitFieldReference(FieldReference node, Void context)
        {
            // add colon so this won't parse
            return ":input(" + node.getFieldIndex() + ")";
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            if (QualifiedName.of("LISTAGG").equals(node.getName())) {
                return visitListagg(node);
            }

            StringBuilder builder = new StringBuilder();

            if (node.getProcessingMode().isPresent()) {
                builder.append(node.getProcessingMode().get().getMode())
                        .append(" ");
            }

            String arguments = joinExpressions(node.getArguments());
            if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
                arguments = "*";
            }
            if (node.isDistinct()) {
                arguments = "DISTINCT " + arguments;
            }

            builder.append(formatName(node.getName()))
                    .append('(').append(arguments);

            if (node.getOrderBy().isPresent()) {
                builder.append(' ').append(formatOrderBy(node.getOrderBy().get()));
            }

            builder.append(')');

            node.getNullTreatment().ifPresent(nullTreatment ->
                    builder.append(switch (nullTreatment) {
                        case IGNORE -> " IGNORE NULLS";
                        case RESPECT -> " RESPECT NULLS";
                    }));

            if (node.getFilter().isPresent()) {
                builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), context));
            }

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(formatWindow(node.getWindow().get()));
            }

            return builder.toString();
        }

        @Override
        protected String visitWindowOperation(WindowOperation node, Void context)
        {
            return process(node.getName(), context) + " OVER " + formatWindow(node.getWindow());
        }

        @Override
        protected String visitLambdaExpression(LambdaExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(');
            Joiner.on(", ").appendTo(builder, node.getArguments());
            builder.append(") -> ");
            builder.append(process(node.getBody(), context));
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
        protected String visitTryExpression(TryExpression node, Void context)
        {
            return "TRY(" + process(node.getInnerExpression(), context) + ")";
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
        protected String visitLikePredicate(LikePredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(process(node.getValue(), context))
                    .append(" LIKE ")
                    .append(process(node.getPattern(), context));

            node.getEscape().ifPresent(escape -> builder.append(" ESCAPE ")
                    .append(process(escape, context)));

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            if (node.getTarget().isPresent()) {
                builder.append(process(node.getTarget().get(), context));
                builder.append(".*");
            }
            else {
                builder.append("*");
            }

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (");
                Joiner.on(", ").appendTo(builder, node.getAliases().stream()
                        .map(alias -> process(alias, context))
                        .collect(toList()));
                builder.append(")");
            }

            return builder.toString();
        }

        @Override
        public String visitCast(Cast node, Void context)
        {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) + ")";
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
            return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(), context) + ")";
        }

        @Override
        protected String visitInListExpression(InListExpression node, Void context)
        {
            return "(" + joinExpressions(node.getValues()) + ")";
        }

        private String visitFilter(Expression node, Void context)
        {
            return "(WHERE " + process(node, context) + ')';
        }

        @Override
        protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, Void context)
        {
            return "(%s %s %s %s)".formatted(
                    process(node.getValue(), context),
                    node.getOperator().getValue(),
                    node.getQuantifier(),
                    process(node.getSubquery(), context));
        }

        @Override
        protected String visitGroupingOperation(GroupingOperation node, Void context)
        {
            return "GROUPING (" + joinExpressions(node.getGroupingColumns()) + ")";
        }

        @Override
        protected String visitRowDataType(RowDataType node, Void context)
        {
            return node.getFields().stream()
                    .map(this::process)
                    .collect(joining(", ", "ROW(", ")"));
        }

        @Override
        protected String visitRowField(RowDataType.Field node, Void context)
        {
            StringBuilder result = new StringBuilder();

            if (node.getName().isPresent()) {
                result.append(process(node.getName().get(), context));
                result.append(" ");
            }

            result.append(process(node.getType(), context));

            return result.toString();
        }

        @Override
        protected String visitGenericDataType(GenericDataType node, Void context)
        {
            StringBuilder result = new StringBuilder();
            result.append(node.getName());

            if (!node.getArguments().isEmpty()) {
                result.append(node.getArguments().stream()
                        .map(this::process)
                        .collect(joining(", ", "(", ")")));
            }

            return result.toString();
        }

        @Override
        protected String visitTypeParameter(TypeParameter node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected String visitNumericTypeParameter(NumericParameter node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected String visitIntervalDataType(IntervalDayTimeDataType node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("INTERVAL ");
            builder.append(node.getFrom());
            if (node.getFrom() != node.getTo()) {
                builder.append(" TO ")
                        .append(node.getTo());
            }

            return builder.toString();
        }

        @Override
        protected String visitDateTimeType(DateTimeDataType node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append(node.getType().toString().toLowerCase(Locale.ENGLISH)); // TODO: normalize to upper case according to standard SQL semantics
            if (node.getPrecision().isPresent()) {
                builder.append("(")
                        .append(node.getPrecision().get())
                        .append(")");
            }

            if (node.isWithTimeZone()) {
                builder.append(" with time zone"); // TODO: normalize to upper case according to standard SQL semantics
            }

            return builder.toString();
        }

        @Override
        protected String visitJsonExists(JsonExists node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("JSON_EXISTS(")
                    .append(formatJsonPathInvocation(node.getJsonPathInvocation()))
                    .append(" ")
                    .append(node.getErrorBehavior())
                    .append(" ON ERROR")
                    .append(")");

            return builder.toString();
        }

        @Override
        protected String visitJsonValue(JsonValue node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("JSON_VALUE(")
                    .append(formatJsonPathInvocation(node.getJsonPathInvocation()));

            if (node.getReturnedType().isPresent()) {
                builder.append(" RETURNING ")
                        .append(process(node.getReturnedType().get()));
            }

            builder.append(" ")
                    .append(node.getEmptyBehavior())
                    .append(node.getEmptyDefault().map(expression -> " " + process(expression)).orElse(""))
                    .append(" ON EMPTY ")
                    .append(node.getErrorBehavior())
                    .append(node.getErrorDefault().map(expression -> " " + process(expression)).orElse(""))
                    .append(" ON ERROR")
                    .append(")");

            return builder.toString();
        }

        @Override
        protected String visitJsonQuery(JsonQuery node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("JSON_QUERY(")
                    .append(formatJsonPathInvocation(node.getJsonPathInvocation()));

            if (node.getReturnedType().isPresent()) {
                builder.append(" RETURNING ")
                        .append(process(node.getReturnedType().get()))
                        .append(node.getOutputFormat().map(value -> " FORMAT " + value).orElse(""));
            }

            builder.append(switch (node.getWrapperBehavior()) {
                case WITHOUT -> " WITHOUT ARRAY WRAPPER";
                case CONDITIONAL -> " WITH CONDITIONAL ARRAY WRAPPER";
                case UNCONDITIONAL -> " WITH UNCONDITIONAL ARRAY WRAPPER";
            });

            if (node.getQuotesBehavior().isPresent()) {
                builder.append(switch (node.getQuotesBehavior().get()) {
                    case KEEP -> " KEEP QUOTES ON SCALAR STRING";
                    case OMIT -> " OMIT QUOTES ON SCALAR STRING";
                });
            }

            builder.append(" ")
                    .append(node.getEmptyBehavior())
                    .append(" ON EMPTY ")
                    .append(node.getErrorBehavior())
                    .append(" ON ERROR")
                    .append(")");

            return builder.toString();
        }

        @Override
        protected String visitJsonObject(JsonObject node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("JSON_OBJECT(");

            if (!node.getMembers().isEmpty()) {
                builder.append(node.getMembers().stream()
                        .map(member -> "KEY " + formatExpression(member.getKey()) + " VALUE " + formatJsonExpression(member.getValue(), member.getFormat()))
                        .collect(joining(", ")));
                builder.append(node.isNullOnNull() ? " NULL ON NULL" : " ABSENT ON NULL");
                builder.append(node.isUniqueKeys() ? " WITH UNIQUE KEYS" : " WITHOUT UNIQUE KEYS");
            }

            if (node.getReturnedType().isPresent()) {
                builder.append(" RETURNING ")
                        .append(process(node.getReturnedType().get()))
                        .append(node.getOutputFormat().map(value -> " FORMAT " + value).orElse(""));
            }

            builder.append(")");

            return builder.toString();
        }

        @Override
        protected String visitJsonArray(JsonArray node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("JSON_ARRAY(");

            if (!node.getElements().isEmpty()) {
                builder.append(node.getElements().stream()
                        .map(element -> formatJsonExpression(element.getValue(), element.getFormat()))
                        .collect(joining(", ")));
                builder.append(node.isNullOnNull() ? " NULL ON NULL" : " ABSENT ON NULL");
            }

            if (node.getReturnedType().isPresent()) {
                builder.append(" RETURNING ")
                        .append(process(node.getReturnedType().get()))
                        .append(node.getOutputFormat().map(value -> " FORMAT " + value).orElse(""));
            }

            builder.append(")");

            return builder.toString();
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

        /**
         * Returns the formatted `LISTAGG` function call corresponding to the specified node.
         * <p>
         * During the parsing of the syntax tree, the `LISTAGG` expression is synthetically converted
         * to a function call. This method formats the specified {@link FunctionCall} node to correspond
         * to the standardised syntax of the `LISTAGG` expression.
         *
         * @param node the `LISTAGG` function call
         */
        private String visitListagg(FunctionCall node)
        {
            StringBuilder builder = new StringBuilder();

            List<Expression> arguments = node.getArguments();
            Expression expression = arguments.get(0);
            Expression separator = arguments.get(1);
            BooleanLiteral overflowError = (BooleanLiteral) arguments.get(2);
            Expression overflowFiller = arguments.get(3);
            BooleanLiteral showOverflowEntryCount = (BooleanLiteral) arguments.get(4);

            String innerArguments = joinExpressions(ImmutableList.of(expression, separator));
            if (node.isDistinct()) {
                innerArguments = "DISTINCT " + innerArguments;
            }

            builder.append("LISTAGG")
                    .append('(').append(innerArguments);

            builder.append(" ON OVERFLOW ");
            if (overflowError.getValue()) {
                builder.append(" ERROR");
            }
            else {
                builder.append(" TRUNCATE")
                        .append(' ')
                        .append(process(overflowFiller, null));
                if (showOverflowEntryCount.getValue()) {
                    builder.append(" WITH COUNT");
                }
                else {
                    builder.append(" WITHOUT COUNT");
                }
            }

            builder.append(')');

            if (node.getOrderBy().isPresent()) {
                builder.append(" WITHIN GROUP ")
                        .append('(')
                        .append(formatOrderBy(node.getOrderBy().get()))
                        .append(')');
            }

            if (node.getFilter().isPresent()) {
                builder.append(" FILTER ").append(visitFilter(node.getFilter().get(), null));
            }

            if (node.getWindow().isPresent()) {
                builder.append(" OVER ").append(formatWindow(node.getWindow().get()));
            }

            return builder.toString();
        }
    }

    static String formatStringLiteral(String s)
    {
        return "'" + s.replace("'", "''") + "'";
    }

    public static String formatOrderBy(OrderBy orderBy)
    {
        return "ORDER BY " + formatSortItems(orderBy.getSortItems());
    }

    public static String formatSortItems(List<SortItem> sortItems)
    {
        return sortItems.stream()
                .map(sortItemFormatterFunction())
                .collect(joining(", "));
    }

    private static String formatWindow(Window window)
    {
        if (window instanceof WindowReference) {
            return formatExpression(((WindowReference) window).getName());
        }

        return formatWindowSpecification((WindowSpecification) window);
    }

    static String formatWindowSpecification(WindowSpecification windowSpecification)
    {
        List<String> parts = new ArrayList<>();

        if (windowSpecification.getExistingWindowName().isPresent()) {
            parts.add(formatExpression(windowSpecification.getExistingWindowName().get()));
        }
        if (!windowSpecification.getPartitionBy().isEmpty()) {
            parts.add("PARTITION BY " + windowSpecification.getPartitionBy().stream()
                    .map(ExpressionFormatter::formatExpression)
                    .collect(joining(", ")));
        }
        if (windowSpecification.getOrderBy().isPresent()) {
            parts.add(formatOrderBy(windowSpecification.getOrderBy().get()));
        }
        if (windowSpecification.getFrame().isPresent()) {
            parts.add(formatFrame(windowSpecification.getFrame().get()));
        }

        return '(' + Joiner.on(' ').join(parts) + ')';
    }

    private static String formatFrame(WindowFrame windowFrame)
    {
        StringBuilder builder = new StringBuilder();

        if (!windowFrame.getMeasures().isEmpty()) {
            builder.append("MEASURES ")
                    .append(windowFrame.getMeasures().stream()
                            .map(measure -> formatExpression(measure.getExpression()) + " AS " + formatExpression(measure.getName()))
                            .collect(joining(", ")))
                    .append(" ");
        }

        builder.append(windowFrame.getType().toString())
                .append(' ');

        if (windowFrame.getEnd().isPresent()) {
            builder.append("BETWEEN ")
                    .append(formatFrameBound(windowFrame.getStart()))
                    .append(" AND ")
                    .append(formatFrameBound(windowFrame.getEnd().get()));
        }
        else {
            builder.append(formatFrameBound(windowFrame.getStart()));
        }

        windowFrame.getAfterMatchSkipTo().ifPresent(skipTo ->
                builder.append(" ")
                        .append(formatSkipTo(skipTo)));
        windowFrame.getPatternSearchMode().ifPresent(searchMode ->
                builder.append(" ")
                        .append(searchMode.getMode().name()));
        windowFrame.getPattern().ifPresent(pattern ->
                builder.append(" PATTERN(")
                        .append(formatPattern(pattern))
                        .append(")"));
        if (!windowFrame.getSubsets().isEmpty()) {
            builder.append(" SUBSET ");
            builder.append(windowFrame.getSubsets().stream()
                    .map(subset -> formatExpression(subset.getName()) + " = " + subset.getIdentifiers().stream()
                            .map(ExpressionFormatter::formatExpression).collect(joining(", ", "(", ")")))
                    .collect(joining(", ")));
        }
        if (!windowFrame.getVariableDefinitions().isEmpty()) {
            builder.append(" DEFINE ");
            builder.append(windowFrame.getVariableDefinitions().stream()
                    .map(variable -> formatExpression(variable.getName()) + " AS " + formatExpression(variable.getExpression()))
                    .collect(joining(", ")));
        }

        return builder.toString();
    }

    private static String formatFrameBound(FrameBound frameBound)
    {
        return switch (frameBound.getType()) {
            case UNBOUNDED_PRECEDING -> "UNBOUNDED PRECEDING";
            case PRECEDING -> formatExpression(frameBound.getValue().orElseThrow()) + " PRECEDING";
            case CURRENT_ROW -> "CURRENT ROW";
            case FOLLOWING -> formatExpression(frameBound.getValue().orElseThrow()) + " FOLLOWING";
            case UNBOUNDED_FOLLOWING -> "UNBOUNDED FOLLOWING";
        };
    }

    public static String formatSkipTo(SkipTo skipTo)
    {
        return switch (skipTo.getPosition()) {
            case PAST_LAST -> "AFTER MATCH SKIP PAST LAST ROW";
            case NEXT -> "AFTER MATCH SKIP TO NEXT ROW";
            case LAST -> {
                checkState(skipTo.getIdentifier().isPresent(), "missing identifier in AFTER MATCH SKIP TO LAST");
                yield "AFTER MATCH SKIP TO LAST " + formatExpression(skipTo.getIdentifier().orElseThrow());
            }
            case FIRST -> {
                checkState(skipTo.getIdentifier().isPresent(), "missing identifier in AFTER MATCH SKIP TO FIRST");
                yield "AFTER MATCH SKIP TO FIRST " + formatExpression(skipTo.getIdentifier().orElseThrow());
            }
        };
    }

    static String formatGroupBy(List<GroupingElement> groupingElements)
    {
        return groupingElements.stream().map(groupingElement -> {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                List<Expression> columns = groupingElement.getExpressions();
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns));
                }
                else {
                    result = formatGroupingSet(columns);
                }
            }
            else if (groupingElement instanceof GroupingSets groupingSets) {
                String type = switch (groupingSets.getType()) {
                    case EXPLICIT -> "GROUPING SETS";
                    case CUBE -> "CUBE";
                    case ROLLUP -> "ROLLUP";
                };

                result = groupingSets.getSets().stream()
                        .map(ExpressionFormatter::formatGroupingSet)
                        .collect(joining(", ", type + " (", ")"));
            }
            return result;
        })
        .collect(joining(", "));
    }

    private static String formatGroupingSet(List<Expression> groupingSet)
    {
        return groupingSet.stream()
                .map(ExpressionFormatter::formatExpression)
                .collect(joining(", ", "(", ")"));
    }

    private static Function<SortItem, String> sortItemFormatterFunction()
    {
        return input -> {
            StringBuilder builder = new StringBuilder();

            builder.append(formatExpression(input.getSortKey()));

            builder.append(switch (input.getOrdering()) {
                case ASCENDING -> " ASC";
                case DESCENDING -> " DESC";
            });

            builder.append(switch (input.getNullOrdering()) {
                case FIRST -> " NULLS FIRST";
                case LAST -> " NULLS LAST";
                case UNDEFINED -> "";
            });

            return builder.toString();
        };
    }

    public static String formatJsonPathInvocation(JsonPathInvocation pathInvocation)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(formatJsonExpression(pathInvocation.getInputExpression(), Optional.of(pathInvocation.getInputFormat())))
                .append(", ")
                .append(formatExpression(pathInvocation.getJsonPath()));

        pathInvocation.getPathName().ifPresent(pathName -> builder
                .append(" AS ")
                .append(formatExpression(pathName)));

        if (!pathInvocation.getPathParameters().isEmpty()) {
            builder.append(" PASSING ");
            builder.append(formatJsonPathParameters(pathInvocation.getPathParameters()));
        }

        return builder.toString();
    }

    private static String formatJsonExpression(Expression expression, Optional<JsonPathParameter.JsonFormat> format)
    {
        return formatExpression(expression) + format.map(jsonFormat -> " FORMAT " + jsonFormat).orElse("");
    }

    private static String formatJsonPathParameters(List<JsonPathParameter> parameters)
    {
        return parameters.stream()
                .map(parameter -> formatJsonExpression(parameter.getParameter(), parameter.getFormat()) + " AS " + formatExpression(parameter.getName()))
                .collect(joining(", "));
    }
}
