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
import io.trino.sql.tree.ArrayWildcardSubscript;
import io.trino.sql.tree.AtLocal;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.AutoGroupBy;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.BooleanTestPredicate;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ComparisonPredicate;
import io.trino.sql.tree.CompositeIntervalQualifier;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DistinctFromPredicate;
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
import io.trino.sql.tree.IntervalDataType;
import io.trino.sql.tree.IntervalField;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IntervalValueExpression;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonConstructor;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonPathInvocation;
import io.trino.sql.tree.JsonPathParameter;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonSerialize;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MatchPredicate;
import io.trino.sql.tree.MethodCall;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.OverlapsPredicate;
import io.trino.sql.tree.Overlay;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonPredicate;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SimpleIntervalQualifier;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StaticMethodCall;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.UniquePredicate;
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

import static com.google.common.base.Preconditions.checkArgument;
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
    {
        public String process(Node node)
        {
            return process(node, null);
        }

        public String process(Node root, Void context)
        {
            return switch (root) {
                case AllColumns node -> visitAllColumns(node, context);
                case AllRows _ -> "ALL";
                case ArithmeticBinaryExpression node -> visitArithmeticBinary(node);
                case ArithmeticUnaryExpression node -> visitArithmeticUnary(node, context);
                case Array node -> visitArray(node);
                case ArrayWildcardSubscript node -> visitExpression(node);
                case AtLocal node -> process(node.getValue(), context) + " AT LOCAL";
                case AtTimeZone node -> visitAtTimeZone(node, context);
                case BetweenPredicate node -> visitBetweenPredicate(node, context);
                case BinaryLiteral node -> visitBinaryLiteral(node);
                case BooleanLiteral node -> visitBooleanLiteral(node);
                case BooleanTestPredicate node -> (node.isNegated() ? "IS NOT " : "IS ") + node.getTruthValue();
                case CallArgument node -> visitCallArgument(node, context);
                case Cast node -> visitCast(node, context);
                case CoalesceExpression node -> "COALESCE(" + joinExpressions(node.getOperands()) + ")";
                case ComparisonPredicate node -> node.getOperator().getValue() + " " + process(node.getRight(), context);
                case CompositeIntervalQualifier node -> visitCompositeIntervalQualifier(node);
                case CurrentCatalog _ -> "CURRENT_CATALOG";
                case CurrentDate _ -> "current_date";
                case CurrentPath _ -> "CURRENT_PATH";
                case CurrentSchema _ -> "CURRENT_SCHEMA";
                case CurrentTime node -> visitCurrentTime(node);
                case CurrentTimestamp node -> visitCurrentTimestamp(node);
                case CurrentUser _ -> "CURRENT_USER";
                case DateTimeDataType node -> visitDateTimeType(node);
                case DecimalLiteral node -> visitDecimalLiteral(node);
                case DereferenceExpression node -> visitDereferenceExpression(node, context);
                case DistinctFromPredicate node -> visitDistinctFromPredicate(node, context);
                case DoubleLiteral node -> visitDoubleLiteral(node);
                case ExistsPredicate node -> "(EXISTS " + formatSql(node.getSubquery()) + ")";
                case Extract node -> "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
                case FieldReference node -> visitFieldReference(node);
                case Format node -> "format(" + joinExpressions(node.getArguments()) + ")";
                case FunctionCall node -> visitFunctionCall(node, context);
                case GenericDataType node -> visitGenericDataType(node);
                case GenericLiteral node -> visitGenericLiteral(node);
                case GroupingOperation node -> "GROUPING (" + joinExpressions(node.getGroupingColumns()) + ")";
                case Identifier node -> visitIdentifier(node);
                case IfExpression node -> visitIfExpression(node, context);
                case InListExpression node -> "(" + joinExpressions(node.getValues()) + ")";
                case InPredicate node -> (node.isNegated() ? "NOT IN " : "IN ") + process(node.getValueList(), context);
                case IntervalDataType node -> "INTERVAL " + process(node.qualifier(), context);
                case IntervalLiteral node -> visitIntervalLiteral(node, context);
                case IntervalValueExpression node -> visitIntervalValueExpression(node, context);
                case IsNullPredicate node -> node.isNegated() ? "IS NOT NULL" : "IS NULL";
                case JsonArray node -> visitJsonArray(node);
                case JsonConstructor node -> "JSON(" + formatJsonExpression(node.getExpression(), Optional.of(node.getFormat())) + ")";
                case JsonExists node -> visitJsonExists(node);
                case JsonObject node -> visitJsonObject(node);
                case JsonQuery node -> visitJsonQuery(node);
                case JsonSerialize node -> visitJsonSerialize(node);
                case JsonValue node -> visitJsonValue(node);
                case LambdaArgumentDeclaration node -> formatExpression(node.getName());
                case LambdaExpression node -> visitLambdaExpression(node, context);
                case LikePredicate node -> visitLikePredicate(node, context);
                case LocalTime node -> visitLocalTime(node);
                case LocalTimestamp node -> visitLocalTimestamp(node);
                case LogicalExpression node -> visitLogicalExpression(node, context);
                case LongLiteral node -> visitLongLiteral(node);
                case MatchPredicate node -> visitMatchPredicate(node, context);
                case MethodCall node -> visitMethodCall(node);
                case NotExpression node -> "(NOT " + process(node.getValue(), context) + ")";
                case NullIfExpression node -> visitNullIfExpression(node, context);
                case NullLiteral node -> visitNullLiteral(node);
                case NumericParameter node -> node.getValue();
                case OverlapsPredicate node -> "OVERLAPS " + process(node.getRight(), context);
                case Overlay node -> visitOverlay(node, context);
                case Parameter _ -> "?";
                case Predicated node -> "(" + process(node.getValue(), context) + " " + process(node.getPredicate(), context) + ")";
                case QuantifiedComparisonPredicate node -> visitQuantifiedComparisonPredicate(node, context);
                case Row node -> visitRow(node, context);
                case Row.Field node -> visitRowField(node, context);
                case RowDataType node -> visitRowDataType(node);
                case RowDataType.Field node -> visitRowDataTypeField(node, context);
                case SearchedCaseExpression node -> visitSearchedCaseExpression(node, context);
                case SimpleCaseExpression node -> visitSimpleCaseExpression(node, context);
                case SimpleIntervalQualifier node -> visitSimpleIntervalQualifier(node);
                case StaticMethodCall node -> visitStaticMethodCall(node);
                case StringLiteral node -> visitStringLiteral(node);
                case SubqueryExpression node -> "(" + formatSql(node.getQuery()) + ")";
                case SubscriptExpression node -> formatSql(node.getBase()) + "[" + formatSql(node.getIndex()) + "]";
                case Trim node -> visitTrim(node, context);
                case TryExpression node -> "TRY(" + process(node.getInnerExpression(), context) + ")";
                case TypeParameter node -> process(node.getValue(), context);
                case UniquePredicate node -> "(UNIQUE " + formatSql(node.getSubquery()) + ")";
                case WhenClause node -> visitWhenClause(node, context);
                case WindowOperation node -> process(node.getName(), context) + " OVER " + formatWindow(node.getWindow());
                default -> throw new UnsupportedOperationException();
            };
        }

        private final Optional<Function<Literal, String>> literalFormatter;

        public Formatter(Optional<Function<Literal, String>> literalFormatter)
        {
            this.literalFormatter = requireNonNull(literalFormatter, "literalFormatter is null");
        }

        private String visitRow(Row node, Void context)
        {
            return node.getFields().stream()
                    .map(child -> process(child, context))
                    .collect(joining(", ", "ROW(", ")"));
        }

        private String visitRowField(Row.Field node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            builder.append(process(node.getExpression(), context));
            if (node.getName().isPresent()) {
                builder.append(" AS ");
                builder.append(process(node.getName().get(), context));
            }
            return builder.toString();
        }

        private String visitExpression(Expression node)
        {
            throw new UnsupportedOperationException("not yet implemented: %s.visit%s".formatted(getClass().getName(), node.getClass().getSimpleName()));
        }

        private String visitAtTimeZone(AtTimeZone node, Void context)
        {
            return process(node.getValue(), context) +
                    " AT TIME ZONE " +
                    process(node.getTimeZone(), context);
        }

        private String visitTrim(Trim node, Void context)
        {
            if (node.getTrimCharacter().isEmpty()) {
                return "trim(%s FROM %s)".formatted(node.getSpecification(), process(node.getTrimSource(), context));
            }

            return "trim(%s %s FROM %s)".formatted(node.getSpecification(), process(node.getTrimCharacter().get(), context), process(node.getTrimSource(), context));
        }

        private String visitOverlay(Overlay node, Void context)
        {
            String formatted = "OVERLAY(%s PLACING %s FROM %s".formatted(
                    process(node.getValue(), context),
                    process(node.getReplacement(), context),
                    process(node.getStart(), context));
            if (node.getLength().isPresent()) {
                formatted += " FOR " + process(node.getLength().get(), context);
            }
            return formatted + ")";
        }

        private String visitCurrentTime(CurrentTime node)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("current_time");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        private String visitCurrentTimestamp(CurrentTimestamp node)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("current_timestamp");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        private String visitLocalTime(LocalTime node)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("localtime");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        private String visitLocalTimestamp(LocalTimestamp node)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("localtimestamp");
            node.getPrecision().ifPresent(precision -> builder.append('(').append(precision).append(')'));

            return builder.toString();
        }

        private String visitBooleanLiteral(BooleanLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> String.valueOf(node.getValue()));
        }

        private String visitStringLiteral(StringLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> formatStringLiteral(node.getValue()));
        }

        private String visitBinaryLiteral(BinaryLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> "X'" + node.toHexString() + "'");
        }

        private String visitArray(Array node)
        {
            return node.getValues().stream()
                    .map(SqlFormatter::formatSql)
                    .collect(joining(",", "ARRAY[", "]"));
        }

        private String visitLongLiteral(LongLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(node::getValue);
        }

        private String visitDoubleLiteral(DoubleLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> DOUBLE_FORMATTER.get().format(node.getValue()));
        }

        private String visitDecimalLiteral(DecimalLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    // TODO return node value without "DECIMAL '..'" when FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
                    .orElseGet(() -> "DECIMAL '" + node.getValue() + "'");
        }

        private String visitGenericLiteral(GenericLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElseGet(() -> node.getType() + " " + formatStringLiteral(node.getValue()));
        }

        private String visitNullLiteral(NullLiteral node)
        {
            return literalFormatter
                    .map(formatter -> formatter.apply(node))
                    .orElse("null");
        }

        private String visitIntervalLiteral(IntervalLiteral node, Void context)
        {
            if (literalFormatter.isPresent()) {
                return literalFormatter.get().apply(node);
            }
            String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "-" : "";
            return "INTERVAL " +
                    sign +
                    "'" + node.getValue() + "' " +
                    process(node.qualifier(), context);
        }

        private String visitSimpleIntervalQualifier(SimpleIntervalQualifier node)
        {
            if (node.getField() instanceof IntervalField.Second(Optional<DataTypeParameter> fractionalPrecision) && fractionalPrecision.isPresent()) {
                checkArgument(node.getPrecision().isPresent(), "Leading precision is required when fractional precision is specified");
                return "SECOND(" + node.getPrecision().get() + ", " + fractionalPrecision.get() + ")";
            }

            return node.getField().name() + node.getPrecision().map(precision -> "(" + precision + ")").orElse("");
        }

        private String visitCompositeIntervalQualifier(CompositeIntervalQualifier node)
        {
            StringBuilder result = new StringBuilder();
            result.append(node.getFrom().name());
            if (node.getPrecision().isPresent()) {
                result.append("(").append(node.getPrecision().get()).append(")");
            }
            result.append(" TO ");
            result.append(node.getTo().name());
            if (node.getTo() instanceof IntervalField.Second(Optional<DataTypeParameter> fractionalPrecision) && fractionalPrecision.isPresent()) {
                result.append("(").append(fractionalPrecision.get()).append(")");
            }
            return result.toString();
        }

        private String visitIdentifier(Identifier node)
        {
            if (node.isDelimited() || reserved(node.getValue())) {
                return quoteIdentifier(node.getValue());
            }
            return node.getValue();
        }

        private String visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            String baseString = process(node.getBase(), context);
            return baseString + "." + node.getField().map(this::process).orElse("*");
        }

        private String visitFieldReference(FieldReference node)
        {
            // add colon so this won't parse
            return ":input(" + node.getFieldIndex() + ")";
        }

        private String visitFunctionCall(FunctionCall node, Void context)
        {
            if (QualifiedName.of("LISTAGG").equals(node.getName())) {
                return visitListagg(node);
            }

            StringBuilder builder = new StringBuilder();

            if (node.getProcessingMode().isPresent()) {
                builder.append(node.getProcessingMode().get().getMode())
                        .append(" ");
            }

            String arguments = node.getArguments().stream()
                    .map(argument -> process(argument, context))
                    .collect(joining(", "));
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

        private String visitCallArgument(CallArgument node, Void context)
        {
            StringBuilder builder = new StringBuilder();
            node.getName().ifPresent(name -> builder.append(formatExpression(name)).append(" => "));
            builder.append(process(node.getValue(), context));
            return builder.toString();
        }

        private String visitStaticMethodCall(StaticMethodCall node)
        {
            return formatName(node.getType()) + "::" + formatMethodName(node.getMethod()) + "(" + joinCallArguments(node.getArguments()) + ")";
        }

        private String visitMethodCall(MethodCall node)
        {
            return "(" + formatExpression(node.getReceiver()) + ")." + formatMethodName(node.getMethod()) + "(" + joinCallArguments(node.getArguments()) + ")";
        }

        private static String formatMethodName(Identifier name)
        {
            if (name.isDelimited()) {
                return quoteIdentifier(name.getValue());
            }
            return name.getValue();
        }

        private static String quoteIdentifier(String value)
        {
            return '"' + value.replace("\"", "\"\"") + '"';
        }

        private String visitLambdaExpression(LambdaExpression node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(');
            Joiner.on(", ").appendTo(builder, node.getArguments());
            builder.append(") -> ");
            builder.append(process(node.getBody(), context));
            return builder.toString();
        }

        private String visitLogicalExpression(LogicalExpression node, Void context)
        {
            return "(" +
                    node.getTerms().stream()
                            .map(term -> process(term, context))
                            .collect(joining(" " + node.getOperator().toString() + " ")) +
                    ")";
        }

        private String visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return (node.isNegated() ? "NOT BETWEEN " : "BETWEEN ") + node.getSymmetry().map(symmetry -> symmetry + " ").orElse("") + process(node.getMin(), context) + " AND " + process(node.getMax(), context);
        }

        private String visitDistinctFromPredicate(DistinctFromPredicate node, Void context)
        {
            return (node.isNegated() ? "IS NOT DISTINCT FROM " : "IS DISTINCT FROM ") + process(node.getRight(), context);
        }

        private String visitLikePredicate(LikePredicate node, Void context)
        {
            return (node.isNegated() ? "NOT LIKE " : "LIKE ") + process(node.getPattern(), context)
                    + node.getEscape().map(escape -> " ESCAPE " + process(escape, context)).orElse("");
        }

        private String visitMatchPredicate(MatchPredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder("MATCH");
            if (node.isUnique()) {
                builder.append(" UNIQUE");
            }
            builder.append(' ').append(node.getType());
            builder.append(' ').append(process(node.getSubquery(), context));
            return builder.toString();
        }

        private String visitQuantifiedComparisonPredicate(QuantifiedComparisonPredicate node, Void context)
        {
            return node.getOperator().getValue() + " " + node.getQuantifier() + " " + process(node.getSubquery(), context);
        }

        private String visitNullIfExpression(NullIfExpression node, Void context)
        {
            return "NULLIF(" + process(node.getFirst(), context) + ", " + process(node.getSecond(), context) + ')';
        }

        private String visitIfExpression(IfExpression node, Void context)
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

        private String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            String value = process(node.getValue(), context);

            return switch (node.getSign()) {
                // Unary is ambiguous with respect to negative numbers. "-1" parses as a number, but "-(1)" parses as "unaryMinus(number)"
                // When parentheses are not present, adding them is necessary to ensure the parsing round trips properly.
                case MINUS -> value.startsWith("(") ? "-" + value : "-(" + value + ")";
                case PLUS -> "+" + value;
            };
        }

        private String visitArithmeticBinary(ArithmeticBinaryExpression node)
        {
            return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
        }

        private String visitAllColumns(AllColumns node, Void context)
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

        private String visitIntervalValueExpression(IntervalValueExpression node, Void context)
        {
            return "((" + process(node.getLeft(), context) + " - " + process(node.getRight(), context) + ") " + process(node.getType().qualifier(), context) + ")";
        }

        private String visitCast(Cast node, Void context)
        {
            return (node.isSafe() ? "TRY_CAST" : "CAST") +
                    "(" + process(node.getExpression(), context) + " AS " + process(node.getType(), context) + ")";
        }

        private String visitSearchedCaseExpression(SearchedCaseExpression node, Void context)
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

        private String visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
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

        private String visitWhenClause(WhenClause node, Void context)
        {
            String match = switch (node.getMatch()) {
                case WhenClause.Operand operand -> process(operand.expression(), context);
                case WhenClause.Partial partial -> process(partial.predicate(), context);
            };
            return "WHEN " + match + " THEN " + process(node.getResult(), context);
        }

        private String visitFilter(Expression node, Void context)
        {
            return "(WHERE " + process(node, context) + ')';
        }

        private String visitRowDataType(RowDataType node)
        {
            return node.getFields().stream()
                    .map(this::process)
                    .collect(joining(", ", "ROW(", ")"));
        }

        private String visitRowDataTypeField(RowDataType.Field node, Void context)
        {
            StringBuilder result = new StringBuilder();

            if (node.getName().isPresent()) {
                result.append(process(node.getName().get(), context));
                result.append(" ");
            }

            result.append(process(node.getType(), context));

            return result.toString();
        }

        private String visitGenericDataType(GenericDataType node)
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

        private String visitDateTimeType(DateTimeDataType node)
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

        private String visitJsonExists(JsonExists node)
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

        private String visitJsonValue(JsonValue node)
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

        private String visitJsonQuery(JsonQuery node)
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

        private String visitJsonSerialize(JsonSerialize node)
        {
            StringBuilder builder = new StringBuilder();

            builder.append("JSON_SERIALIZE(")
                    .append(formatJsonExpression(node.getExpression(), Optional.of(node.getInputFormat())));

            if (node.getReturnedType().isPresent()) {
                builder.append(" RETURNING ")
                        .append(process(node.getReturnedType().get()))
                        .append(node.getOutputFormat().map(value -> " FORMAT " + value).orElse(""));
            }

            // Emit ON ERROR only when it differs from the default (ERROR), so unchanged
            // expressions round-trip to their original SQL.
            if (node.getErrorBehavior() != JsonSerialize.OnErrorBehavior.ERROR) {
                builder.append(" ").append(node.getErrorBehavior()).append(" ON ERROR");
            }

            builder.append(")");
            return builder.toString();
        }

        private String visitJsonObject(JsonObject node)
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

        private String visitJsonArray(JsonArray node)
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

        private String joinCallArguments(List<CallArgument> arguments)
        {
            return arguments.stream()
                    .map(argument -> process(argument, null))
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

            List<CallArgument> arguments = node.getArguments();
            Expression expression = arguments.get(0).getValue();
            Expression separator = arguments.get(1).getValue();
            BooleanLiteral overflowError = (BooleanLiteral) arguments.get(2).getValue();
            Expression overflowFiller = arguments.get(3).getValue();
            BooleanLiteral showOverflowEntryCount = (BooleanLiteral) arguments.get(4).getValue();

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
        return switch (window) {
            case WindowReference windowReference -> formatExpression(windowReference.getName());
            case WindowSpecification windowSpecification -> formatWindowSpecification(windowSpecification);
        };
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

        switch (windowFrame.getExclusion()) {
            case CURRENT_ROW -> builder.append(" EXCLUDE CURRENT ROW");
            case GROUP -> builder.append(" EXCLUDE GROUP");
            case TIES -> builder.append(" EXCLUDE TIES");
            case NO_OTHERS -> {} // the default; an absent clause parses back to NO_OTHERS, so leave it unprinted
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
        return groupingElements.stream()
                .map(groupingElement -> {
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
                    else if (groupingElement instanceof AutoGroupBy) {
                        result = "AUTO";
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
