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
package io.trino.plugin.pinot.query;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Identifier;
import org.apache.pinot.common.request.Literal;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.pinot.common.request.ExpressionType.FUNCTION;
import static org.apache.pinot.common.request.ExpressionType.LITERAL;

public class FilterToPinotSqlConverter
{
    private static final Map<String, String> BINARY_OPERATORS = ImmutableMap.<String, String>builder()
            .put("equals", "=")
            .put("not_equals", "!=")
            .put("greater_than", ">")
            .put("less_than", "<")
            .put("greater_than_or_equal", ">=")
            .put("less_than_or_equal", "<=")
            .put("plus", "+")
            .put("minus", "-")
            .put("times", "*")
            .put("divide", "/")
            .build();

    // Pinot does not recognize double literals with scientific notation
    private static final ThreadLocal<DecimalFormat> doubleFormatter = ThreadLocal.withInitial(
            () -> { DecimalFormat decimalFormat = new DecimalFormat("0", new DecimalFormatSymbols(Locale.US));
                decimalFormat.setMaximumFractionDigits(340);
                return decimalFormat; });

    private final Map<String, ColumnHandle> columnHandles;

    private FilterToPinotSqlConverter(Map<String, ColumnHandle> columnHandles)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    }

    public static String convertFilter(org.apache.pinot.common.request.PinotQuery pinotQuery, Map<String, ColumnHandle> columnHandles)
    {
        return new FilterToPinotSqlConverter(columnHandles).formatExpression(pinotQuery.getFilterExpression());
    }

    private String formatExpression(Expression expression)
    {
        switch (expression.getType()) {
            case FUNCTION:
                return formatFunction(expression.getFunctionCall());
            case LITERAL:
                return formatLiteral(expression.getLiteral());
            case IDENTIFIER:
                return formatIdentifier(expression.getIdentifier());
            default:
                throw new UnsupportedOperationException(format("Unknown type: '%s'", expression.getType()));
        }
    }

    private String formatFunction(Function functionCall)
    {
        String binaryOperator = BINARY_OPERATORS.get(functionCall.getOperator().toLowerCase(ENGLISH));
        if (binaryOperator != null) {
            return formatEqualsMinusZero(functionCall).orElse(format("(%s) %s (%s)",
                    formatExpression(functionCall.getOperands().get(0)),
                    binaryOperator,
                    formatExpression(functionCall.getOperands().get(1))));
        }
        else if (functionCall.getOperator().equalsIgnoreCase("cast")) {
            checkState(functionCall.getOperands().size() == 2, "Unexpected size for cast operator");
            return format("CAST(%s AS %s)", formatExpression(functionCall.getOperands().get(0)), functionCall.getOperands().get(1).getLiteral().getStringValue());
        }
        else if (functionCall.getOperator().equalsIgnoreCase("in") || functionCall.getOperator().equalsIgnoreCase("not_in")) {
            return formatInClause(functionCall);
        }
        else if (functionCall.getOperator().equalsIgnoreCase("case")) {
            return formatCaseStatement(functionCall);
        }
        return functionCall.getOperator() + "(" + functionCall.getOperands().stream().map(this::formatExpression).collect(joining(", ")) + ")";
    }

    // Pinot parses "a = b" as "a - b = 0" which can result in invalid sql
    private Optional<String> formatEqualsMinusZero(Function functionCall)
    {
        if (!functionCall.getOperator().equalsIgnoreCase("equals")) {
            return Optional.empty();
        }

        Expression left = functionCall.getOperands().get(0);
        if (left.getType() != FUNCTION || !left.getFunctionCall().getOperator().equalsIgnoreCase("minus")) {
            return Optional.empty();
        }

        Expression right = functionCall.getOperands().get(1);
        if (right.getType() != LITERAL || !formatLiteral(right.getLiteral()).equals("0")) {
            return Optional.empty();
        }
        Function minus = left.getFunctionCall();
        return Optional.of(format("(%s) = (%s)", formatExpression(minus.getOperands().get(0)), formatExpression(minus.getOperands().get(1))));
    }

    private String formatInClause(Function functionCall)
    {
        checkState(functionCall.getOperator().equalsIgnoreCase("in") ||
                functionCall.getOperator().equalsIgnoreCase("not_in"),
                "Unexpected operator '%s'", functionCall.getOperator());
        checkState(functionCall.getOperands().size() > 1, "Unexpected expression");
        String operator;
        if (functionCall.getOperator().equalsIgnoreCase("in")) {
            operator = "IN";
        }
        else {
            operator = "NOT IN";
        }
        return format("%s %s (%s)", formatExpression(functionCall.getOperands().get(0)),
                operator,
                functionCall.getOperands().subList(1, functionCall.getOperands().size()).stream()
                        .map(this::formatExpression)
                        .collect(joining(", ")));
    }

    private String formatCaseStatement(Function functionCall)
    {
        checkState(functionCall.getOperator().equalsIgnoreCase("case"), "Unexpected operator '%s'", functionCall.getOperator());
        checkState(functionCall.getOperands().size() >= 2, "Unexpected expression");
        int whenStatements = functionCall.getOperands().size() / 2;
        StringBuilder builder = new StringBuilder("CASE ");

        builder.append("WHEN ")
                .append(formatExpression(functionCall.getOperands().get(0)))
                .append(" THEN ")
                .append(formatExpression(functionCall.getOperands().get(whenStatements)));

        for (int index = 1; index < whenStatements; index++) {
            builder.append(" ")
                    .append("WHEN ")
                    .append(formatExpression(functionCall.getOperands().get(index)))
                    .append(" THEN ")
                    .append(formatExpression(functionCall.getOperands().get(index + whenStatements)));
        }

        if (functionCall.getOperands().size() % 2 != 0) {
            builder.append(" ELSE " + formatExpression(functionCall.getOperands().get(functionCall.getOperands().size() - 1)));
        }
        return builder.append(" END").toString();
    }

    private String formatLiteral(Literal literal)
    {
        if (!literal.isSet()) {
            return "null";
        }
        switch (literal.getSetField()) {
            case LONG_VALUE:
                return String.valueOf(literal.getLongValue());
            case INT_VALUE:
                return String.valueOf(literal.getIntValue());
            case BOOL_VALUE:
                return String.valueOf(literal.getBoolValue());
            case STRING_VALUE:
                return format("'%s'", literal.getStringValue().replaceAll("'", "''"));
            case BYTE_VALUE:
                return String.valueOf(literal.getByteValue());
            case BINARY_VALUE:
                return Hex.encodeHexString(literal.getBinaryValue());
            case DOUBLE_VALUE:
                return doubleFormatter.get().format(literal.getDoubleValue());
            case SHORT_VALUE:
                return String.valueOf(literal.getShortValue());
            default:
                throw new UnsupportedOperationException(format("Unknown literal type: '%s'", literal.getSetField()));
        }
    }

    private String formatIdentifier(Identifier identifier)
    {
        PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) requireNonNull(columnHandles.get(identifier.getName()), "Column not found");
        checkArgument(!pinotColumnHandle.getColumnName().contains("\""), "Column name contains double quotes: '%s'", pinotColumnHandle.getColumnName());
        return format("\"%s\"", pinotColumnHandle.getColumnName());
    }
}
