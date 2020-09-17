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
package io.prestosql.pinot.query;

import com.google.common.collect.ImmutableMap;
import io.prestosql.pinot.PinotColumnHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.pinot.common.request.FilterOperator;
import org.apache.pinot.common.request.FilterQuery;
import org.apache.pinot.common.request.FilterQueryMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class FilterToPqlConverter
{
    private final FilterQueryMap filterQueryMap;
    private final Map<String, ColumnHandle> columnHandles;
    private final SchemaTableName schemaTableName;

    private static final char LOWER_OPEN = '(';
    private static final char LOWER_CLOSED = '[';
    private static final char UPPER_OPEN = ')';
    private static final char UPPER_CLOSED = ']';
    private static final String WILDCARD = "*";
    private static final String RANGE_DELIMITER = "\t\t";
    private static final String LT = " < ";
    private static final String GT = " > ";
    private static final String LE = " <= ";
    private static final String GE = " >= ";
    private static final String BETWEEN = " between ";
    private static final String AND = " and ";
    private static final String OR = " or ";

    public FilterToPqlConverter(SchemaTableName schemaTableName, FilterQueryMap filterQueryMap, Map<String, ColumnHandle> columnHandles)
    {
        this.filterQueryMap = requireNonNull(filterQueryMap, "filterQueryMap is null");
        this.columnHandles = ImmutableMap.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
    }

    public String convert(FilterQuery filterQuery)
    {
        switch (filterQuery.getOperator()) {
            case IN:
                return convertIn(filterQuery);
            case NOT_IN:
                return convertNotIn(filterQuery);
            case EQUALITY:
                return convertEquality(filterQuery);
            case NOT:
                return convertInequality(filterQuery);
            case REGEXP_LIKE:
                return convertRegexpLike(filterQuery);
            case RANGE:
                return convertRange(filterQuery);
            case AND:
                return convertAnd(filterQuery);
            case OR:
                return convertOr(filterQuery);
            default:
                throw new IllegalArgumentException("Invalid filter operator");
        }
    }

    private String convertOr(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.OR);
        checkState(filterQuery.getNestedFilterQueryIdsSize() >= 2, "Malformed 'OR' expression '%s'", filterQuery);
        return filterQuery.getNestedFilterQueryIds().stream()
                .map(filterQueryId -> filterQueryMap.getFilterQueryMap().get(filterQueryId))
                .map(filter -> encloseInParentheses(convert(filter)))
                .collect(joining(" OR "));
    }

    private String convertAnd(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.AND);
        checkState(filterQuery.getNestedFilterQueryIdsSize() >= 2, "Malformed 'AND' expression '%s'", filterQuery);
        return filterQuery.getNestedFilterQueryIds().stream()
                .map(filterQueryId -> filterQueryMap.getFilterQueryMap().get(filterQueryId))
                .map(filter -> encloseInParentheses(convert(filter)))
                .collect(joining(" AND "));
    }

    private String convertRange(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.RANGE);
        String range = getOnlyElement(filterQuery.getValue());
        String[] bounds = range.split(RANGE_DELIMITER);
        checkState(bounds.length >= 2, "invalid range '%s'", range);
        char lowerBound = bounds[0].charAt(0);
        checkLowerBoundIsValid(lowerBound);
        String lowerValue = bounds[0].substring(1);
        char upperBound = bounds[bounds.length - 1].charAt(bounds[bounds.length - 1].length() - 1);
        checkUpperBoundIsValid(upperBound);
        String upperValue = bounds[bounds.length - 1].substring(0, bounds[bounds.length - 1].length() - 1);
        if (lowerBound == LOWER_OPEN) {
            if (lowerValue.equals(WILDCARD)) {
                checkState(!upperValue.equals(WILDCARD), "Malformed range expression '%s'", range);
                String operator;
                if (upperBound == UPPER_OPEN) {
                    operator = LT;
                }
                else {
                    operator = LE;
                }
                return new StringBuilder().append(getPinotColumnName(filterQuery.getColumn()))
                        .append(operator)
                        .append(upperValue)
                        .toString();
            }
            checkState(upperValue.equals(WILDCARD) && upperBound == UPPER_OPEN, "Malformed range expression '%s'", range);
            return new StringBuilder().append(getPinotColumnName(filterQuery.getColumn()))
                    .append(GT)
                    .append(lowerValue)
                    .toString();
        }
        else {
            checkState(!lowerValue.equals(WILDCARD), "Malformed range expression '%s'", range);
            if (upperBound == UPPER_OPEN) {
                checkState(upperValue.equals(WILDCARD), "Malformed range expression '%s'", range);
                return new StringBuilder().append(getPinotColumnName(filterQuery.getColumn()))
                        .append(GE)
                        .append(lowerValue)
                        .toString();
            }
            else {
                checkState(!upperValue.equals(WILDCARD), "Malformed range expression '%s'", range);
                return new StringBuilder().append(getPinotColumnName(filterQuery.getColumn()))
                        .append(BETWEEN)
                        .append(lowerValue)
                        .append(AND)
                        .append(upperValue)
                        .toString();
            }
        }
    }

    private String convertIn(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.IN);
        return new StringBuilder()
                .append(getPinotColumnName(filterQuery.getColumn()))
                .append(" IN ")
                .append(getValuesList(filterQuery.getValue()))
                .toString();
    }

    private String convertNotIn(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.NOT_IN);
        return new StringBuilder()
                .append(getPinotColumnName(filterQuery.getColumn()))
                .append(" NOT IN ")
                .append(getValuesList(filterQuery.getValue()))
                .toString();
    }

    private String convertEquality(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.EQUALITY);
        return new StringBuilder()
                .append(getPinotColumnName(filterQuery.getColumn()))
                .append(" = ")
                .append(toSingleQuotedValue(getOnlyElement(filterQuery.getValue())))
                .toString();
    }

    private String convertInequality(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.NOT);
        return new StringBuilder()
                .append(getPinotColumnName(filterQuery.getColumn()))
                .append(" != ")
                .append(toSingleQuotedValue(getOnlyElement(filterQuery.getValue())))
                .toString();
    }

    private String convertRegexpLike(FilterQuery filterQuery)
    {
        checkOperator(filterQuery, FilterOperator.REGEXP_LIKE);
        return new StringBuilder()
                .append("regexp_like(")
                .append(getPinotColumnName(filterQuery.getColumn()))
                .append(", ")
                .append(toSingleQuotedValue(getOnlyElement(filterQuery.getValue())))
                .append(")")
                .toString();
    }

    private static void checkOperator(FilterQuery filterQuery, FilterOperator filterOperator)
    {
        checkState(filterQuery.getOperator() == filterOperator, "unexpected operator '%s' expected '%s'", filterQuery.getOperator(), filterOperator);
    }

    private String getPinotColumnName(String columnName)
    {
        PinotColumnHandle columnHandle = ((PinotColumnHandle) columnHandles.get(columnName));
        if (columnHandle != null) {
            return columnHandle.getColumnName();
        }
        return columnName;
    }

    private String getValuesList(List<String> values)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("(")
                .append(values.stream()
                        .map(this::toSingleQuotedValue)
                        .collect(joining(", ")))
                .append(")");
        return builder.toString();
    }

    private String toSingleQuotedValue(String value)
    {
        return format("'%s'", value);
    }

    public static String encloseInParentheses(String value)
    {
        return format("(%s)", value);
    }

    private static void checkLowerBoundIsValid(char lowerBoundChar)
    {
        checkState(lowerBoundChar == LOWER_OPEN || lowerBoundChar == LOWER_CLOSED, "invalid lower bound character '%s'", lowerBoundChar);
    }

    private static void checkUpperBoundIsValid(char upperBoundChar)
    {
        checkState(upperBoundChar == UPPER_OPEN || upperBoundChar == UPPER_CLOSED, "invalid upper bound character '%s'", upperBoundChar);
    }
}
