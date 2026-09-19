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
package io.trino.plugin.trino;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Locale;
import java.util.Set;

import static io.trino.plugin.trino.TrinoRemoteCapabilities.CharToVarcharCastSemantics.TRIMS_TRAILING_SPACES;
import static java.util.Objects.requireNonNull;

final class TrinoCompatibilityRegistry
{
    private static final Set<FunctionName> STANDARD_OPERATORS = ImmutableSet.of(
            StandardFunctions.AND_FUNCTION_NAME,
            StandardFunctions.OR_FUNCTION_NAME,
            StandardFunctions.NOT_FUNCTION_NAME,
            StandardFunctions.IS_NULL_FUNCTION_NAME,
            StandardFunctions.NULLIF_FUNCTION_NAME,
            StandardFunctions.CAST_FUNCTION_NAME,
            StandardFunctions.TRY_CAST_FUNCTION_NAME,
            StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME,
            StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.IDENTICAL_OPERATOR_FUNCTION_NAME,
            StandardFunctions.ADD_FUNCTION_NAME,
            StandardFunctions.SUBTRACT_FUNCTION_NAME,
            StandardFunctions.MULTIPLY_FUNCTION_NAME,
            StandardFunctions.DIVIDE_FUNCTION_NAME,
            StandardFunctions.MODULO_FUNCTION_NAME,
            StandardFunctions.NEGATE_FUNCTION_NAME,
            StandardFunctions.LIKE_FUNCTION_NAME,
            StandardFunctions.IN_PREDICATE_FUNCTION_NAME,
            StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME);

    private static final Set<String> FUNCTION_ALLOWLIST = ImmutableSet.of(
            "abs",
            "array_distinct",
            "array_join",
            "array_position",
            "at_timezone",
            "cardinality",
            "ceil",
            "ceiling",
            "concat",
            "contains",
            "date",
            "date_add",
            "date_diff",
            "date_trunc",
            "day",
            "day_of_month",
            "day_of_week",
            "day_of_year",
            "element_at",
            "floor",
            "from_iso8601_date",
            "from_iso8601_timestamp",
            "from_unixtime",
            "greatest",
            "hour",
            "json_array_contains",
            "json_extract",
            "json_extract_scalar",
            "json_format",
            "json_parse",
            "least",
            "length",
            "lower",
            "lpad",
            "minute",
            "month",
            "regexp_extract",
            "regexp_extract_all",
            "regexp_like",
            "regexp_replace",
            "replace",
            "round",
            "rpad",
            "second",
            "split",
            "split_part",
            "strpos",
            "substr",
            "substring",
            "timezone_hour",
            "timezone_minute",
            "to_iso8601",
            "to_unixtime",
            "trim",
            "upper",
            "week",
            "with_timezone",
            "year");

    private static final Set<String> SESSION_SENSITIVE_DENYLIST = ImmutableSet.of(
            "current_date",
            "current_time",
            "current_timestamp",
            "current_timezone",
            "localtime",
            "localtimestamp",
            "now");

    private static final Set<String> AGGREGATION_ALLOWLIST = ImmutableSet.of(
            "avg",
            "checksum",
            "count",
            "count_if",
            "max",
            "min",
            "sum");

    boolean isFunctionSupported(ConnectorSession session, Call call, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(session, "session is null");
        requireNonNull(call, "call is null");
        requireNonNull(capabilities, "capabilities is null");

        FunctionName functionName = call.getFunctionName();
        if (functionName.equals(StandardFunctions.CAST_FUNCTION_NAME) || functionName.equals(StandardFunctions.TRY_CAST_FUNCTION_NAME)) {
            CastCompatibility compatibility = castCompatibility(call);
            CastSessionDependency dependency = compatibility.sessionDependency();
            if (dependency == CastSessionDependency.UNSUPPORTED ||
                    dependency == CastSessionDependency.SESSION_START ||
                    (dependency == CastSessionDependency.TIME_ZONE && !capabilities.hasSameTimeZone(session))) {
                return false;
            }
            if (compatibility.charToVarcharCastLocation() != CharToVarcharCastLocation.NONE &&
                    capabilities.charToVarcharCastSemantics().isEmpty()) {
                return false;
            }
            if (compatibility.charToVarcharCastLocation() == CharToVarcharCastLocation.NESTED &&
                    capabilities.charToVarcharCastSemantics().orElseThrow() != TRIMS_TRAILING_SPACES) {
                return false;
            }
        }
        if (STANDARD_OPERATORS.contains(functionName)) {
            return true;
        }
        if (functionName.getCatalogSchema().isPresent()) {
            return false;
        }

        String name = canonicalName(functionName);
        if (SESSION_SENSITIVE_DENYLIST.contains(name)) {
            return false;
        }
        if (usesSessionTimeZone(name, call) && !capabilities.hasSameTimeZone(session)) {
            return false;
        }
        if (isSubscriptFunction(name)) {
            return true;
        }
        return FUNCTION_ALLOWLIST.contains(name) && capabilities.hasFunction(name);
    }

    boolean isAggregationSupported(AggregateFunction aggregate, TrinoRemoteCapabilities capabilities)
    {
        requireNonNull(aggregate, "aggregate is null");
        requireNonNull(capabilities, "capabilities is null");
        String name = aggregate.getFunctionName().toLowerCase(Locale.ENGLISH);
        return AGGREGATION_ALLOWLIST.contains(name) && capabilities.hasFunction(name);
    }

    static String canonicalName(FunctionName functionName)
    {
        return functionName.getName().toLowerCase(Locale.ENGLISH);
    }

    static boolean isSubscriptFunction(String name)
    {
        return name.equals("subscript") || name.equals("$operator$subscript");
    }

    private static CastCompatibility castCompatibility(Call call)
    {
        if (call.getArguments().size() != 1) {
            return new CastCompatibility(CastSessionDependency.SESSION_START, CharToVarcharCastLocation.NONE);
        }
        ConnectorExpression source = call.getArguments().getFirst();
        return castCompatibility(source.getType(), call.getType());
    }

    private static CastCompatibility castCompatibility(Type sourceType, Type targetType)
    {
        if (sourceType instanceof ArrayType sourceArray && targetType instanceof ArrayType targetArray) {
            return castCompatibility(sourceArray.getElementType(), targetArray.getElementType()).nested();
        }
        if (sourceType instanceof MapType sourceMap && targetType instanceof MapType targetMap) {
            return CastCompatibility.combine(
                            castCompatibility(sourceMap.getKeyType(), targetMap.getKeyType()),
                            castCompatibility(sourceMap.getValueType(), targetMap.getValueType()))
                    .nested();
        }
        if (sourceType instanceof RowType sourceRow && targetType instanceof RowType targetRow) {
            if (sourceRow.getFields().size() != targetRow.getFields().size()) {
                return new CastCompatibility(CastSessionDependency.UNSUPPORTED, CharToVarcharCastLocation.NONE);
            }
            CastCompatibility compatibility = new CastCompatibility(CastSessionDependency.NONE, CharToVarcharCastLocation.NONE);
            for (int index = 0; index < sourceRow.getFields().size(); index++) {
                compatibility = CastCompatibility.combine(
                        compatibility,
                        castCompatibility(
                                sourceRow.getFields().get(index).getType(),
                                targetRow.getFields().get(index).getType()));
            }
            return compatibility.nested();
        }

        CharToVarcharCastLocation charToVarcharCastLocation =
                sourceType instanceof CharType && targetType instanceof VarcharType
                        ? CharToVarcharCastLocation.DIRECT
                        : CharToVarcharCastLocation.NONE;

        // These casts use the session start instant or its local date. Local and remote
        // queries do not share a start instant, so matching time zones are insufficient.
        if ((sourceType instanceof TimeType &&
                (targetType instanceof TimeWithTimeZoneType || targetType instanceof TimestampType || targetType instanceof TimestampWithTimeZoneType)) ||
                (sourceType instanceof TimeWithTimeZoneType &&
                        (targetType instanceof TimestampType || targetType instanceof TimestampWithTimeZoneType)) ||
                (sourceType instanceof TimestampType && targetType instanceof TimeWithTimeZoneType) ||
                ((sourceType instanceof CharType || sourceType instanceof VarcharType) && targetType instanceof TimeWithTimeZoneType)) {
            return new CastCompatibility(CastSessionDependency.SESSION_START, charToVarcharCastLocation);
        }

        // Casts that attach a time zone are safe only when the connector and remote
        // sessions use the same zone.
        if (((sourceType instanceof DateType || sourceType instanceof TimestampType || sourceType instanceof CharType || sourceType instanceof VarcharType) &&
                targetType instanceof TimestampWithTimeZoneType)) {
            return new CastCompatibility(CastSessionDependency.TIME_ZONE, charToVarcharCastLocation);
        }

        // Fail closed for temporal-to-temporal casts. Every valid pair is either
        // classified above or explicitly known to be independent of session state.
        if (isTemporalType(sourceType) && isTemporalType(targetType) && !isSessionIndependentTemporalCast(sourceType, targetType)) {
            return new CastCompatibility(CastSessionDependency.UNSUPPORTED, charToVarcharCastLocation);
        }
        return new CastCompatibility(CastSessionDependency.NONE, charToVarcharCastLocation);
    }

    private static boolean isTemporalType(Type type)
    {
        return type instanceof DateType ||
                type instanceof TimeType ||
                type instanceof TimeWithTimeZoneType ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType;
    }

    private static boolean isSessionIndependentTemporalCast(Type sourceType, Type targetType)
    {
        return (sourceType instanceof DateType && (targetType instanceof DateType || targetType instanceof TimestampType)) ||
                (sourceType instanceof TimeType && targetType instanceof TimeType) ||
                (sourceType instanceof TimeWithTimeZoneType &&
                        (targetType instanceof TimeType || targetType instanceof TimeWithTimeZoneType)) ||
                (sourceType instanceof TimestampType &&
                        (targetType instanceof DateType || targetType instanceof TimeType || targetType instanceof TimestampType)) ||
                (sourceType instanceof TimestampWithTimeZoneType &&
                        (targetType instanceof DateType || targetType instanceof TimeType || targetType instanceof TimeWithTimeZoneType ||
                                targetType instanceof TimestampType || targetType instanceof TimestampWithTimeZoneType));
    }

    private enum CastSessionDependency
    {
        NONE,
        TIME_ZONE,
        SESSION_START,
        UNSUPPORTED;

        private static CastSessionDependency combine(CastSessionDependency first, CastSessionDependency second)
        {
            return first.ordinal() >= second.ordinal() ? first : second;
        }
    }

    private enum CharToVarcharCastLocation
    {
        NONE,
        DIRECT,
        NESTED;

        private static CharToVarcharCastLocation combine(CharToVarcharCastLocation first, CharToVarcharCastLocation second)
        {
            if (first == NESTED || second == NESTED) {
                return NESTED;
            }
            if (first == DIRECT || second == DIRECT) {
                return DIRECT;
            }
            return NONE;
        }
    }

    private record CastCompatibility(
            CastSessionDependency sessionDependency,
            CharToVarcharCastLocation charToVarcharCastLocation)
    {
        private static CastCompatibility combine(CastCompatibility first, CastCompatibility second)
        {
            return new CastCompatibility(
                    CastSessionDependency.combine(first.sessionDependency(), second.sessionDependency()),
                    CharToVarcharCastLocation.combine(first.charToVarcharCastLocation(), second.charToVarcharCastLocation()));
        }

        private CastCompatibility nested()
        {
            if (charToVarcharCastLocation == CharToVarcharCastLocation.NONE) {
                return this;
            }
            return new CastCompatibility(sessionDependency, CharToVarcharCastLocation.NESTED);
        }
    }

    private static boolean usesSessionTimeZone(String name, Call call)
    {
        return (name.equals("from_unixtime") && call.getArguments().size() == 1) ||
                name.equals("from_iso8601_timestamp");
    }
}
