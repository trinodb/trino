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
package io.trino.spi.type;

import java.util.List;
import java.util.Optional;

import static java.lang.String.join;
import static java.util.stream.Collectors.toUnmodifiableList;

/// Renders a [TypeDescriptor] in user-visible SQL surface syntax.
///
/// This is the presentation layer that sits *on top* of the internal representation: a
/// [TypeDescriptor]/[TypeTemplate] renders itself as the standardized `base(arg, …)` IR form
/// ([TypeDescriptor#toString]), which is used only internally (type identity, function-id, debug) and
/// never shown to users. This class is the one place that knows the SQL spellings that depart from
/// that generic form — the elided length of an unbounded `varchar`, and the time-zone word order of
/// `timestamp`/`time` — and produces the form a user sees.
public final class TypeSyntax
{
    private TypeSyntax() {}

    /// The user-visible SQL spelling of a ground type.
    public static String toSql(TypeDescriptor descriptor)
    {
        List<String> parameters = descriptor.getParameters().stream()
                .map(TypeSyntax::parameterToSql)
                .collect(toUnmodifiableList());
        return render(descriptor.getBase(), parameters);
    }

    /// The user-visible SQL spelling of an open (variable-bearing) type, e.g. for rendering a function
    /// signature in a diagnostic.
    public static String toSql(TypeTemplate template)
    {
        return switch (template) {
            case TypeTemplate.TypeVariable(String name) -> name;
            case TypeTemplate.TypeApplication(String base, List<TemplateParameter> parameters) -> render(base, parameters.stream().map(TypeSyntax::parameterToSql).collect(toUnmodifiableList()));
        };
    }

    /// The user-visible SQL spelling of a single ground parameter — a nested type (optionally carrying a
    /// field name) or a numeric argument.
    public static String toSql(TypeParameter parameter)
    {
        return parameterToSql(parameter);
    }

    private static String parameterToSql(TypeParameter parameter)
    {
        return switch (parameter) {
            case TypeParameter.Type(Optional<String> name, TypeDescriptor type) -> name.map(fieldName -> "\"" + fieldName.replace("\"", "\"\"") + "\" ").orElse("") + toSql(type);
            case TypeParameter.Numeric(long value) -> Long.toString(value);
        };
    }

    private static String parameterToSql(TemplateParameter parameter)
    {
        return switch (parameter) {
            case TemplateParameter.TypeArgument(Optional<String> name, TypeTemplate type) -> name.map(fieldName -> "\"" + fieldName.replace("\"", "\"\"") + "\" ").orElse("") + toSql(type);
            case TemplateParameter.NumericArgument(NumericExpression value) -> NumericExpressions.render(value);
        };
    }

    private static String render(String base, List<String> parameters)
    {
        // The time-zone datetime types carry an opaque internal base ($timestamp_tz/$time_tz); their SQL
        // surface spells the time-zone phrase as a trailing clause around the optional precision. These
        // run before the empty-parameter short-circuit so the bare, unparameterized form (as in a
        // function signature) still renders the SQL name rather than the internal token.
        if (base.equalsIgnoreCase(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)) {
            return timeZoneSpelling("timestamp", "with time zone", parameters);
        }
        if (base.equalsIgnoreCase(StandardTypes.TIME_WITH_TIME_ZONE)) {
            return timeZoneSpelling("time", "with time zone", parameters);
        }
        if (parameters.isEmpty()) {
            // The interval types likewise carry an opaque internal base ($interval_day_time /
            // $interval_year_month); their bare form spells the SQL qualifier.
            if (base.equalsIgnoreCase(StandardTypes.INTERVAL_DAY_TO_SECOND)) {
                return "interval day to second";
            }
            if (base.equalsIgnoreCase(StandardTypes.INTERVAL_YEAR_TO_MONTH)) {
                return "interval year to month";
            }
            return base;
        }
        // An unbounded varchar renders without its sentinel length, as `varchar`.
        if (base.equalsIgnoreCase(StandardTypes.VARCHAR)
                && parameters.size() == 1
                && parameters.get(0).equals(Long.toString(VarcharType.UNBOUNDED_LENGTH))) {
            return base;
        }
        // An interval's fields are spelled as an SQL qualifier (`day to second`), not generic parameters.
        // In a function signature the fields may instead be numeric variables — a signature generic over
        // every interval qualifier — which has no SQL field form; spell the canonical full qualifier, the
        // same form the bare, unparameterized interval takes above.
        if (base.equalsIgnoreCase(StandardTypes.INTERVAL_DAY_TO_SECOND)) {
            return isFieldCode(parameters.get(0)) && isFieldCode(parameters.get(1)) ? renderInterval(parameters) : "interval day to second";
        }
        if (base.equalsIgnoreCase(StandardTypes.INTERVAL_YEAR_TO_MONTH)) {
            return isFieldCode(parameters.get(0)) && isFieldCode(parameters.get(1)) ? renderInterval(parameters) : "interval year to month";
        }
        return base + "(" + join(",", parameters) + ")";
    }

    private static String timeZoneSpelling(String kind, String zone, List<String> parameters)
    {
        return parameters.isEmpty()
                ? kind + " " + zone
                : kind + "(" + parameters.get(0) + ") " + zone;
    }

    /// Renders an interval qualifier. The first two parameters are the start and end [IntervalField]
    /// codes (always literal, so the keyword comes from the single [IntervalField] table); the third,
    /// when present, is the leading-field precision and the fourth, for a `SECOND` trailing field, the
    /// fractional-seconds precision. Precisions render verbatim — they may be a literal or, in a
    /// [TypeTemplate], a variable name.
    private static String renderInterval(List<String> parameters)
    {
        String start = IntervalField.fromCode(Integer.parseInt(parameters.get(0))).keyword();
        String end = IntervalField.fromCode(Integer.parseInt(parameters.get(1))).keyword();
        // The leading precision is omitted when unspecified (rendered as the zero sentinel); a declared
        // precision is at least 1.
        boolean hasLeading = parameters.size() > 2 && !parameters.get(2).equals("0");
        String leading = hasLeading ? "interval " + start + "(" + parameters.get(2) + ")" : "interval " + start;
        boolean secondEnd = end.equals("second") && parameters.size() > 3;
        if (start.equals(end)) {
            return secondEnd ? "interval second(" + parameters.get(2) + ", " + parameters.get(3) + ")" : leading;
        }
        return secondEnd ? leading + " to second(" + parameters.get(3) + ")" : leading + " to " + end;
    }

    /// Whether a rendered parameter is a concrete interval field code (digits) rather than a numeric
    /// variable in a function signature.
    private static boolean isFieldCode(String parameter)
    {
        return !parameter.isEmpty() && parameter.chars().allMatch(Character::isDigit);
    }
}
