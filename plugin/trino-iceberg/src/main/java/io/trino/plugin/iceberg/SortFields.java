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
package io.trino.plugin.iceberg;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Expressions;

import java.util.List;
import java.util.function.Consumer;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergUtil.FUNCTION_ARGUMENT_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.FUNCTION_ARGUMENT_NAME_AND_INT;
import static io.trino.plugin.iceberg.IcebergUtil.IDENTIFIER;
import static io.trino.plugin.iceberg.IcebergUtil.fromIdentifier;
import static io.trino.plugin.iceberg.IcebergUtil.toIdentifier;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public final class SortFields
{
    // YEAR patterns
    private static final Pattern YEAR_PATTERN = Pattern.compile("\\s*?(?i:year)" + FUNCTION_ARGUMENT_NAME);
    private static final Pattern YEAR_PATTERN_ASC = Pattern.compile("\\s*?(?i:year)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?");
    private static final Pattern YEAR_PATTERN_ASC_NULLS_FIRST = Pattern.compile("\\s*?(?i:year)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern YEAR_PATTERN_ASC_NULLS_LAST = Pattern.compile("\\s*?(?i:year)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?LAST)\\s*?");
    private static final Pattern YEAR_PATTERN_DESC = Pattern.compile("\\s*?year" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?");
    private static final Pattern YEAR_PATTERN_DESC_NULLS_FIRST = Pattern.compile("\\s*?(?i:year)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern YEAR_PATTERN_DESC_NULLS_LAST = Pattern.compile("\\s*?(?i:year)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?LAST)\\s*?");

    // MONTH patterns
    private static final Pattern MONTH_PATTERN = Pattern.compile("\\s*?(?i:month)" + FUNCTION_ARGUMENT_NAME);
    private static final Pattern MONTH_PATTERN_ASC = Pattern.compile("\\s*?(?i:month)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?");
    private static final Pattern MONTH_PATTERN_ASC_NULLS_FIRST = Pattern.compile("\\s*?(?i:(?i:month))" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern MONTH_PATTERN_ASC_NULLS_LAST = Pattern.compile("\\s*?(?i:month)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?LAST)\\s*?");
    private static final Pattern MONTH_PATTERN_DESC = Pattern.compile("\\s*?(?i:month)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?");
    private static final Pattern MONTH_PATTERN_DESC_NULLS_FIRST = Pattern.compile("\\s*?(?i:month)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern MONTH_PATTERN_DESC_NULLS_LAST = Pattern.compile("\\s*?(?i:month)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?LAST)\\s*?");

    // DAY patterns
    private static final Pattern DAY_PATTERN = Pattern.compile("\\s*?(?i:day)" + FUNCTION_ARGUMENT_NAME);
    private static final Pattern DAY_PATTERN_ASC = Pattern.compile("\\s*?(?i:day)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?");
    private static final Pattern DAY_PATTERN_ASC_NULLS_FIRST = Pattern.compile("\\s*?(?i:day)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern DAY_PATTERN_ASC_NULLS_LAST = Pattern.compile("\\s*?(?i:day)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?LAST)\\s*?");
    private static final Pattern DAY_PATTERN_DESC = Pattern.compile("\\s*?(?i:day)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?");
    private static final Pattern DAY_PATTERN_DESC_NULLS_FIRST = Pattern.compile("\\s*?(?i:day)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern DAY_PATTERN_DESC_NULLS_LAST = Pattern.compile("\\s*?(?i:day)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?LAST)\\s*?");

    // HOUR patterns
    private static final Pattern HOUR_PATTERN = Pattern.compile("\\s*?(?i:hour)" + FUNCTION_ARGUMENT_NAME);
    private static final Pattern HOUR_PATTERN_ASC = Pattern.compile("\\s*?(?i:hour)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?");
    private static final Pattern HOUR_PATTERN_ASC_NULLS_FIRST = Pattern.compile("\\s*?(?i:hour)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern HOUR_PATTERN_ASC_NULLS_LAST = Pattern.compile("\\s*?(?i:hour)" + FUNCTION_ARGUMENT_NAME + "\\s*?(ASC)\\s*?(NULLS\\s*?LAST)\\s*?");
    private static final Pattern HOUR_PATTERN_DESC = Pattern.compile("\\s*?(?i:hour)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?");
    private static final Pattern HOUR_PATTERN_DESC_NULLS_FIRST = Pattern.compile("\\s*?(?i:hour)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern HOUR_PATTERN_DESC_NULLS_LAST = Pattern.compile("\\s*?(?i:hour)" + FUNCTION_ARGUMENT_NAME + "\\s*?(DESC)\\s*?(NULLS\\s*?LAST)\\s*?");

    // Truncate
    private static final Pattern ICEBERG_TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("\\s*?(?i:truncate)" + FUNCTION_ARGUMENT_NAME_AND_INT);
    private static final Pattern TRUNCATE_ASC_PATTERN = Pattern.compile("\\s*?(?i:truncate)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(ASC)\\s*?");
    private static final Pattern TRUNCATE_ASC_NULLS_FIRST_PATTERN = Pattern.compile("\\s*?(?i:truncate)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(ASC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern TRUNCATE_ASC_NULLS_LAST_PATTERN = Pattern.compile("\\s*?(?i:truncate)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(ASC)\\s*?(NULLS\\s*?LAST)\\s*?");
    private static final Pattern TRUNCATE_DESC_PATTERN = Pattern.compile("\\s*?(?i:truncate)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(DESC)\\s*?");
    private static final Pattern TRUNCATE_DESC_NULLS_FIRST_PATTERN = Pattern.compile("\\s*?(?i:truncate)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(DESC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern TRUNCATE_DESC_NULLS_LAST_PATTERN = Pattern.compile("\\s*?(?i:truncate)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(DESC)\\s*?(NULLS\\s*?LAST)\\s*?");

    // Bucket
    private static final Pattern ICEBERG_BUCKET_PATTERN = Pattern.compile("\\s*?(?i:bucket)\\[(\\d+)]");
    private static final Pattern BUCKET_PATTERN = Pattern.compile("\\s*?(?i:bucket)" + FUNCTION_ARGUMENT_NAME_AND_INT);
    private static final Pattern BUCKET_ASC_PATTERN = Pattern.compile("\\s*?(?i:bucket)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(ASC)\\s*?");
    private static final Pattern BUCKET_ASC_NULLS_FIRST_PATTERN = Pattern.compile("\\s*?(?i:bucket)" + FUNCTION_ARGUMENT_NAME_AND_INT + " \\s*?(ASC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern BUCKET_ASC_NULLS_LAST_PATTERN = Pattern.compile("\\s*?(?i:bucket)" + FUNCTION_ARGUMENT_NAME_AND_INT + " \\s*?(ASC)\\s*?(NULLS\\s*?LAST)\\s*?");
    private static final Pattern BUCKET_DESC_PATTERN = Pattern.compile("\\s*?(?i:bucket)" + FUNCTION_ARGUMENT_NAME_AND_INT + "\\s*?(DESC)\\s*?");
    private static final Pattern BUCKET_DESC_NULLS_FIRST_PATTERN = Pattern.compile("\\s*?(?i:bucket)" + FUNCTION_ARGUMENT_NAME_AND_INT + " \\s*?(DESC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern BUCKET_DESC_NULLS_LAST_PATTERN = Pattern.compile("\\s*?(?i:bucket)" + FUNCTION_ARGUMENT_NAME_AND_INT + " \\s*?(DESC)\\s*?(NULLS\\s*?LAST)\\s*?");

    // identity
    private static final Pattern IDENTITY_PATTERN = Pattern.compile(IDENTIFIER);
    private static final Pattern IDENTITY_ASC_PATTERN = Pattern.compile(IDENTIFIER + "\\s*?(ASC)\\s*?");
    private static final Pattern IDENTITY_ASC_NULLS_FIRST_PATTERN = Pattern.compile(IDENTIFIER + "\\s*?(ASC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern IDENTITY_ASC_NULLS_LAST_PATTERN = Pattern.compile(IDENTIFIER + "\\s*?(ASC)\\s*?(NULLS\\s*?LAST)\\s*?");
    private static final Pattern IDENTITY_DESC_PATTERN = Pattern.compile(IDENTIFIER + "\\s*?(DESC)\\s*?");
    private static final Pattern IDENTITY_DESC_NULLS_FIRST_PATTERN = Pattern.compile(IDENTIFIER + "\\s*?(DESC)\\s*?(NULLS\\s*?FIRST)\\s*?");
    private static final Pattern IDENTITY_DESC_NULLS_LAST_PATTERN = Pattern.compile(IDENTIFIER + "\\s*?(DESC)\\s*?(NULLS\\s*?LAST)\\s*?");

    private SortFields() {}

    public static SortOrder parseSortFields(Schema schema, List<String> fields)
    {
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        for (String field : fields) {
            parseSortField(builder, field);
        }
        return builder.build();
    }

    public static void parseSortField(SortOrder.Builder builder, String field)
    {
        boolean matched = false ||
                tryMatchYear(builder, field) ||
                tryMatchMonth(builder, field) ||
                tryMatchDay(builder, field) ||
                tryMatchHour(builder, field) ||
                tryMatchBucket(builder, field) ||
                tryMatchTruncate(builder, field) ||
                tryMatchWithoutTransform(builder, field);

        if (!matched) {
            throw new IllegalArgumentException("Invalid sort field declaration: " + field);
        }
    }

    private static boolean tryMatchWithoutTransform(SortOrder.Builder builder, String field)
    {
        return tryMatch(field, IDENTITY_ASC_NULLS_FIRST_PATTERN, match -> builder.asc(fromIdentifier(match.group(1).trim()), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, IDENTITY_ASC_NULLS_LAST_PATTERN, match -> builder.asc(fromIdentifier(match.group(1).trim()), NullOrder.NULLS_LAST)) ||
                tryMatch(field, IDENTITY_ASC_PATTERN, match -> builder.asc(fromIdentifier(match.group(1).trim()))) ||
                tryMatch(field, IDENTITY_DESC_NULLS_FIRST_PATTERN, match -> builder.desc(fromIdentifier(match.group(1).trim()), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, IDENTITY_DESC_NULLS_LAST_PATTERN, match -> builder.desc(fromIdentifier(match.group(1).trim()), NullOrder.NULLS_LAST)) ||
                tryMatch(field, IDENTITY_DESC_PATTERN, match -> builder.desc(fromIdentifier(match.group(1).trim()))) ||
                tryMatch(field, IDENTITY_PATTERN, match -> builder.asc(fromIdentifier(match.group(1).trim()))) ||
                false;
    }

    private static boolean tryMatchBucket(SortOrder.Builder builder, String field)
    {
        return tryMatch(field, BUCKET_ASC_NULLS_FIRST_PATTERN, match -> builder.asc(Expressions.bucket(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, BUCKET_ASC_NULLS_LAST_PATTERN, match -> builder.asc(Expressions.bucket(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, BUCKET_ASC_PATTERN, match -> builder.asc(Expressions.bucket(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())))) ||
                tryMatch(field, BUCKET_DESC_NULLS_FIRST_PATTERN, match -> builder.desc(Expressions.bucket(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, BUCKET_DESC_NULLS_LAST_PATTERN, match -> builder.desc(Expressions.bucket(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, BUCKET_DESC_PATTERN, match -> builder.desc(Expressions.bucket(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())))) ||
                tryMatch(field, BUCKET_PATTERN, match -> builder.asc(Expressions.bucket(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())))) ||
                false;
    }

    private static boolean tryMatchTruncate(SortOrder.Builder builder, String field)
    {
        return tryMatch(field, TRUNCATE_ASC_NULLS_FIRST_PATTERN, match -> builder.asc(Expressions.truncate(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, TRUNCATE_ASC_NULLS_LAST_PATTERN, match -> builder.asc(Expressions.truncate(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, TRUNCATE_ASC_PATTERN, match -> builder.asc(Expressions.truncate(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())))) ||
                tryMatch(field, TRUNCATE_DESC_NULLS_FIRST_PATTERN, match -> builder.desc(Expressions.truncate(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, TRUNCATE_DESC_NULLS_LAST_PATTERN, match -> builder.desc(Expressions.truncate(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, TRUNCATE_DESC_PATTERN, match -> builder.desc(Expressions.truncate(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())))) ||
                tryMatch(field, TRUNCATE_PATTERN, match -> builder.asc(Expressions.truncate(fromIdentifier(match.group(1).trim()), parseInt(match.group(2).trim())))) ||
                false;
    }

    private static boolean tryMatchHour(SortOrder.Builder builder, String field)
    {
        return tryMatch(field, HOUR_PATTERN_ASC_NULLS_FIRST, match -> builder.asc(Expressions.hour(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, HOUR_PATTERN_ASC_NULLS_LAST, match -> builder.asc(Expressions.hour(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, HOUR_PATTERN_ASC, match -> builder.asc(Expressions.hour(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, HOUR_PATTERN_DESC_NULLS_FIRST, match -> builder.desc(Expressions.hour(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, HOUR_PATTERN_DESC_NULLS_LAST, match -> builder.desc(Expressions.hour(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, HOUR_PATTERN_DESC, match -> builder.desc(Expressions.hour(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, HOUR_PATTERN, match -> builder.asc(Expressions.hour(fromIdentifier(match.group(1).trim())))) ||
                false;
    }

    private static boolean tryMatchDay(SortOrder.Builder builder, String field)
    {
        return tryMatch(field, DAY_PATTERN_ASC_NULLS_FIRST, match -> builder.asc(Expressions.day(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, DAY_PATTERN_ASC_NULLS_LAST, match -> builder.asc(Expressions.day(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, DAY_PATTERN_ASC, match -> builder.asc(Expressions.day(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, DAY_PATTERN_DESC_NULLS_FIRST, match -> builder.desc(Expressions.day(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, DAY_PATTERN_DESC_NULLS_LAST, match -> builder.desc(Expressions.day(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, DAY_PATTERN_DESC, match -> builder.desc(Expressions.day(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, DAY_PATTERN, match -> builder.asc(Expressions.day(fromIdentifier(match.group(1).trim())))) ||
                false;
    }

    private static boolean tryMatchMonth(SortOrder.Builder builder, String field)
    {
        return tryMatch(field, MONTH_PATTERN_ASC_NULLS_FIRST, match -> builder.asc(Expressions.month(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, MONTH_PATTERN_ASC_NULLS_LAST, match -> builder.asc(Expressions.month(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, MONTH_PATTERN_ASC, match -> builder.asc(Expressions.month(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, MONTH_PATTERN_DESC_NULLS_FIRST, match -> builder.desc(Expressions.month(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, MONTH_PATTERN_DESC_NULLS_LAST, match -> builder.desc(Expressions.month(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, MONTH_PATTERN_DESC, match -> builder.desc(Expressions.month(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, MONTH_PATTERN, match -> builder.asc(Expressions.month(fromIdentifier(match.group(1).trim())))) ||
                false;
    }

    private static boolean tryMatchYear(SortOrder.Builder builder, String field)
    {
        return tryMatch(field, YEAR_PATTERN_ASC_NULLS_FIRST, match -> builder.asc(Expressions.year(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, YEAR_PATTERN_ASC_NULLS_LAST, match -> builder.asc(Expressions.year(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, YEAR_PATTERN_ASC, match -> builder.asc(Expressions.year(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, YEAR_PATTERN_DESC_NULLS_FIRST, match -> builder.desc(Expressions.year(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_FIRST)) ||
                tryMatch(field, YEAR_PATTERN_DESC_NULLS_LAST, match -> builder.desc(Expressions.year(fromIdentifier(match.group(1).trim())), NullOrder.NULLS_LAST)) ||
                tryMatch(field, YEAR_PATTERN_DESC, match -> builder.desc(Expressions.year(fromIdentifier(match.group(1).trim())))) ||
                tryMatch(field, YEAR_PATTERN, match -> builder.asc(Expressions.year(fromIdentifier(match.group(1).trim())))) ||
                false;
    }

    private static boolean tryMatch(String value, Pattern pattern, Consumer<MatchResult> match)
    {
        Matcher matcher = pattern.matcher(value);
        if (matcher.matches()) {
            match.accept(matcher.toMatchResult());
            return true;
        }
        return false;
    }

    public static List<String> toSortFields(SortOrder spec)
    {
        return spec.fields().stream()
                .map(field -> toSortField(spec, field))
                .collect(toImmutableList());
    }

    private static String toSortField(SortOrder spec, SortField field)
    {
        String name = toIdentifier(spec.schema().findColumnName(field.sourceId()));
        String transform = field.transform().toString();
        String sortDirection = field.direction().toString();
        String nullOrder = field.nullOrder().toString();
        String suffix = format("%s %s", sortDirection, nullOrder);

        switch (transform) {
            case "identity":
                return format("%s %s", name, suffix);
            case "year":
            case "month":
            case "day":
            case "hour":
                return format("%s(%s) %s", transform, name, suffix);
        }

        Matcher matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return format("bucket(%s, %s) %s", name, matcher.group(1), suffix);
        }

        matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return format("truncate(%s, %s) %s", name, matcher.group(1), suffix);
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }
}
