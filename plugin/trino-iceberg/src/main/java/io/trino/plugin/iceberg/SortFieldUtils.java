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

import io.trino.spi.TrinoException;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderBuilder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.transforms.Transforms;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergTableProperties.SORTED_BY_PROPERTY;
import static io.trino.plugin.iceberg.TransformUtils.BUCKET_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.DAY_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.HOUR_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.IDENTITY_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.MONTH_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.TRUNCATE_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.VOID_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.YEAR_PATTERN;
import static io.trino.plugin.iceberg.TransformUtils.fromIdentifierToColumn;
import static io.trino.plugin.iceberg.TransformUtils.toTransformTerm;
import static io.trino.plugin.iceberg.TransformUtils.tryMatch;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNullElse;

public final class SortFieldUtils
{
    private SortFieldUtils() {}

    private static final Pattern PATTERN = Pattern.compile(
            "\\s*(?<term>.+?)"
                    + "(?i:\\s+(?<ordering>ASC|DESC))?"
                    + "(?i:\\s+NULLS\\s+(?<nullOrder>FIRST|LAST))?"
                    + "\\s*");

    public static SortOrder parseSortFields(Schema schema, List<String> fields)
    {
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        parseSortFields(builder, fields);
        SortOrder sortOrder;
        try {
            sortOrder = builder.build();
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid " + SORTED_BY_PROPERTY + " definition: " + e.getMessage(), e);
        }

        for (SortField field : sortOrder.fields()) {
            if (schema.accessorForField(field.sourceId()) == null) {
                throw new TrinoException(COLUMN_NOT_FOUND, "Column not found: " + schema.findColumnName(field.sourceId()));
            }
        }

        return sortOrder;
    }

    public static void parseSortFields(SortOrderBuilder<?> sortOrderBuilder, List<String> fields)
    {
        try {
            fields.forEach(field -> parseSortField(sortOrderBuilder, field));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Invalid " + SORTED_BY_PROPERTY + " definition: " + e.getMessage(), e);
        }
    }

    private static void parseSortField(SortOrderBuilder<?> builder, String field)
    {
        Matcher matcher = PATTERN.matcher(field);
        if (!matcher.matches()) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Unable to parse sort field: [%s]".formatted(field));
        }

        Term term = parseTerm(field, matcher.group("term"));

        boolean ascending = switch (requireNonNullElse(matcher.group("ordering"), "ASC").toUpperCase(ENGLISH)) {
            case "ASC" -> true;
            case "DESC" -> false;
            default -> throw new IllegalStateException("Unexpected ordering value"); // Unreachable
        };

        String nullOrderDefault = ascending ? "FIRST" : "LAST";
        NullOrder nullOrder = switch (requireNonNullElse(matcher.group("nullOrder"), nullOrderDefault).toUpperCase(ENGLISH)) {
            case "FIRST" -> NullOrder.NULLS_FIRST;
            case "LAST" -> NullOrder.NULLS_LAST;
            default -> throw new IllegalStateException("Unexpected null ordering value"); // Unreachable
        };

        if (ascending) {
            builder.asc(term, nullOrder);
        }
        else {
            builder.desc(term, nullOrder);
        }
    }

    private static Term parseTerm(String field, String termString)
    {
        AtomicReference<Term> target = new AtomicReference<>();
        boolean matched = tryMatch(termString, IDENTITY_PATTERN, match -> target.set(Expressions.ref(fromIdentifierToColumn(match.group()))))
                || tryMatch(termString, YEAR_PATTERN, match -> target.set(Expressions.year(fromIdentifierToColumn(match.group(1)))))
                || tryMatch(termString, MONTH_PATTERN, match -> target.set(Expressions.month(fromIdentifierToColumn(match.group(1)))))
                || tryMatch(termString, DAY_PATTERN, match -> target.set(Expressions.day(fromIdentifierToColumn(match.group(1)))))
                || tryMatch(termString, HOUR_PATTERN, match -> target.set(Expressions.hour(fromIdentifierToColumn(match.group(1)))))
                || tryMatch(termString, BUCKET_PATTERN, match -> target.set(Expressions.bucket(fromIdentifierToColumn(match.group(1)), parseInt(match.group(2)))))
                || tryMatch(termString, TRUNCATE_PATTERN, match -> target.set(Expressions.truncate(fromIdentifierToColumn(match.group(1)), parseInt(match.group(2)))))
                || tryMatch(termString, VOID_PATTERN, match -> target.set(Expressions.transform(fromIdentifierToColumn(match.group(1)), Transforms.alwaysNull())));
        if (!matched) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Unable to parse sort field: [%s]".formatted(field));
        }
        return target.get();
    }

    // Returns empty when the sort order contains a transform that Trino cannot render
    public static Optional<List<String>> tryToSortFields(SortOrder spec)
    {
        try {
            return Optional.of(toSortFields(spec));
        }
        catch (UnsupportedOperationException e) {
            return Optional.empty();
        }
    }

    public static List<String> toSortFields(SortOrder spec)
    {
        return spec.fields().stream()
                .map(field -> toSortField(spec, field))
                .collect(toImmutableList());
    }

    private static String toSortField(SortOrder spec, SortField field)
    {
        String term = toTransformTerm(spec.schema().findColumnName(field.sourceId()), field.transform().toString());
        return format("%s %s %s", term, field.direction(), field.nullOrder());
    }
}
