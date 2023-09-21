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
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import java.util.List;
import java.util.function.Consumer;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public final class PartitionFields
{
    private static final String UNQUOTED_IDENTIFIER = "[a-zA-Z_][a-zA-Z0-9_]*";
    private static final String QUOTED_IDENTIFIER = "\"(?:\"\"|[^\"])*\"";
    public static final String IDENTIFIER = "(" + UNQUOTED_IDENTIFIER + "|" + QUOTED_IDENTIFIER + ")";
    private static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile(UNQUOTED_IDENTIFIER);
    private static final Pattern QUOTED_IDENTIFIER_PATTERN = Pattern.compile(QUOTED_IDENTIFIER);

    private static final String FUNCTION_ARGUMENT_NAME = "\\(" + IDENTIFIER + "\\)\\s*";
    private static final String FUNCTION_ARGUMENT_NAME_AND_INT = "\\(" + IDENTIFIER + ",\\s*(\\d+)\\)";

    private static final Pattern IDENTITY_PATTERN = Pattern.compile(IDENTIFIER, CASE_INSENSITIVE);
    private static final Pattern YEAR_PATTERN = Pattern.compile("year" + FUNCTION_ARGUMENT_NAME, CASE_INSENSITIVE);
    private static final Pattern MONTH_PATTERN = Pattern.compile("month" + FUNCTION_ARGUMENT_NAME, CASE_INSENSITIVE);
    private static final Pattern DAY_PATTERN = Pattern.compile("day" + FUNCTION_ARGUMENT_NAME, CASE_INSENSITIVE);
    private static final Pattern HOUR_PATTERN = Pattern.compile("hour" + FUNCTION_ARGUMENT_NAME, CASE_INSENSITIVE);
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket" + FUNCTION_ARGUMENT_NAME_AND_INT, CASE_INSENSITIVE);
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate" + FUNCTION_ARGUMENT_NAME_AND_INT, CASE_INSENSITIVE);
    private static final Pattern VOID_PATTERN = Pattern.compile("void" + FUNCTION_ARGUMENT_NAME, CASE_INSENSITIVE);

    private static final Pattern ICEBERG_BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern ICEBERG_TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");

    private PartitionFields() {}

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields)
    {
        try {
            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
            for (String field : fields) {
                parsePartitionField(builder, field);
            }
            return builder.build();
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Unable to parse partitioning value: " + e.getMessage(), e);
        }
    }

    public static void parsePartitionField(PartitionSpec.Builder builder, String field)
    {
        boolean matched = tryMatch(field, IDENTITY_PATTERN, match -> builder.identity(fromIdentifierToColumn(match.group()))) ||
                tryMatch(field, YEAR_PATTERN, match -> builder.year(fromIdentifierToColumn(match.group(1)))) ||
                tryMatch(field, MONTH_PATTERN, match -> builder.month(fromIdentifierToColumn(match.group(1)))) ||
                tryMatch(field, DAY_PATTERN, match -> builder.day(fromIdentifierToColumn(match.group(1)))) ||
                tryMatch(field, HOUR_PATTERN, match -> builder.hour(fromIdentifierToColumn(match.group(1)))) ||
                tryMatch(field, BUCKET_PATTERN, match -> builder.bucket(fromIdentifierToColumn(match.group(1)), parseInt(match.group(2)))) ||
                tryMatch(field, TRUNCATE_PATTERN, match -> builder.truncate(fromIdentifierToColumn(match.group(1)), parseInt(match.group(2)))) ||
                tryMatch(field, VOID_PATTERN, match -> builder.alwaysNull(fromIdentifierToColumn(match.group(1))));
        if (!matched) {
            throw new IllegalArgumentException("Invalid partition field declaration: " + field);
        }
    }

    public static String fromIdentifierToColumn(String identifier)
    {
        if (QUOTED_IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            // We only support lowercase quoted identifiers for now.
            // See https://github.com/trinodb/trino/issues/12226#issuecomment-1128839259
            // TODO: Enhance quoted identifiers support in Iceberg partitioning to support mixed case identifiers
            //  See https://github.com/trinodb/trino/issues/12668
            if (!identifier.toLowerCase(ENGLISH).equals(identifier)) {
                throw new IllegalArgumentException(format("Uppercase characters in identifier '%s' are not supported.", identifier));
            }
            return identifier.substring(1, identifier.length() - 1).replace("\"\"", "\"");
        }
        // Currently, all Iceberg columns are stored in lowercase in the Iceberg metadata files.
        // Unquoted identifiers are canonicalized to lowercase here which is not according ANSI SQL spec.
        // See https://github.com/trinodb/trino/issues/17
        return identifier.toLowerCase(ENGLISH);
    }

    private static boolean tryMatch(CharSequence value, Pattern pattern, Consumer<MatchResult> match)
    {
        Matcher matcher = pattern.matcher(value);
        if (matcher.matches()) {
            match.accept(matcher.toMatchResult());
            return true;
        }
        return false;
    }

    public static List<String> toPartitionFields(PartitionSpec spec)
    {
        return spec.fields().stream()
                .map(field -> toPartitionField(spec, field))
                .collect(toImmutableList());
    }

    private static String toPartitionField(PartitionSpec spec, PartitionField field)
    {
        String name = fromColumnToIdentifier(spec.schema().findColumnName(field.sourceId()));
        String transform = field.transform().toString();

        switch (transform) {
            case "identity":
                return name;
            case "year":
            case "month":
            case "day":
            case "hour":
            case "void":
                return format("%s(%s)", transform, name);
        }

        Matcher matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return format("bucket(%s, %s)", name, matcher.group(1));
        }

        matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return format("truncate(%s, %s)", name, matcher.group(1));
        }

        throw new UnsupportedOperationException("Unsupported partition transform: " + field);
    }

    private static String fromColumnToIdentifier(String column)
    {
        return quotedName(column);
    }

    public static String quotedName(String name)
    {
        if (UNQUOTED_IDENTIFIER_PATTERN.matcher(name).matches()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }
}
