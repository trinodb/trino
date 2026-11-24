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

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;

import java.util.List;
import java.util.regex.Matcher;

import static com.google.common.collect.ImmutableList.toImmutableList;
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
import static io.trino.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static java.lang.Integer.parseInt;

public final class PartitionFields
{
    private PartitionFields() {}

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields)
    {
        return parsePartitionFields(schema, fields, ImmutableList.of());
    }

    public static PartitionSpec parsePartitionFields(Schema schema, List<String> fields, List<PartitionField> existingPartitionFields)
    {
        try {
            PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
            for (String field : fields) {
                parsePartitionField(schema, fields, builder, field, includeWidthInPartitionName(field, existingPartitionFields));
            }
            return builder.build();
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_TABLE_PROPERTY, "Unable to parse partitioning value: " + e.getMessage(), e);
        }
    }

    private static void parsePartitionField(Schema schema, List<String> fields, PartitionSpec.Builder builder, String field, boolean includeWidthInPartitionName)
    {
        for (int i = 1; i < schema.columns().size() + fields.size(); i++) {
            try {
                parsePartitionField(builder, field, i == 1 ? "" : "_" + i, includeWidthInPartitionName);
                return;
            }
            catch (IllegalArgumentException e) {
                if (e.getMessage().contains("Cannot create partition from name that exists in schema")
                        || e.getMessage().contains("Cannot create identity partition sourced from different field in schema")) {
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalArgumentException("Cannot resolve partition field: " + field);
    }

    // Check if width should be included in partition field name to avoid conflicts
    private static boolean includeWidthInPartitionName(String field, List<PartitionField> existingFields)
    {
        Matcher matcher = TRUNCATE_PATTERN.matcher(field);
        if (matcher.matches()) {
            return isFieldNameUsedByAnotherTransform(
                    existingFields,
                    fromIdentifierToColumn(matcher.group(1)) + "_trunc",
                    Transforms.truncate(parseInt(matcher.group(2))));
        }
        matcher = BUCKET_PATTERN.matcher(field);
        if (matcher.matches()) {
            return isFieldNameUsedByAnotherTransform(
                    existingFields,
                    fromIdentifierToColumn(matcher.group(1)) + "_bucket",
                    Transforms.bucket(parseInt(matcher.group(2))));
        }
        return false;
    }

    private static boolean isFieldNameUsedByAnotherTransform(List<PartitionField> existingPartitionFields, String partitionFieldName, Transform<?, ?> transform)
    {
        // Check if a partition field name is already used by another transform
        return existingPartitionFields.stream().anyMatch(
                partitionField -> partitionField.name().equalsIgnoreCase(partitionFieldName)
                        && !partitionField.transform().equals(transform));
    }

    public static void parsePartitionField(PartitionSpec.Builder builder, String field, String suffix, boolean includeWidthInPartitionName)
    {
        boolean matched = tryMatch(field, IDENTITY_PATTERN, match -> {
            // identity doesn't allow specifying an alias
            builder.identity(fromIdentifierToColumn(match.group()));
        }) ||
                tryMatch(field, YEAR_PATTERN, match -> {
                    String column = fromIdentifierToColumn(match.group(1));
                    builder.year(column, column + "_year" + suffix);
                }) ||
                tryMatch(field, MONTH_PATTERN, match -> {
                    String column = fromIdentifierToColumn(match.group(1));
                    builder.month(column, column + "_month" + suffix);
                }) ||
                tryMatch(field, DAY_PATTERN, match -> {
                    String column = fromIdentifierToColumn(match.group(1));
                    builder.day(column, column + "_day" + suffix);
                }) ||
                tryMatch(field, HOUR_PATTERN, match -> {
                    String column = fromIdentifierToColumn(match.group(1));
                    builder.hour(column, column + "_hour" + suffix);
                }) ||
                tryMatch(field, BUCKET_PATTERN, match -> {
                    String column = fromIdentifierToColumn(match.group(1));
                    int numBuckets = parseInt(match.group(2));
                    String newSuffix = includeWidthInPartitionName ? "_" + numBuckets + suffix : suffix;
                    builder.bucket(column, numBuckets, column + "_bucket" + newSuffix);
                }) ||
                tryMatch(field, TRUNCATE_PATTERN, match -> {
                    String column = fromIdentifierToColumn(match.group(1));
                    int width = parseInt(match.group(2));
                    String newSuffix = includeWidthInPartitionName ? "_" + width + suffix : suffix;
                    builder.truncate(column, width, column + "_trunc" + newSuffix);
                }) ||
                tryMatch(field, VOID_PATTERN, match -> {
                    String column = fromIdentifierToColumn(match.group(1));
                    builder.alwaysNull(column, column + "_null" + suffix);
                });
        if (!matched) {
            throw new IllegalArgumentException("Invalid partition field declaration: " + field);
        }
    }

    public static List<String> toPartitionFields(PartitionSpec spec)
    {
        return spec.fields().stream()
                .map(field -> toPartitionField(spec, field))
                .collect(toImmutableList());
    }

    private static String toPartitionField(PartitionSpec spec, PartitionField field)
    {
        return toTransformTerm(spec.schema().findColumnName(field.sourceId()), field.transform().toString());
    }
}
