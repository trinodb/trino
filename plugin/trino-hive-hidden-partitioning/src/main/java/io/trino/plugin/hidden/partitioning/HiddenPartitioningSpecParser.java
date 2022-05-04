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
package io.trino.plugin.hidden.partitioning;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public class HiddenPartitioningSpecParser
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String FUNCTION_INT = "\\[ *(\\d+) *\\]";
    private static final Pattern IDENTITY_PATTERN = Pattern.compile("identity");
    private static final Pattern YEAR_PATTERN = Pattern.compile("year");
    private static final Pattern MONTH_PATTERN = Pattern.compile("month");
    private static final Pattern DAY_PATTERN = Pattern.compile("day");
    private static final Pattern HOUR_PATTERN = Pattern.compile("hour");
    private static final Pattern BUCKET_PATTERN = Pattern.compile("bucket" + FUNCTION_INT);
    private static final Pattern TRUNCATE_PATTERN = Pattern.compile("truncate" + FUNCTION_INT);

    private HiddenPartitioningSpecParser()
    {
    }

    public static HiddenPartitioningPartitionSpec fromJson(String partitionSpecJson)
    {
        try {
            return MAPPER.readValue(partitionSpecJson, HiddenPartitioningPartitionSpec.class);
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Unable to parse partition spec: " + partitionSpecJson);
        }
    }

    public static void validateSpecJson(String partitionSpecJson)
    {
        HiddenPartitioningPartitionSpec hiddenPartitioningPartitionSpec = fromJson(partitionSpecJson);
        for (HiddenPartitioningPartitionSpec.PartitionField specField : hiddenPartitioningPartitionSpec.getFields()) {
            requireNonNull(specField.getName(), "partition spec name is null");
            requireNonNull(specField.getTransform(), "partition spec transform is null");
            requireNonNull(specField.getSourceName(), "partition spec source name is null");
        }
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

    public static org.apache.iceberg.PartitionSpec toIcebergPartitionSpec(HiddenPartitioningPartitionSpec hiddenPartitioningPartitionSpec, Schema icebergSchema)
    {
        // Iceberg's PartitionSpec builder requires names that aren't already in the Schema, so first remove the spec field names from the Schema
        ImmutableSet<String> partitionColNames = hiddenPartitioningPartitionSpec.getFields().stream()
                .map(HiddenPartitioningPartitionSpec.PartitionField::getName)
                .map(String::toLowerCase)
                .collect(toImmutableSet());
        List<Types.NestedField> baseFields = icebergSchema.columns().stream()
                .filter(schemaField -> !partitionColNames.contains(schemaField.name()))
                .collect(Collectors.toList());
        Schema baseSchema = new Schema(baseFields);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        TypeUtil.assignFreshIds(baseSchema, nextFieldId::getAndIncrement);

        org.apache.iceberg.PartitionSpec.Builder builder = org.apache.iceberg.PartitionSpec.builderFor(baseSchema);
        for (HiddenPartitioningPartitionSpec.PartitionField trinoPartitionField : hiddenPartitioningPartitionSpec.getFields()) {
            boolean matched = tryMatch(trinoPartitionField.getTransform(), IDENTITY_PATTERN, match -> builder.identity(match.group())) ||
                    tryMatch(trinoPartitionField.getTransform(), YEAR_PATTERN, match -> builder.year(trinoPartitionField.getSourceName(), trinoPartitionField.getName())) ||
                    tryMatch(trinoPartitionField.getTransform(), MONTH_PATTERN, match -> builder.month(trinoPartitionField.getSourceName(), trinoPartitionField.getName())) ||
                    tryMatch(trinoPartitionField.getTransform(), DAY_PATTERN, match -> builder.day(trinoPartitionField.getSourceName(), trinoPartitionField.getName())) ||
                    tryMatch(trinoPartitionField.getTransform(), HOUR_PATTERN, match -> builder.hour(trinoPartitionField.getSourceName(), trinoPartitionField.getName())) ||
                    tryMatch(trinoPartitionField.getTransform(), BUCKET_PATTERN, match -> builder.bucket(trinoPartitionField.getSourceName(), parseInt(match.group(1)), trinoPartitionField.getName())) ||
                    tryMatch(trinoPartitionField.getTransform(), TRUNCATE_PATTERN, match -> builder.truncate(trinoPartitionField.getSourceName(), parseInt(match.group(1)), trinoPartitionField.getName()));

            if (!matched) {
                throw new IllegalArgumentException("Invalid partition field declaration: " + trinoPartitionField.getTransform());
            }
        }
        return builder.build();
    }
}
