/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.models;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.internal.Column;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.starburst.schema.discovery.infer.InferredPartitionProjection.NON_PROJECTED_PARTITION;
import static java.util.Objects.requireNonNull;

public record DiscoveredPartitions(List<Column> columns, List<DiscoveredPartitionValues> values, Map<LowerCaseString, InferredPartitionProjection> columnProjections)
{
    public static final DiscoveredPartitions EMPTY_DISCOVERED_PARTITIONS = new DiscoveredPartitions(ImmutableList.of(), ImmutableList.of());

    public DiscoveredPartitions
    {
        columns = ImmutableList.copyOf(columns);
        values = values.stream()
                .sorted(comparePartitionValuesByColumnOrder(columns))
                .collect(toImmutableList());
        // for backward compatibility, previously discovered partitions had no idea about projections
        final List<Column> finalColumns = columns;
        columnProjections = Optional.ofNullable(columnProjections)
                .orElseGet(() -> buildNonProjectedPartitionsFromColumns(finalColumns));
        validateDiscoveredPartitions(columns, values, columnProjections);
    }

    public DiscoveredPartitions(List<Column> columns, List<DiscoveredPartitionValues> values)
    {
        this(columns, values, buildNonProjectedPartitionsFromColumns(columns));
    }

    public static ValidatedPartitions createValidatedPartitions(List<Column> columns, List<DiscoveredPartitionValues> values, Map<LowerCaseString, InferredPartitionProjection> columnProjections)
    {
        try {
            DiscoveredPartitions discoveredPartitions = new DiscoveredPartitions(columns, values, columnProjections);
            return new ValidatedPartitions(Optional.empty(), discoveredPartitions);
        }
        catch (IllegalArgumentException e) {
            return new ValidatedPartitions(Optional.of(e.getMessage()), EMPTY_DISCOVERED_PARTITIONS);
        }
    }

    private static ImmutableMap<LowerCaseString, InferredPartitionProjection> buildNonProjectedPartitionsFromColumns(List<Column> columns)
    {
        return columns.stream().map(Column::name).collect(toImmutableMap(Function.identity(), ignored -> NON_PROJECTED_PARTITION));
    }

    private static Comparator<DiscoveredPartitionValues> comparePartitionValuesByColumnOrder(List<Column> columns)
    {
        return (v1, v2) -> {
            int compareResult = 0;
            for (Column column : columns) {
                String v1ColumnValue = v1.values().getOrDefault(column.name(), "");
                String v2ColumnValue = v2.values().getOrDefault(column.name(), "");
                compareResult = v1ColumnValue.compareTo(v2ColumnValue);
                if (compareResult != 0) {
                    break;
                }
            }
            return compareResult;
        };
    }

    private static void validateDiscoveredPartitions(List<Column> columns, List<DiscoveredPartitionValues> values, Map<LowerCaseString, InferredPartitionProjection> columnProjections)
    {
        final List<LowerCaseString> columnNames = columns.stream().map(Column::name).collect(toImmutableList());
        final int columnsSize = columnNames.size();
        checkArgument(columnsSize == columnProjections.size(), "Not all columns have associated possible projection. Expected values for: [%s], but found: [%s]", columnNames, columnProjections.keySet());
        values.stream()
                .map(DiscoveredPartitionValues::values)
                .forEach(value -> checkArgument(value.size() == columnsSize, "Partition value missing. Expected values for: [%s], but found: [%s]. There is mismatched folder structure in some partitions.".formatted(columnNames, value.keySet())));
    }

    public IntegerProjectionMinMaxRange computeIntegerProjectionRange(LowerCaseString columnName)
    {
        Set<Integer> integerProjectionValues = values.stream().map(v -> v.values().get(columnName))
                .map(Integer::parseInt)
                .collect(toImmutableSet());
        int min = Collections.min(integerProjectionValues);
        int max = Collections.max(integerProjectionValues);

        return new IntegerProjectionMinMaxRange(min, max);
    }

    public String computeEnumProjectionPossibleValues(LowerCaseString columnName)
    {
        return values.stream().map(v -> v.values().get(columnName))
                .distinct()
                .sorted(String::compareToIgnoreCase)
                .map(v -> "'" + v + "'")
                .collect(Collectors.joining(", "));
    }

    public record IntegerProjectionMinMaxRange(int min, int max)
    {}

    public record ValidatedPartitions(Optional<String> errorMessage, DiscoveredPartitions partitions)
    {
        public ValidatedPartitions
        {
            requireNonNull(errorMessage, "errorMessage is null");
            requireNonNull(partitions, "partitions is null");
        }
    }
}
