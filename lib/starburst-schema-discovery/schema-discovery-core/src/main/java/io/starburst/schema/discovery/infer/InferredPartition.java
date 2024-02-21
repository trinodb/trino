/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.infer;

import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.LowerCaseString;
import io.trino.filesystem.Location;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static java.util.Objects.requireNonNull;

public class InferredPartition
{
    private final Location path;
    private final Column column;
    private final String value;
    private final boolean isBucket;
    private final InferredPartitionProjection partitionProjection;

    public InferredPartition(Location path, Column column, String value, boolean isBucket, InferredPartitionProjection partitionProjection)
    {
        this.path = path;
        this.column = requireNonNull(column, "column cannot be null");
        this.value = requireNonNull(value, "value cannot be null");
        this.isBucket = isBucket;
        this.partitionProjection = requireNonNull(partitionProjection, "partitionProjection is null");
    }

    public static List<Column> buildColumns(List<InferredPartition> partitions)
    {
        return partitions.stream()
                .filter(partition -> !partition.isBucket())
                .map(InferredPartition::column)
                .collect(toImmutableList());
    }

    public static List<DiscoveredPartitionValues> buildValues(List<InferredPartition> partitions)
    {
        Map<Location, List<InferredPartition>> groupedByPath = partitions.stream()
                .filter(partition -> !partition.isBucket())
                .collect(Collectors.groupingBy(InferredPartition::path));
        return groupedByPath.entrySet()
                .stream()
                .map(entry -> new DiscoveredPartitionValues(ensureEndsWithSlash(entry.getKey().toString()), mapValues(entry.getValue())))
                .collect(toImmutableList());
    }

    public static List<LowerCaseString> buildBuckets(List<InferredPartition> partitions)
    {
        return partitions.stream()
                .filter(InferredPartition::isBucket)
                .map(partition -> partition.column.name())
                .collect(toImmutableList());
    }

    private static Map<LowerCaseString, String> mapValues(List<InferredPartition> partitions)
    {
        return partitions.stream()
                .filter(partition -> !partition.isBucket())
                .collect(toImmutableMap(partition -> partition.column.name(), partition -> partition.value));
    }

    public Location path()
    {
        return path;
    }

    public Column column()
    {
        return column;
    }

    public String value()
    {
        return value;
    }

    public boolean isBucket()
    {
        return isBucket;
    }

    public InferredPartitionProjection partitionProjection()
    {
        return partitionProjection;
    }
}
