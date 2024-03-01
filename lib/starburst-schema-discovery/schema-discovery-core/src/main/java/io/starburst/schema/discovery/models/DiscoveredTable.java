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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.formats.csv.CsvOptions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.starburst.schema.discovery.models.DiscoveredColumns.EMPTY_DISCOVERED_COLUMNS;
import static io.starburst.schema.discovery.models.DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.TableFormat.ERROR;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("BooleanMethodIsAlwaysInverted")
public record DiscoveredTable(boolean valid, TablePath path, TableName tableName, TableFormat format,
                              Map<String, String> options, DiscoveredColumns columns,
                              DiscoveredPartitions discoveredPartitions, Collection<LowerCaseString> buckets,
                              List<String> errors)
{
    public static final DiscoveredTable EMPTY_DISCOVERED_TABLE = new DiscoveredTable(false, SlashEndedPath.SINGLE_SLASH_EMPTY, new TableName(Optional.empty(), toLowerCase("error")), ERROR, CsvOptions.standard(), EMPTY_DISCOVERED_COLUMNS, EMPTY_DISCOVERED_PARTITIONS, ImmutableList.of(), ImmutableList.of());

    public DiscoveredTable
    {
        requireNonNull(path, "path cannot be null");
        requireNonNull(tableName, "tableName cannot be null");
        requireNonNull(format, "format cannot be null");
        requireNonNull(columns, "columns cannot be null");
        requireNonNull(discoveredPartitions, "discoveredPartitions cannot be null");
        options = ImmutableMap.copyOf(options);
        buckets = ImmutableSet.copyOf(buckets);
        errors = Optional.ofNullable(errors).map(ImmutableList::copyOf).orElseGet(ImmutableList::of);
    }

    // for backwards compatibility
    public DiscoveredTable(boolean valid, SlashEndedPath path, TableName tableName, TableFormat tableFormat, Map<String, String> options, DiscoveredColumns columns, DiscoveredPartitions discoveredPartitions, Collection<LowerCaseString> buckets)
    {
        this(valid, path, tableName, tableFormat, options, columns, discoveredPartitions, buckets, ImmutableList.of());
    }

    public DiscoveredTable withColumns(DiscoveredColumns columns)
    {
        return new DiscoveredTable(valid, path, tableName, format, options, columns, discoveredPartitions, buckets, errors);
    }

    public DiscoveredTable asRecursiveTable(SlashEndedPath newPath, TableName tableName)
    {
        return new DiscoveredTable(
                valid,
                newPath,
                tableName,
                format,
                options,
                columns,
                EMPTY_DISCOVERED_PARTITIONS,
                buckets,
                errors);
    }

    public boolean hasAnyProjectedPartition()
    {
        return discoveredPartitions.columnProjections().values().stream().anyMatch(p -> !p.isEqualSignSeparatedPartition());
    }

    @VisibleForTesting
    public DiscoveredTable skipErrors()
    {
        return new DiscoveredTable(valid, path, tableName, format, options, columns, discoveredPartitions, buckets, ImmutableList.of());
    }

    public DiscoveredTable withPath(TablePath path)
    {
        return new DiscoveredTable(
                valid,
                path,
                tableName,
                format,
                options,
                columns,
                discoveredPartitions,
                buckets,
                errors);
    }

    public static DiscoveredTable emptyWithPathAndErrors(TablePath path, List<String> errors)
    {
        return new DiscoveredTable(
                EMPTY_DISCOVERED_TABLE.valid(),
                path,
                EMPTY_DISCOVERED_TABLE.tableName(),
                EMPTY_DISCOVERED_TABLE.format(),
                EMPTY_DISCOVERED_TABLE.options(),
                EMPTY_DISCOVERED_TABLE.columns(),
                EMPTY_DISCOVERED_TABLE.discoveredPartitions(),
                EMPTY_DISCOVERED_TABLE.buckets(),
                errors);
    }
}
