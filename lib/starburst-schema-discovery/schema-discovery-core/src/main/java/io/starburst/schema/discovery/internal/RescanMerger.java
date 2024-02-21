/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.starburst.schema.discovery.SchemaDiscoveryErrorCode;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.SlashEndedPath;
import io.trino.spi.TrinoException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class RescanMerger
{
    private RescanMerger() {}

    public static DiscoveredSchema mergeRescan(DiscoveredSchema discoveredSchema, ObjectMapper objectMapper, String rescanMetadata)
    {
        List<DiscoveredTable> previousTables;
        try {
            previousTables = objectMapper.readValue(rescanMetadata, new TypeReference<>() {});
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(SchemaDiscoveryErrorCode.INVALID_METADATA, "Invalid rescan metadata", e);
        }

        Map<SlashEndedPath, DiscoveredTable> discoveredTablesMap = new HashMap<>(discoveredSchema.tables().stream().collect(toImmutableMap(DiscoveredTable::path, Function.identity())));

        List<DiscoveredTable> updatedTables = previousTables.stream()
                .map(previousTable -> {
                    DiscoveredTable updatedTable = discoveredTablesMap.remove(previousTable.path());
                    return (updatedTable != null) ? updatedTable : previousTable;
                })
                .collect(toImmutableList());

        if (!discoveredTablesMap.isEmpty()) {
            updatedTables = ImmutableList.<DiscoveredTable>builder()
                    .addAll(updatedTables)
                    .addAll(discoveredTablesMap.values())
                    .build();
        }

        return new DiscoveredSchema(discoveredSchema.rootPath(), updatedTables, discoveredSchema.errors());
    }
}
