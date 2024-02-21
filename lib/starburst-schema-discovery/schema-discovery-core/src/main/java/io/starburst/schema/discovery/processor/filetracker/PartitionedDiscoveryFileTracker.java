/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor.filetracker;

import io.starburst.schema.discovery.formats.lakehouse.LakehouseFormat;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.trino.filesystem.Location;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class PartitionedDiscoveryFileTracker
        extends FileTrackerBase
{
    private final Map<Location, AtomicInteger> potentialTableFileCount = new ConcurrentHashMap<>();

    public PartitionedDiscoveryFileTracker(Location root, GeneralOptions generalOptions)
    {
        super(root, generalOptions);
    }

    @Override
    public boolean shouldStopAddingTables(Location potentialTablePath)
    {
        return (!potentialTableFileCount.containsKey(potentialTablePath) && hasEnoughSampledTables(potentialTablePath));
    }

    @Override
    public boolean hasEnoughSampledTables(Location ignored)
    {
        return potentialTableFileCount.size() >= generalOptions.maxSampleTables();
    }

    @Override
    public SampleFileResult getNextSampleFileForTable(Location directoryPath, Location filePath, Optional<LakehouseFormat> lakehouseFormat)
    {
        AtomicInteger fileIndex = potentialTableFileCount.computeIfAbsent(directoryPath, __ -> new AtomicInteger());
        return getNextSampleFile(filePath, lakehouseFormat, fileIndex);
    }
}
