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
import io.starburst.schema.discovery.formats.lakehouse.LakehouseUtil;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.trino.filesystem.Location;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.starburst.schema.discovery.io.LocationUtils.parentOf;

final class RecursiveDirectoriesFileTracker
        extends FileTrackerBase
{
    private final Map<Location, AtomicInteger> recursiveTableFileCount = new ConcurrentHashMap<>();

    public RecursiveDirectoriesFileTracker(Location root, GeneralOptions generalOptions)
    {
        super(root, generalOptions);
    }

    @Override
    public boolean shouldStopAddingTables(Location potentialTablePath)
    {
        Location recursiveTablePath = getPathClosestToRoot(potentialTablePath);
        return (!recursiveTableFileCount.containsKey(recursiveTablePath) && hasEnoughSampledTables(potentialTablePath));
    }

    @Override
    public boolean hasEnoughSampledTables(Location directoryPath)
    {
        if (LakehouseUtil.deltaLakeParent(root, directoryPath).isPresent()) {
            return false;
        }
        Location potentialTablePath = getPathClosestToRoot(directoryPath);
        return recursiveTableFileCount.size() >= generalOptions.maxSampleTables() ||
               recursiveTableFileCount.computeIfAbsent(potentialTablePath, __ -> new AtomicInteger()).get() > generalOptions.maxSampleFilesPerTable();
    }

    @Override
    public SampleFileResult getNextSampleFileForTable(Location directoryPath, Location filePath, Optional<LakehouseFormat> lakehouseFormat)
    {
        Location recursiveTablePath = getPathClosestToRoot(directoryPath);
        AtomicInteger fileIndex = recursiveTableFileCount.computeIfAbsent(recursiveTablePath, __ -> new AtomicInteger());
        return getNextSampleFile(filePath, lakehouseFormat, fileIndex);
    }

    private Location getPathClosestToRoot(Location path)
    {
        if (!path.toString().startsWith(root.toString())) {
            throw new RuntimeException("Internal error - path: [%s] outside of root directory: [%s]".formatted(path, root));
        }

        Location parent = path;
        while (!root.equals(parent) && !root.equals(parentOf(parent))) {
            parent = parentOf(parent);
        }
        return parent;
    }
}
