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

import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.formats.lakehouse.LakehouseFormat;
import io.trino.filesystem.Location;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RecordingFileTrackerDecorator
        implements FileTracker
{
    private final FileTracker delegate;
    private final ImmutableSet.Builder<Location> recordedPaths = ImmutableSet.builder();

    public RecordingFileTrackerDecorator(FileTracker delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public boolean shouldStopAddingTables(Location potentialTablePath)
    {
        return delegate.shouldStopAddingTables(potentialTablePath);
    }

    @Override
    public boolean hasEnoughSampledTables(Location directoryPath)
    {
        recordedPaths.add(directoryPath);
        return delegate.hasEnoughSampledTables(directoryPath);
    }

    @Override
    public SampleFileResult getNextSampleFileForTable(Location directoryPath, Location filePath, Optional<LakehouseFormat> lakehouseFormat)
    {
        recordedPaths.add(filePath);
        return delegate.getNextSampleFileForTable(directoryPath, filePath, lakehouseFormat);
    }

    public Set<Location> getRecordedPaths()
    {
        return recordedPaths.build();
    }
}
