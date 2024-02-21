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
import io.starburst.schema.discovery.processor.Processor.ProcessorPath;
import io.trino.filesystem.Location;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.starburst.schema.discovery.processor.filetracker.FileTracker.SampleFileResult.enoughSamples;
import static io.starburst.schema.discovery.processor.filetracker.FileTracker.SampleFileResult.notEnoughSamples;
import static io.starburst.schema.discovery.processor.filetracker.FileTracker.SampleFileResult.sampleFile;
import static java.util.Objects.requireNonNull;

abstract sealed class FileTrackerBase
        implements FileTracker
        permits PartitionedDiscoveryFileTracker, RecursiveDirectoriesFileTracker
{
    protected final Location root;
    protected final GeneralOptions generalOptions;

    FileTrackerBase(Location root, GeneralOptions generalOptions)
    {
        this.root = requireNonNull(root, "root is null");
        this.generalOptions = requireNonNull(generalOptions, "generalOptions is null");
    }

    protected SampleFileResult getNextSampleFile(Location path, Optional<LakehouseFormat> lakehouseFormat, AtomicInteger fileIndex)
    {
        if (LakehouseUtil.deltaLakeParent(root, path).isPresent()) {
            return sampleFile(new ProcessorPath(path, Optional.empty()));
        }
        int fIndex = fileIndex.getAndIncrement();
        if ((fIndex == 0) || ((fIndex % generalOptions.sampleFilesPerTableModulo()) == 0)) {    // always sample the first file in case there aren't enough
            int includedFilesCount = fIndex / generalOptions.sampleFilesPerTableModulo();
            if (includedFilesCount < generalOptions.maxSampleFilesPerTable()) {
                return sampleFile(new ProcessorPath(lakehouseFormat.map(LakehouseFormat::path).orElse(path), lakehouseFormat));
            }
            else {
                return enoughSamples();
            }
        }
        return notEnoughSamples();
    }
}
