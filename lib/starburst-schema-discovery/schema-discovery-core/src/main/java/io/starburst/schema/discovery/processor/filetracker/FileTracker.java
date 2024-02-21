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
import io.starburst.schema.discovery.processor.Processor.ProcessorPath;
import io.trino.filesystem.Location;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface FileTracker
{
    boolean shouldStopAddingTables(Location potentialTablePath);

    boolean hasEnoughSampledTables(Location directoryPath);

    SampleFileResult getNextSampleFileForTable(Location directoryPath, Location filePath, Optional<LakehouseFormat> lakehouseFormat);

    record SampleFileResult(boolean hasEnoughSamples, Optional<ProcessorPath> filePath)
    {
        public SampleFileResult
        {
            requireNonNull(filePath, "filePath is null");
        }

        public static SampleFileResult enoughSamples()
        {
            return new SampleFileResult(true, Optional.empty());
        }

        static SampleFileResult notEnoughSamples()
        {
            return new SampleFileResult(false, Optional.empty());
        }

        static SampleFileResult sampleFile(ProcessorPath sampleFilePath)
        {
            return new SampleFileResult(false, Optional.of(sampleFilePath));
        }
    }
}
