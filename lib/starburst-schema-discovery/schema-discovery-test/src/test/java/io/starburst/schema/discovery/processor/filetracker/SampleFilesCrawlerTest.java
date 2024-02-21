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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.starburst.schema.discovery.Util;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor.ProcessorPath;
import io.starburst.schema.discovery.processor.SampleFilesCrawler;
import io.trino.filesystem.Location;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.assertj.core.api.Assertions.assertThat;

public class SampleFilesCrawlerTest
{
    @Test
    public void testRecursiveShouldBreakAfterScanningEnoughFilesForTable()
            throws Exception
    {
        Location recursiveDirectory = Util.testFilePath("recursive");
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "1", GeneralOptions.DISCOVERY_MODE, "recursive_directories"));
        RecordingFileTrackerDecorator recordingTracker = new RecordingFileTrackerDecorator(new RecursiveDirectoriesFileTracker(recursiveDirectory, new GeneralOptions(optionsMap)));

        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(Util.fileSystem(), recursiveDirectory, optionsMap, directExecutor(), recordingTracker);
        List<ProcessorPath> processorPaths = sampleFilesCrawler.startBuildSampleFilesListAsync().get(5, TimeUnit.SECONDS);
        Set<Location> sampleFilePaths = processorPaths.stream().map(ProcessorPath::path).collect(toImmutableSet());
        assertThat(sampleFilePaths).hasSize(2); // samples should be 1 per table as specified in options
        // tracker should record 2 per table, 1st is sample, 2nd is file that breaks out of discovery
        assertThat(recordingTracker.getRecordedPaths()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(
                recursiveDirectory.appendSuffix("/rtable1/"),
                recursiveDirectory.appendSuffix("/rtable1/March-2023/simple2023.csv"),
                recursiveDirectory.appendSuffix("/rtable1/March-2024/simple2024.csv"),
                recursiveDirectory.appendSuffix("/rtable2/"),
                recursiveDirectory.appendSuffix("/rtable2/April-2023/simple2023.csv"),
                recursiveDirectory.appendSuffix("/rtable2/April-2024/simple2024.csv")));
    }

    @Test
    public void testPartitionedShouldBreakWhenScanningEnoughFilesForPartition()
            throws Exception
    {
        Location partitionedDirectory = Util.testFilePath("csv/partitioned");
        OptionsMap optionsMap = new OptionsMap(ImmutableMap.of(GeneralOptions.SAMPLE_FILES_PER_TABLE_MODULO, "1", GeneralOptions.MAX_SAMPLE_FILES_PER_TABLE, "1"));
        RecordingFileTrackerDecorator recordingTracker = new RecordingFileTrackerDecorator(new PartitionedDiscoveryFileTracker(partitionedDirectory, new GeneralOptions(optionsMap)));

        SampleFilesCrawler sampleFilesCrawler = new SampleFilesCrawler(Util.fileSystem(), partitionedDirectory, optionsMap, directExecutor(), recordingTracker);
        List<ProcessorPath> processorPaths = sampleFilesCrawler.startBuildSampleFilesListAsync().get(5, TimeUnit.SECONDS);
        Set<Location> sampleFilePaths = processorPaths.stream().map(ProcessorPath::path).collect(toImmutableSet());
        assertThat(sampleFilePaths).hasSize(2); // samples should be 1 per table(partition) as specified in options
        // tracker should record 2 per table(partition), 1st is sample, 2nd is file that breaks out of discovery
        assertThat(recordingTracker.getRecordedPaths()).containsExactlyInAnyOrderElementsOf(ImmutableSet.of(
                partitionedDirectory,
                partitionedDirectory.appendSuffix("/ds=2012-12-29/"),
                partitionedDirectory.appendSuffix("/ds=2012-12-29/000000_0"),
                partitionedDirectory.appendSuffix("/ds=2012-12-29/000000_1"),
                partitionedDirectory.appendSuffix("/ds=2012-12-30/"),
                partitionedDirectory.appendSuffix("/ds=2012-12-30/000000_0"),
                partitionedDirectory.appendSuffix("/ds=2012-12-30/000000_1")));
    }
}
