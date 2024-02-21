/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.starburst.schema.discovery.formats.lakehouse.LakehouseFormat;
import io.starburst.schema.discovery.formats.lakehouse.LakehouseUtil;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.options.DiscoveryMode;
import io.starburst.schema.discovery.options.GeneralOptions;
import io.starburst.schema.discovery.options.OptionsMap;
import io.starburst.schema.discovery.processor.Processor.ProcessorPath;
import io.starburst.schema.discovery.processor.filetracker.FileTracker;
import io.starburst.schema.discovery.processor.filetracker.FileTracker.SampleFileResult;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.submit;
import static com.google.common.util.concurrent.Futures.submitAsync;
import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.LOCATION_DOES_NOT_EXISTS;
import static io.starburst.schema.discovery.infer.InferPartitions.PARTITION_SEPARATOR;
import static io.starburst.schema.discovery.io.LocationUtils.directoryOrFileName;
import static java.util.Objects.requireNonNull;

public class SampleFilesCrawler
{
    private final FileTracker fileTracker;
    private final DiscoveryTrinoFileSystem fileSystem;
    private final Predicate<Location> filter;
    private final Executor executor;
    private final Location root;
    private final DiscoveryMode discoveryMode;

    public SampleFilesCrawler(DiscoveryTrinoFileSystem fileSystem, Location root, OptionsMap options, Executor executor, FileTracker fileTracker)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.root = requireNonNull(root, "root is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.filter = new DiscoveryFilter(options);
        this.fileTracker = fileTracker;
        GeneralOptions generalOptions = new GeneralOptions(options);
        this.discoveryMode = generalOptions.discoveryMode();
    }

    public ListenableFuture<List<ProcessorPath>> startBuildSampleFilesListAsync()
    {
        throwIfInvalidFileStatus(root);
        return FluentFuture.from(Futures.allAsList(submitAsync(this::crawlRootDirectory, executor)))
                .transform(results -> getProcessorPathStream(root, results), executor);
    }

    private ListenableFuture<List<ProcessorPath>> crawlRootDirectory()
    {
        if (discoveryMode == DiscoveryMode.RECURSIVE_DIRECTORIES) {
            return crawlRecursiveDirectories();
        }
        else {
            return crawlDirectoryAsync(root);
        }
    }

    private ListenableFuture<List<ProcessorPath>> crawlRecursiveDirectories()
    {
        ImmutableList.Builder<ListenableFuture<List<ProcessorPath>>> crawlRecursiveTables = ImmutableList.builder();

        fileSystem.listDirectories(root).forEach(directory ->
                crawlRecursiveTables.add(submit(() -> crawlDirectorySync(directory), executor)));

        return FluentFuture.from(Futures.allAsList(crawlRecursiveTables.build()))
                .transform(results -> getProcessorPathStream(root, results), executor);
    }

    public List<ProcessorPath> crawlDirectorySync(Location directory)
    {
        ImmutableList.Builder<ProcessorPath> sampleFiles = ImmutableList.builder();

        createFileEntryStream(directory)
                .takeWhile(ignore -> !fileTracker.hasEnoughSampledTables(directory))
                .filter(file -> file.length() > 0 && filter.test(file.location()))
                .map(file -> getNextValidSampleFile(file.location()))
                .takeWhile(sampleFileResult -> !sampleFileResult.hasEnoughSamples())
                .flatMap(sampleFileResult -> sampleFileResult.filePath().stream())
                .forEach(sampleFiles::add);

        return LakehouseUtil.applyDeltaLakeFormatMatch(root, sampleFiles.build());
    }

    private ListenableFuture<List<ProcessorPath>> crawlDirectoryAsync(Location directory)
    {
        if (fileTracker.hasEnoughSampledTables(directory)) {
            return Futures.immediateFuture(ImmutableList.of());
        }

        ImmutableList.Builder<ProcessorPath> sampleFiles = ImmutableList.builder();
        ImmutableList.Builder<ListenableFuture<List<ProcessorPath>>> crawlNestedDirectories = ImmutableList.builder();

        Set<Location> directories = fileSystem.listDirectories(directory);
        for (Location dir : directories) {
            if (directoryOrFileName(dir).contains(PARTITION_SEPARATOR)) {
                crawlNestedDirectories.add(submit(() -> crawlDirectorySync(dir), executor));
            }
            else {
                crawlNestedDirectories.add(submitAsync(() -> crawlDirectoryAsync(dir), executor));
            }
        }

        // no directories in this directory, so proceed scanning files
        if (directories.isEmpty()) {
            createFileEntryStream(directory)
                    .filter(file -> file.length() > 0 && filter.test(file.location()))
                    .map(file -> getNextValidSampleFile(file.location()))
                    .takeWhile(sampleFileResult -> !sampleFileResult.hasEnoughSamples())
                    .flatMap(sampleFileResult -> sampleFileResult.filePath().stream())
                    .forEach(sampleFiles::add);
        }

        return FluentFuture.from(Futures.allAsList(crawlNestedDirectories.add(immediateFuture(sampleFiles.build())).build()))
                .transform(results -> getProcessorPathStream(root, results), executor);
    }

    private Stream<FileEntry> createFileEntryStream(Location directory)
    {
        FileIterator fileIterator = fileSystem.listFiles(directory);
        return Streams.stream(new Iterator<>()
        {
            @Override
            public boolean hasNext()
            {
                try {
                    return fileIterator.hasNext();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public FileEntry next()
            {
                try {
                    return fileIterator.next();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    private SampleFileResult getNextValidSampleFile(Location path)
    {
        Optional<LakehouseFormat> lakehouseFormat = LakehouseUtil.checkIcebergFormatMatch(path);
        Location potentialTablePath = lakehouseFormat.map(LakehouseFormat::path).orElseGet(path::parentDirectory);
        if (fileTracker.shouldStopAddingTables(potentialTablePath)) {
            return SampleFileResult.enoughSamples();
        }
        return fileTracker.getNextSampleFileForTable(potentialTablePath, path, lakehouseFormat);
    }

    private void throwIfInvalidFileStatus(Location root)
    {
        if (!fileSystem.directoryExists(root)) {
            throw new TrinoException(LOCATION_DOES_NOT_EXISTS, "Root directory is empty or isn't a directory: " + root);
        }
    }

    private static List<ProcessorPath> getProcessorPathStream(Location root, List<List<ProcessorPath>> results)
    {
        return results.stream().flatMap(files -> LakehouseUtil.applyDeltaLakeFormatMatch(root, files).stream()).collect(toImmutableList());
    }
}
