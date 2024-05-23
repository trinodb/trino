/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.util.MergingPageIterator;
import io.trino.plugin.hive.util.SortBuffer;
import io.trino.plugin.hive.util.TempFileReader;
import io.trino.plugin.hive.util.TempFileWriter;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public final class SortingFileWriter
        implements FileWriter
{
    private static final Logger log = Logger.get(SortingFileWriter.class);

    private static final int INSTANCE_SIZE = instanceSize(SortingFileWriter.class);

    private final TrinoFileSystem fileSystem;
    private final Location tempFilePrefix;
    private final int maxOpenTempFiles;
    private final List<Type> types;
    private final List<Type> sortTypes;
    private final List<Integer> sortFields;
    private final List<SortOrder> sortOrders;
    private final FileWriter outputWriter;
    private final SortBuffer sortBuffer;
    private final Queue<TempFile> tempFiles = new PriorityQueue<>(comparing(TempFile::size));
    private final AtomicLong nextFileId = new AtomicLong();
    private final TypeOperators typeOperators;
    private final List<List<Integer>> nestedSortPaths;
    private final int originalChannelCount;
    private final int[] keepChannels;

    private boolean flushed;
    private long tempFilesWrittenBytes;

    public SortingFileWriter(
            TrinoFileSystem fileSystem,
            Location tempFilePrefix,
            FileWriter outputWriter,
            DataSize maxMemory,
            int maxOpenTempFiles,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            PageSorter pageSorter,
            TypeOperators typeOperators,
            List<List<Integer>> nestedSortPaths)
    {
        checkArgument(maxOpenTempFiles >= 2, "maxOpenTempFiles must be at least two");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.tempFilePrefix = requireNonNull(tempFilePrefix, "tempFilePrefix is null");
        this.maxOpenTempFiles = maxOpenTempFiles;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.sortFields = ImmutableList.copyOf(requireNonNull(sortFields, "sortFields is null"));
        this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        this.outputWriter = requireNonNull(outputWriter, "outputWriter is null");
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.nestedSortPaths = ImmutableList.copyOf(nestedSortPaths);
        this.originalChannelCount = types.size();
        this.keepChannels = nestedSortPaths.isEmpty() ? new int[0] : IntStream.range(0, types.size()).toArray();

        ImmutableList.Builder<Type> sortTypesBuilder = ImmutableList.builder();
        sortTypesBuilder.addAll(types);
        for (List<Integer> path : nestedSortPaths) {
            Type type = types.get(path.getFirst());
            for (int i = 1; i < path.size(); i++) {
                type = type.getTypeParameters().get(path.get(i));
            }
            sortTypesBuilder.add(type);
        }
        this.sortTypes = sortTypesBuilder.build();

        this.sortBuffer = new SortBuffer(maxMemory, sortTypes, sortFields, sortOrders, pageSorter);
    }

    @Override
    public long getWrittenBytes()
    {
        if (flushed) {
            return outputWriter.getWrittenBytes();
        }

        // This is an approximation, since the outputWriter is not used until this write is committed.
        // Returning an approximation is important as the value is used by the PageSink to split files
        // into a reasonable size.
        return tempFilesWrittenBytes;
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE + sortBuffer.getRetainedBytes() + outputWriter.getMemoryUsage();
    }

    @Override
    public void appendRows(Page page)
    {
        long retainedSizeInBytes = page.getRetainedSizeInBytes();
        Page sortPage = expandPage(page);
        if (!sortBuffer.canAdd(sortPage)) {
            flushToTempFile();
        }
        sortBuffer.add(sortPage, retainedSizeInBytes);
    }

    @Override
    public RollbackAction commit()
    {
        flushed = true;

        RollbackAction rollbackAction = createRollbackAction(fileSystem, tempFiles);
        if (!sortBuffer.isEmpty()) {
            // skip temporary files entirely if the total output size is small
            if (tempFiles.isEmpty()) {
                sortBuffer.flushTo(page -> outputWriter.appendRows(stripPage(page)));
                outputWriter.commit();
                return rollbackAction;
            }

            flushToTempFile();
        }

        try {
            writeSorted();
            outputWriter.commit();
        }
        catch (UncheckedIOException e) {
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }

        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        RollbackAction rollbackAction = createRollbackAction(fileSystem, tempFiles);
        try (Closer closer = Closer.create()) {
            closer.register(outputWriter::rollback);
            closer.register(rollbackAction::run);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static RollbackAction createRollbackAction(TrinoFileSystem fileSystem, Queue<TempFile> tempFiles)
    {
        return () -> {
            for (TempFile file : tempFiles) {
                cleanupFile(fileSystem, file.location());
            }
        };
    }

    @Override
    public long getValidationCpuNanos()
    {
        return outputWriter.getValidationCpuNanos();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tempFilePrefix", tempFilePrefix)
                .add("outputWriter", outputWriter)
                .toString();
    }

    private void flushToTempFile()
    {
        writeTempFile(writer -> sortBuffer.flushTo(page -> writer.writePage(stripPage(page))));
    }

    // TODO: change connector SPI to make this resumable and have memory tracking
    private void writeSorted()
    {
        combineFiles();

        mergeFiles(tempFiles, outputWriter::appendRows);
    }

    private void combineFiles()
    {
        while (tempFiles.size() > maxOpenTempFiles) {
            int count = min(maxOpenTempFiles, tempFiles.size() - (maxOpenTempFiles - 1));

            List<TempFile> smallestFiles = IntStream.range(0, count)
                    .mapToObj(_ -> tempFiles.poll())
                    .collect(toImmutableList());

            writeTempFile(writer -> mergeFiles(smallestFiles, writer::writePage));
        }
    }

    private void mergeFiles(Iterable<TempFile> files, Consumer<Page> consumer)
    {
        try (Closer closer = Closer.create()) {
            Collection<Iterator<Page>> iterators = new ArrayList<>();

            for (TempFile tempFile : files) {
                TempFileReader reader = new TempFileReader(types, fileSystem, tempFile.location());
                closer.register(reader);
                iterators.add(Iterators.transform(reader, this::expandPage));
            }

            new MergingPageIterator(iterators, sortTypes, sortFields, sortOrders, typeOperators)
                    .forEachRemaining(page -> consumer.accept(stripPage(page)));

            for (TempFile tempFile : files) {
                fileSystem.deleteFile(tempFile.location());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeTempFile(Consumer<TempFileWriter> consumer)
    {
        Location tempFile = getTempFileName();

        try (TempFileWriter writer = new TempFileWriter(types, fileSystem, tempFile)) {
            consumer.accept(writer);
            writer.close();
            tempFiles.add(new TempFile(tempFile, writer.getWrittenBytes()));
            tempFilesWrittenBytes += writer.getWrittenBytes();
        }
        catch (IOException | UncheckedIOException e) {
            cleanupFile(fileSystem, tempFile);
            throw new TrinoException(HIVE_WRITER_DATA_ERROR, "Failed to write temporary file: " + tempFile, e);
        }
    }

    private Page expandPage(Page page)
    {
        if (nestedSortPaths.isEmpty()) {
            return page;
        }
        Block[] blocks = new Block[originalChannelCount + nestedSortPaths.size()];
        for (int i = 0; i < originalChannelCount; i++) {
            blocks[i] = page.getBlock(i);
        }
        for (int i = 0; i < nestedSortPaths.size(); i++) {
            List<Integer> path = nestedSortPaths.get(i);
            Block block = page.getBlock(path.getFirst());
            for (int depth = 1; depth < path.size(); depth++) {
                block = getRowFieldsFromBlock(block).get(path.get(depth));
            }
            blocks[originalChannelCount + i] = block;
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private Page stripPage(Page page)
    {
        if (nestedSortPaths.isEmpty()) {
            return page;
        }
        return page.getColumns(keepChannels);
    }

    private static void cleanupFile(TrinoFileSystem fileSystem, Location location)
    {
        try {
            fileSystem.deleteFile(location);
        }
        catch (IOException e) {
            log.warn(e, "Failed to delete temporary file: %s", location);
        }
    }

    private Location getTempFileName()
    {
        return Location.of(tempFilePrefix + "." + nextFileId.getAndIncrement());
    }

    private record TempFile(Location location, long size)
    {
        public TempFile
        {
            checkArgument(size >= 0, "size is negative");
            requireNonNull(location, "location is null");
        }
    }
}
