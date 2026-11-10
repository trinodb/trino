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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

public final class PositionDeleteReader
{
    private PositionDeleteReader() {}

    /**
     * Starts reading the position delete files that apply to the split and returns a future of the combined deletion vector.
     * Each file is opened on {@code openExecutor}, and decoded and merged on {@code decodeExecutor}.
     */
    public static ListenableFuture<Optional<DeletionVector>> readPositionDeletes(
            String dataFilePath,
            List<DeleteFile> positionDeleteFiles,
            OptionalLong startRowPosition,
            OptionalLong endRowPosition,
            DeletePageSourceProvider deletePageSourceProvider,
            TypeManager typeManager,
            Executor openExecutor,
            Executor decodeExecutor,
            MemoryContext memoryContext)
    {
        if (positionDeleteFiles.isEmpty()) {
            return immediateFuture(Optional.empty());
        }

        Slice targetPath = utf8Slice(dataFilePath);

        verify(startRowPosition.isPresent() == endRowPosition.isPresent(), "startRowPosition and endRowPosition must be specified together");
        IcebergColumnHandle deleteFilePath = getColumnHandle(DELETE_FILE_PATH, typeManager);
        IcebergColumnHandle deleteFilePos = getColumnHandle(DELETE_FILE_POS, typeManager);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));
        if (startRowPosition.isPresent()) {
            Range positionRange = Range.range(deleteFilePos.getType(), startRowPosition.orElseThrow(), true, endRowPosition.orElseThrow(), true);
            TupleDomain<IcebergColumnHandle> positionDomain = TupleDomain.withColumnDomains(ImmutableMap.of(deleteFilePos, Domain.create(ValueSet.ofRanges(positionRange), false)));
            deleteDomain = deleteDomain.intersect(positionDomain);
        }
        TupleDomain<IcebergColumnHandle> effectiveDeleteDomain = deleteDomain;

        DeletionVector.Builder deletionVector = DeletionVector.builder();
        ReferenceCountedLoader<ConnectorPageSource> loader = new ReferenceCountedLoader<>();
        List<DeleteFile> filesToLoad = positionDeleteFiles.stream()
                .filter(deleteFile -> shouldLoadPositionDeleteFile(deleteFile, startRowPosition, endRowPosition))
                .collect(toImmutableList());
        ListenableFuture<Void> loads = BoundedLoader.loadAll(
                filesToLoad,
                deleteFile -> loader.load(
                        deleteFile.path(),
                        openExecutor,
                        () -> deletePageSourceProvider.openDeletes(deleteFile, deleteColumns, effectiveDeleteDomain),
                        decodeExecutor,
                        pageSource -> {
                            // decodes run one at a time on the thread reading the split, so they add to the vector directly
                            readMultiFilePositionDeletes(pageSource, targetPath, deletionVector::add);
                            // report after each file so that concurrent loads get memory pool pushback
                            // before the whole set is read
                            memoryContext.setBytes(deletionVector.retainedSizeInBytes());
                        }));
        return Futures.transform(loads, _ -> deletionVector.build(), directExecutor());
    }

    private static boolean shouldLoadPositionDeleteFile(DeleteFile deleteFile, OptionalLong startRowPosition, OptionalLong endRowPosition)
    {
        if (startRowPosition.isEmpty()) {
            return true;
        }

        OptionalLong positionLowerBound = deleteFile.rowPositionLowerBound();
        OptionalLong positionUpperBound = deleteFile.rowPositionUpperBound();
        return (positionLowerBound.isEmpty() || positionLowerBound.orElseThrow() <= endRowPosition.orElseThrow()) &&
                (positionUpperBound.isEmpty() || positionUpperBound.orElseThrow() >= startRowPosition.orElseThrow());
    }

    public static void readSingleFilePositionDeletes(ConnectorPageSource pageSource, LongConsumer filePositionConsumer)
    {
        while (!pageSource.isFinished()) {
            getFutureValue(pageSource.isBlocked());
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                continue;
            }

            Block block = page.getBlock(0);

            for (int position = 0; position < page.getPositionCount(); position++) {
                filePositionConsumer.accept(BIGINT.getLong(block, position));
            }
        }
    }

    private static void readMultiFilePositionDeletes(ConnectorPageSource pageSource, Slice targetPath, LongConsumer filePositionConsumer)
    {
        // Use a linear search since we expect most deletion files to only contain
        // entries for a single path. The comparison cost is minimal if the
        // path values are dictionary encoded, since we only do the comparison once.
        while (!pageSource.isFinished()) {
            getFutureValue(pageSource.isBlocked());
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                continue;
            }

            Block pathBlock = page.getBlock(0);
            Block block = page.getBlock(1);

            // TODO: add specialized code for RLE and dictionary blocks
            int lastPathPosition = -1;
            int result = 0;
            for (int position = 0; position < page.getPositionCount(); position++) {
                // The expected case is a dictionary block with many entries for the
                // same path. Only perform a comparison if the path has changed.
                int pathPosition = pathBlock.getUnderlyingValuePosition(position);
                if (lastPathPosition != pathPosition) {
                    checkArgument(!pathBlock.isNull(position), "position is null");
                    lastPathPosition = pathPosition;
                    result = VARCHAR.getSlice(pathBlock, position).compareTo(targetPath);
                }
                if (result > 0) {
                    // deletion files are sorted by path, so we're done
                    return;
                }
                if (result == 0) {
                    filePositionConsumer.accept(BIGINT.getLong(block, position));
                }
            }
        }
    }

    public static void readMultiFilePositionDeletes(ConnectorPageSource pageSource, BiConsumer<String, Long> filePositionConsumer)
    {
        while (!pageSource.isFinished()) {
            getFutureValue(pageSource.isBlocked());
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                continue;
            }

            Block pathBlock = page.getBlock(0);
            Block posBlock = page.getBlock(1);

            int lastPathPosition = -1;
            String pathString = null;
            for (int position = 0; position < page.getPositionCount(); position++) {
                // We expect that paths are dictionary or RLE encoded, and thus the delete
                // positions for a data file share the same underlying value position,
                // so this caches the conversion to string.
                int pathPosition = pathBlock.getUnderlyingValuePosition(position);
                if (lastPathPosition != pathPosition) {
                    lastPathPosition = pathPosition;
                    pathString = VARCHAR.getSlice(pathBlock, position).toStringUtf8();
                }
                filePositionConsumer.accept(pathString, BIGINT.getLong(posBlock, position));
            }
        }
    }
}
