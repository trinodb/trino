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
import io.airlift.slice.Slice;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.BiConsumer;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;

public final class PositionDeleteReader
{
    private PositionDeleteReader() {}

    public static Optional<DeletionVector> readPositionDeletes(
            String dataFilePath,
            List<DeleteFile> positionDeleteFiles,
            OptionalLong startRowPosition,
            OptionalLong endRowPosition,
            DeletePageSourceProvider deletePageSourceProvider,
            TypeManager typeManager)
    {
        if (positionDeleteFiles.isEmpty()) {
            return Optional.empty();
        }

        Slice targetPath = utf8Slice(dataFilePath);

        verify(startRowPosition.isPresent() == endRowPosition.isPresent(), "startRowPosition and endRowPosition must be specified together");
        IcebergColumnHandle deleteFilePath = getColumnHandle(DELETE_FILE_PATH, typeManager);
        IcebergColumnHandle deleteFilePos = getColumnHandle(DELETE_FILE_POS, typeManager);
        List<IcebergColumnHandle> deleteColumns = ImmutableList.of(deleteFilePath, deleteFilePos);
        TupleDomain<IcebergColumnHandle> deleteDomain = TupleDomain.fromFixedValues(ImmutableMap.of(deleteFilePath, NullableValue.of(VARCHAR, targetPath)));
        if (startRowPosition.isPresent()) {
            Range positionRange = Range.range(deleteFilePos.getType(), startRowPosition.getAsLong(), true, endRowPosition.getAsLong(), true);
            TupleDomain<IcebergColumnHandle> positionDomain = TupleDomain.withColumnDomains(ImmutableMap.of(deleteFilePos, Domain.create(ValueSet.ofRanges(positionRange), false)));
            deleteDomain = deleteDomain.intersect(positionDomain);
        }

        DeletionVector.Builder deletionVector = DeletionVector.builder();
        for (DeleteFile deleteFile : positionDeleteFiles) {
            if (shouldLoadPositionDeleteFile(deleteFile, startRowPosition, endRowPosition)) {
                try (ConnectorPageSource pageSource = deletePageSourceProvider.openDeletes(deleteFile, deleteColumns, deleteDomain)) {
                    readMultiFilePositionDeletes(pageSource, targetPath, deletionVector::add);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        return deletionVector.build();
    }

    private static boolean shouldLoadPositionDeleteFile(DeleteFile deleteFile, OptionalLong startRowPosition, OptionalLong endRowPosition)
    {
        if (startRowPosition.isEmpty()) {
            return true;
        }

        OptionalLong positionLowerBound = deleteFile.rowPositionLowerBound();
        OptionalLong positionUpperBound = deleteFile.rowPositionUpperBound();
        return (positionLowerBound.isEmpty() || positionLowerBound.getAsLong() <= endRowPosition.orElseThrow()) &&
                (positionUpperBound.isEmpty() || positionUpperBound.getAsLong() >= startRowPosition.getAsLong());
    }

    public static void readSingleFilePositionDeletes(ConnectorPageSource pageSource, LongConsumer filePositionConsumer)
    {
        while (!pageSource.isFinished()) {
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
        CachingVarcharComparator comparator = new CachingVarcharComparator(targetPath);
        while (!pageSource.isFinished()) {
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                continue;
            }

            Block pathBlock = page.getBlock(0);
            Block block = page.getBlock(1);

            // TODO: add specialized code for RLE and dictionary blocks
            for (int position = 0; position < page.getPositionCount(); position++) {
                int result = comparator.compare(pathBlock, position);
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
            SourcePage page = pageSource.getNextSourcePage();
            if (page == null) {
                continue;
            }

            Block pathBlock = page.getBlock(0);
            Block posBlock = page.getBlock(1);

            Slice lastPath = null;
            String pathString = null;
            for (int position = 0; position < page.getPositionCount(); position++) {
                Slice path = VARCHAR.getSlice(pathBlock, position);
                // We expect that paths are dictionary or RLE encoded, and thus getSlice()
                // will return the same object for each delete position for a data file,
                // so this caches the conversion to string.
                if (lastPath != path) {
                    lastPath = path;
                    pathString = path.toStringUtf8();
                }
                filePositionConsumer.accept(pathString, BIGINT.getLong(posBlock, position));
            }
        }
    }

    private static final class CachingVarcharComparator
    {
        private final Slice reference;
        private int result;
        private Slice value;

        public CachingVarcharComparator(Slice reference)
        {
            this.reference = requireNonNull(reference, "reference is null");
        }

        @SuppressWarnings({"ObjectEquality", "ReferenceEquality"})
        public int compare(Block block, int position)
        {
            checkArgument(!block.isNull(position), "position is null");
            Slice next = VARCHAR.getSlice(block, position);
            return compare(next);
        }

        public int compare(Slice next)
        {
            // The expected case is a dictionary block with many entries for the
            // same path. Only perform a comparison if the object has changed.
            if (value != next) {
                value = next;
                result = value.compareTo(reference);
            }
            return result;
        }
    }
}
