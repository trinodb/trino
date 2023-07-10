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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.collect.Maps;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.orc.FileOrcDataSource;
import io.trino.orc.OrcPredicate;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.orc.OrcWriter;
import io.trino.plugin.raptor.legacy.metadata.ColumnInfo;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcReader.createOrcReader;
import static io.trino.plugin.raptor.legacy.storage.OrcFileWriter.createOrcFileWriter;
import static io.trino.plugin.raptor.legacy.storage.RaptorStorageManager.getColumnInfo;
import static java.lang.Math.toIntExact;

public final class OrcFileRewriter
{
    private static final Logger log = Logger.get(OrcFileRewriter.class);

    private OrcFileRewriter() {}

    public static OrcFileInfo rewrite(TypeManager typeManager, File input, File output, BitSet rowsToDelete)
            throws IOException
    {
        OrcReaderOptions options = new OrcReaderOptions();
        OrcReader reader = createOrcReader(new FileOrcDataSource(input, options), options)
                .orElseThrow(() -> new IOException("File is empty: " + input));
        return rewrite(typeManager, reader, output, rowsToDelete);
    }

    public static OrcFileInfo rewrite(TypeManager typeManager, OrcReader reader, File output, BitSet rowsToDelete)
            throws IOException
    {
        long start = System.nanoTime();

        List<ColumnInfo> columnInfo = getColumnInfo(typeManager, reader);

        List<String> columnNames = columnInfo.stream()
                .map(info -> String.valueOf(info.getColumnId()))
                .collect(toImmutableList());

        List<Type> columnTypes = columnInfo.stream()
                .map(ColumnInfo::getType)
                .collect(toImmutableList());

        OrcRecordReader recordReader = reader.createRecordReader(
                reader.getRootColumn().getNestedColumns(),
                columnTypes,
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RaptorPageSource::handleException);

        long fileRowCount = recordReader.getFileRowCount();
        if (fileRowCount < rowsToDelete.length()) {
            throw new IOException("File has fewer rows than deletion vector");
        }
        int deleteRowCount = rowsToDelete.cardinality();
        if (fileRowCount == deleteRowCount) {
            return new OrcFileInfo(0, 0);
        }
        if (fileRowCount >= Integer.MAX_VALUE) {
            throw new IOException("File has too many rows");
        }

        Map<String, String> metadata = Maps.transformValues(reader.getFooter().getUserMetadata(), Slice::toStringUtf8);

        OrcFileInfo fileInfo;
        try (OrcWriter writer = createOrcFileWriter(output, columnNames, columnTypes, reader.getFooter().getTypes(), metadata)) {
            fileInfo = rewrite(recordReader, writer, rowsToDelete);
        }
        log.debug("Rewrote file in %s (input rows: %s, output rows: %s)", nanosSince(start), fileRowCount, fileRowCount - deleteRowCount);
        return fileInfo;
    }

    private static OrcFileInfo rewrite(OrcRecordReader reader, OrcWriter writer, BitSet rowsToDelete)
            throws IOException
    {
        long rowCount = 0;
        long uncompressedSize = 0;

        while (true) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedIOException();
            }

            Page page = reader.nextPage();
            if (page == null) {
                break;
            }

            int start = toIntExact(reader.getFilePosition());
            page = maskedPage(page, rowsToDelete, start);
            writer.write(page);

            rowCount += page.getPositionCount();
            uncompressedSize += page.getLogicalSizeInBytes();
        }

        return new OrcFileInfo(rowCount, uncompressedSize);
    }

    private static Page maskedPage(Page page, BitSet rowsToDelete, int start)
    {
        int end = start + page.getPositionCount();
        if (rowsToDelete.nextSetBit(start) >= end) {
            return page;
        }
        if (rowsToDelete.nextClearBit(start) >= end) {
            return page.copyPositions(new int[0], 0, 0);
        }

        int[] ids = new int[page.getPositionCount()];
        int size = 0;
        for (int i = 0; i < ids.length; i++) {
            if (!rowsToDelete.get(start + i)) {
                ids[size] = i;
                size++;
            }
        }

        Block[] maskedBlocks = new Block[page.getChannelCount()];
        for (int i = 0; i < maskedBlocks.length; i++) {
            maskedBlocks[i] = DictionaryBlock.create(size, page.getBlock(i), ids);
        }
        return new Page(maskedBlocks);
    }

    public static class OrcFileInfo
    {
        private final long rowCount;
        private final long uncompressedSize;

        public OrcFileInfo(long rowCount, long uncompressedSize)
        {
            this.rowCount = rowCount;
            this.uncompressedSize = uncompressedSize;
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public long getUncompressedSize()
        {
            return uncompressedSize;
        }
    }
}
