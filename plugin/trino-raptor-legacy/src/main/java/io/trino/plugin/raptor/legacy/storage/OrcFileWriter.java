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

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.trino.orc.OrcDataSink;
import io.trino.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.trino.orc.OrcWriter;
import io.trino.orc.OrcWriterOptions;
import io.trino.orc.OrcWriterStats;
import io.trino.orc.OutputStreamOrcDataSink;
import io.trino.orc.metadata.ColumnMetadata;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.OrcType;
import io.trino.plugin.raptor.legacy.util.SyncingFileOutputStream;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.orc.metadata.OrcType.createRootOrcType;
import static io.trino.plugin.raptor.legacy.RaptorErrorCode.RAPTOR_ERROR;
import static io.trino.plugin.raptor.legacy.storage.RaptorStorageManager.toOrcFileType;

public class OrcFileWriter
        implements Closeable
{
    private static final JsonCodec<OrcFileMetadata> METADATA_CODEC = jsonCodec(OrcFileMetadata.class);

    private final PageBuilder pageBuilder;
    private final OrcWriter orcWriter;

    private boolean closed;
    private long rowCount;
    private long uncompressedSize;

    public OrcFileWriter(TypeManager typeManager, List<Long> columnIds, List<Type> columnTypes, File target)
    {
        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        List<String> columnNames = columnIds.stream()
                .map(Objects::toString)
                .collect(toImmutableList());

        columnTypes = columnTypes.stream()
                .map(type -> toOrcFileType(type, typeManager))
                .collect(toImmutableList());

        this.pageBuilder = new PageBuilder(columnTypes);

        ColumnMetadata<OrcType> orcTypes = createRootOrcType(columnNames, columnTypes);
        Map<String, String> metadata = createFileMetadata(columnIds, columnTypes);

        this.orcWriter = createOrcFileWriter(target, columnNames, columnTypes, orcTypes, metadata);
    }

    public void appendPages(List<Page> pages)
    {
        for (Page page : pages) {
            appendPage(page);
        }
    }

    public void appendPages(List<Page> pages, int[] pageIndexes, int[] positionIndexes)
    {
        checkArgument(pageIndexes.length == positionIndexes.length, "pageIndexes and positionIndexes do not match");

        for (int i = 0; i < pageIndexes.length; i++) {
            Page page = pages.get(pageIndexes[i]);
            int position = positionIndexes[i];

            pageBuilder.declarePosition();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                BlockBuilder output = pageBuilder.getBlockBuilder(channel);
                pageBuilder.getType(channel).appendTo(block, position, output);
            }

            if (pageBuilder.isFull()) {
                appendPage(pageBuilder.build());
                pageBuilder.reset();
            }
        }

        if (!pageBuilder.isEmpty()) {
            appendPage(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    private void appendPage(Page page)
    {
        rowCount += page.getPositionCount();
        uncompressedSize += page.getLogicalSizeInBytes();

        try {
            orcWriter.write(page);
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to write data", e);
        }
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getUncompressedSize()
    {
        return uncompressedSize;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            orcWriter.close();
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to close writer", e);
        }
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return new HashSet<>(items).size() == items.size();
    }

    private static Map<String, String> createFileMetadata(List<Long> columnIds, List<Type> columnTypes)
    {
        ImmutableMap.Builder<Long, TypeId> columnTypesMap = ImmutableMap.builder();
        for (int i = 0; i < columnIds.size(); i++) {
            columnTypesMap.put(columnIds.get(i), columnTypes.get(i).getTypeId());
        }
        OrcFileMetadata metadata = new OrcFileMetadata(columnTypesMap.buildOrThrow());
        return ImmutableMap.of(OrcFileMetadata.KEY, METADATA_CODEC.toJson(metadata));
    }

    private static OrcDataSink createOrcDataSink(File target)
    {
        try {
            return OutputStreamOrcDataSink.create(new SyncingFileOutputStream(target));
        }
        catch (IOException e) {
            throw new TrinoException(RAPTOR_ERROR, "Failed to open output file: " + target, e);
        }
    }

    public static OrcWriter createOrcFileWriter(
            File target,
            List<String> columnNames,
            List<Type> types,
            ColumnMetadata<OrcType> orcTypes,
            Map<String, String> metadata)
    {
        return new OrcWriter(
                createOrcDataSink(target),
                columnNames,
                types,
                orcTypes,
                CompressionKind.SNAPPY,
                new OrcWriterOptions(),
                metadata,
                true,
                OrcWriteValidationMode.BOTH,
                new OrcWriterStats());
    }
}
