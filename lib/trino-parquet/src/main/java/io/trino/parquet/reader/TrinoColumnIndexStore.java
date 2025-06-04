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
package io.trino.parquet.reader;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.parquet.metadata.IndexReference;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import jakarta.annotation.Nullable;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.filter2.columnindex.ColumnIndexStore;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetMetadataConverter.fromParquetColumnIndex;
import static io.trino.parquet.ParquetMetadataConverter.fromParquetOffsetIndex;
import static java.util.Objects.requireNonNull;

/**
 * Internal implementation of {@link ColumnIndexStore}.
 * Similar to org.apache.parquet.hadoop.ColumnIndexStoreImpl which is not accessible
 */
public class TrinoColumnIndexStore
        implements ColumnIndexStore
{
    private final ParquetDataSource dataSource;
    private final List<ColumnIndexMetadata> columnIndexReferences;
    private final List<ColumnIndexMetadata> offsetIndexReferences;

    @Nullable
    private Map<ColumnPath, ColumnIndex> columnIndexStore;
    @Nullable
    private Map<ColumnPath, OffsetIndex> offsetIndexStore;

    /**
     * Creates a column index store which lazily reads column/offset indexes for the columns in paths.
     *
     * @param columnsRead is the set of columns used for projection
     * @param columnsFiltered is the set of columns used for filtering
     */
    public TrinoColumnIndexStore(
            ParquetDataSource dataSource,
            BlockMetadata block,
            Set<ColumnPath> columnsRead,
            Set<ColumnPath> columnsFiltered)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        requireNonNull(block, "block is null");
        requireNonNull(columnsRead, "columnsRead is null");
        requireNonNull(columnsFiltered, "columnsFiltered is null");

        ImmutableList.Builder<ColumnIndexMetadata> columnIndexBuilder = ImmutableList.builderWithExpectedSize(columnsFiltered.size());
        ImmutableList.Builder<ColumnIndexMetadata> offsetIndexBuilder = ImmutableList.builderWithExpectedSize(columnsRead.size());
        for (ColumnChunkMetadata column : block.columns()) {
            ColumnPath path = column.getPath();
            if (column.getColumnIndexReference() != null && columnsFiltered.contains(path)) {
                columnIndexBuilder.add(new ColumnIndexMetadata(
                        column.getColumnIndexReference(),
                        path,
                        column.getPrimitiveType()));
            }
            if (column.getOffsetIndexReference() != null && columnsRead.contains(path)) {
                offsetIndexBuilder.add(new ColumnIndexMetadata(
                        column.getOffsetIndexReference(),
                        path,
                        column.getPrimitiveType()));
            }
        }
        this.columnIndexReferences = columnIndexBuilder.build();
        this.offsetIndexReferences = offsetIndexBuilder.build();
    }

    @Override
    public ColumnIndex getColumnIndex(ColumnPath column)
    {
        if (columnIndexStore == null) {
            columnIndexStore = loadIndexes(dataSource, columnIndexReferences, (inputStream, columnMetadata) -> {
                try {
                    return fromParquetColumnIndex(columnMetadata.getPrimitiveType(), Util.readColumnIndex(inputStream));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        return columnIndexStore.get(column);
    }

    @Override
    public OffsetIndex getOffsetIndex(ColumnPath column)
    {
        if (offsetIndexStore == null) {
            offsetIndexStore = loadIndexes(dataSource, offsetIndexReferences, (inputStream, columnMetadata) -> {
                try {
                    return fromParquetOffsetIndex(Util.readOffsetIndex(inputStream));
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        return offsetIndexStore.get(column);
    }

    public static Optional<ColumnIndexStore> getColumnIndexStore(
            ParquetDataSource dataSource,
            BlockMetadata blockMetadata,
            Map<List<String>, ColumnDescriptor> descriptorsByPath,
            TupleDomain<ColumnDescriptor> parquetTupleDomain,
            ParquetReaderOptions options)
    {
        if (!options.isUseColumnIndex() || parquetTupleDomain.isAll() || parquetTupleDomain.isNone()) {
            return Optional.empty();
        }

        boolean hasColumnIndex = false;
        for (ColumnChunkMetadata column : blockMetadata.columns()) {
            if (column.getColumnIndexReference() != null && column.getOffsetIndexReference() != null) {
                hasColumnIndex = true;
                break;
            }
        }

        if (!hasColumnIndex) {
            return Optional.empty();
        }

        Set<ColumnPath> columnsReadPaths = new HashSet<>(descriptorsByPath.size());
        for (List<String> path : descriptorsByPath.keySet()) {
            columnsReadPaths.add(ColumnPath.get(path.toArray(new String[0])));
        }

        Map<ColumnDescriptor, Domain> parquetDomains = parquetTupleDomain.getDomains()
                .orElseThrow(() -> new IllegalStateException("Predicate other than none should have domains"));
        Set<ColumnPath> columnsFilteredPaths = parquetDomains.keySet().stream()
                .map(column -> ColumnPath.get(column.getPath()))
                .collect(toImmutableSet());

        return Optional.of(new TrinoColumnIndexStore(dataSource, blockMetadata, columnsReadPaths, columnsFilteredPaths));
    }

    private static <T> Map<ColumnPath, T> loadIndexes(
            ParquetDataSource dataSource,
            List<ColumnIndexMetadata> indexMetadata,
            BiFunction<InputStream, ColumnIndexMetadata, T> deserializer)
    {
        // Merge multiple small reads of the file for indexes stored close to each other
        ListMultimap<ColumnPath, DiskRange> ranges = ArrayListMultimap.create(indexMetadata.size(), 1);
        for (ColumnIndexMetadata column : indexMetadata) {
            ranges.put(column.getPath(), column.getDiskRange());
        }

        Map<ColumnPath, ChunkedInputStream> columnInputStreams = dataSource.planRead(ranges, newSimpleAggregatedMemoryContext());
        try {
            return indexMetadata.stream()
                    .collect(toImmutableMap(
                            ColumnIndexMetadata::getPath,
                            column -> deserializer.apply(columnInputStreams.get(column.getPath()), column)));
        }
        finally {
            columnInputStreams.values().forEach(ChunkedInputStream::close);
        }
    }

    private static class ColumnIndexMetadata
    {
        private final DiskRange diskRange;
        private final ColumnPath path;
        private final PrimitiveType primitiveType;

        private ColumnIndexMetadata(IndexReference indexReference, ColumnPath path, PrimitiveType primitiveType)
        {
            requireNonNull(indexReference, "indexReference is null");
            this.diskRange = new DiskRange(indexReference.getOffset(), indexReference.getLength());
            this.path = requireNonNull(path, "path is null");
            this.primitiveType = requireNonNull(primitiveType, "primitiveType is null");
        }

        private DiskRange getDiskRange()
        {
            return diskRange;
        }

        private ColumnPath getPath()
        {
            return path;
        }

        private PrimitiveType getPrimitiveType()
        {
            return primitiveType;
        }
    }
}
