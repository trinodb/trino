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
package io.trino.plugin.iceberg.gpu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.plugin.iceberg.IcebergTableHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.nio.file.StandardOpenOption.READ;

/**
 * The data files of an Iceberg table snapshot, as the Iceberg connector's split manager lists them,
 * when the device can read them: local Parquet files without delete files or encryption, whose
 * top-level columns sit at the same index in every file.
 */
final class DataFiles
{
    private static final int MAX_CACHED_FOOTERS = 100_000;

    /**
     * Data file to its top-level Parquet columns' indices, by Iceberg field id. Data files are immutable.
     */
    private static final Map<String, Map<Integer, Integer>> COLUMN_INDICES = new ConcurrentHashMap<>();

    /**
     * The snapshot's data files, their total rows, and each field id's column index in all of them.
     */
    record Listing(String key, ImmutableList<String> files, long rows, Map<Integer, Integer> columnIndices) {}

    private DataFiles() {}

    static Optional<Listing> list(ConnectorSplitManager splitManager, ConnectorTransactionHandle transaction, ConnectorSession session, IcebergTableHandle table)
    {
        if (table.getSnapshotId().isEmpty()) {
            return Optional.empty();
        }
        Map<String, Long> rows = new LinkedHashMap<>();
        try (ConnectorSplitSource source = splitManager.getSplits(transaction, session, table, ImmutableSet.of(), Constraint.alwaysTrue())) {
            while (!source.isFinished()) {
                for (ConnectorSplit split : getFutureValue(source.getNextBatch(1000, DynamicFilterSnapshot.EMPTY))) {
                    if (!(split instanceof IcebergSplit iceberg)
                            || !iceberg.deletes().isEmpty()
                            || iceberg.fileFormat() != IcebergFileFormat.PARQUET
                            || iceberg.parquetFileDecryptionData().isPresent()) {
                        return Optional.empty();
                    }
                    Optional<String> path = localPath(iceberg.path());
                    if (path.isEmpty()) {
                        return Optional.empty();
                    }
                    rows.put(path.get(), iceberg.fileRecordCount());
                }
            }
        }
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        Map<Integer, Integer> columnIndices = null;
        for (String file : rows.keySet()) {
            Map<Integer, Integer> indices = columnIndices(file);
            if (columnIndices == null) {
                columnIndices = indices;
            }
            else if (!columnIndices.equals(indices)) {
                // Files written before and after a schema change: the device reads columns by index.
                return Optional.empty();
            }
        }
        long total = rows.values().stream().mapToLong(Long::longValue).sum();
        String key = table.getSchemaTableName() + "@" + table.getSnapshotId().orElseThrow();
        return Optional.of(new Listing(key, ImmutableList.copyOf(rows.keySet()), total, columnIndices));
    }

    private static Optional<String> localPath(String location)
    {
        Location parsed = Location.of(location);
        Optional<String> scheme = parsed.scheme();
        if (scheme.isEmpty() || scheme.get().equals("file") || scheme.get().equals("local")) {
            return Optional.of("/" + parsed.path().replaceFirst("^/+", ""));
        }
        return Optional.empty();
    }

    private static Map<Integer, Integer> columnIndices(String file)
    {
        Map<Integer, Integer> cached = COLUMN_INDICES.get(file);
        if (cached != null) {
            return cached;
        }
        MessageType schema;
        try (LocalParquetDataSource source = new LocalParquetDataSource(Path.of(file))) {
            schema = MetadataReader.readFooter(source, Optional.empty()).getFileMetaData().getSchema();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        ImmutableMap.Builder<Integer, Integer> indices = ImmutableMap.builder();
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Type field = schema.getType(i);
            if (field.getId() != null) {
                indices.put(field.getId().intValue(), i);
            }
        }
        Map<Integer, Integer> result = indices.buildOrThrow();
        if (COLUMN_INDICES.size() > MAX_CACHED_FOOTERS) {
            COLUMN_INDICES.clear();
        }
        COLUMN_INDICES.put(file, result);
        return result;
    }

    /**
     * A local Parquet file, read for its footer.
     */
    private static final class LocalParquetDataSource
            extends AbstractParquetDataSource
    {
        private final FileChannel channel;

        private LocalParquetDataSource(Path path)
                throws IOException
        {
            this(path, FileChannel.open(path, READ));
        }

        private LocalParquetDataSource(Path path, FileChannel channel)
                throws IOException
        {
            super(new ParquetDataSourceId(path.toString()), channel.size(), ParquetReaderOptions.defaultOptions());
            this.channel = channel;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            ByteBuffer target = ByteBuffer.wrap(buffer, bufferOffset, bufferLength);
            long at = position;
            while (target.hasRemaining()) {
                int read = channel.read(target, at);
                if (read < 0) {
                    throw new IOException("Unexpected end of file " + getId());
                }
                at += read;
            }
        }

        @Override
        public void close()
                throws IOException
        {
            channel.close();
        }
    }
}
