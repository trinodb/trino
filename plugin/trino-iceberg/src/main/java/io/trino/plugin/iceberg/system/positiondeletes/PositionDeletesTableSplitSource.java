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
package io.trino.plugin.iceberg.system.positiondeletes;

import io.trino.plugin.iceberg.PartitionData;
import io.trino.plugin.iceberg.system.IcebergPartitionColumn;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.IcebergManifestUtils.FileEntryWithMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.IcebergManifestUtils.liveEntriesWithMetadata;
import static org.apache.iceberg.ManifestContent.DELETES;
import static org.apache.iceberg.ManifestFiles.readDeleteManifest;

public final class PositionDeletesTableSplitSource
        implements ConnectorSplitSource
{
    private final Table icebergTable;
    private final String schemaJson;
    private final Map<Integer, String> partitionSpecsByIdJson;
    private final Optional<IcebergPartitionColumn> partitionColumnType;
    private boolean finished;

    public PositionDeletesTableSplitSource(
            Table icebergTable,
            String schemaJson,
            Map<Integer, String> partitionSpecsByIdJson,
            Optional<IcebergPartitionColumn> partitionColumnType)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
        this.partitionSpecsByIdJson = requireNonNull(partitionSpecsByIdJson, "partitionSpecsByIdJson is null");
        this.partitionColumnType = requireNonNull(partitionColumnType, "partitionColumnType is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        TableScan scan = icebergTable.newScan();
        List<ConnectorSplit> splits = new ArrayList<>();

        try (FileIO fileIO = icebergTable.io()) {
            for (ManifestFile manifestFile : scan.snapshot().allManifests(fileIO)) {
                if (manifestFile.content() != DELETES) {
                    continue;
                }

                ManifestReader<DeleteFile> manifestReader = readDeleteManifest(manifestFile, fileIO, icebergTable.specs());
                try (CloseableIterable<FileEntryWithMetadata> entries = liveEntriesWithMetadata(manifestReader)) {
                    for (FileEntryWithMetadata entry : entries) {
                        DeleteFile deleteFile = (DeleteFile) entry.file();
                        if (deleteFile.content() != POSITION_DELETES) {
                            continue;
                        }

                        OptionalLong contentOffset = deleteFile.contentOffset() == null ? OptionalLong.empty() : OptionalLong.of(deleteFile.contentOffset());
                        Optional<Integer> contentSizeInBytes = Optional.ofNullable(deleteFile.contentSizeInBytes()).map(Math::toIntExact);
                        Optional<String> referencedDataFile = Optional.ofNullable(deleteFile.referencedDataFile());

                        splits.add(new PositionDeletesTableSplit(
                                deleteFile.location(),
                                deleteFile.format(),
                                deleteFile.fileSizeInBytes(),
                                PartitionData.toJson(deleteFile.partition()),
                                deleteFile.specId(),
                                schemaJson,
                                partitionSpecsByIdJson,
                                partitionColumnType.map(IcebergPartitionColumn::rowType),
                                partitionColumnType.map(IcebergPartitionColumn::fieldIds).orElseGet(List::of),
                                contentOffset,
                                contentSizeInBytes,
                                referencedDataFile));
                    }
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        finished = true;
        return completedFuture(new ConnectorSplitBatch(splits, true));
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }
}
