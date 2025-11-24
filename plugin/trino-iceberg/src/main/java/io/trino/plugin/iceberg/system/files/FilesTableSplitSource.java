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
package io.trino.plugin.iceberg.system.files;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.FileIO;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public final class FilesTableSplitSource
        implements ConnectorSplitSource
{
    private final Table icebergTable;
    private final Optional<Long> snapshotId;
    private final String schemaJson;
    private final String metadataSchemaJson;
    private final Map<Integer, String> partitionSpecsByIdJson;
    private final Optional<Type> partitionColumnType;
    private final Map<String, String> fileIoProperties;
    private boolean finished;

    public FilesTableSplitSource(
            Table icebergTable,
            Optional<Long> snapshotId,
            String schemaJson,
            String metadataSchemaJson,
            Map<Integer, String> partitionSpecsByIdJson,
            Optional<Type> partitionColumnType,
            Map<String, String> fileIoProperties)
    {
        this.icebergTable = requireNonNull(icebergTable, "icebergTable is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
        this.metadataSchemaJson = requireNonNull(metadataSchemaJson, "metadataSchemaJson is null");
        this.partitionSpecsByIdJson = ImmutableMap.copyOf(partitionSpecsByIdJson);
        this.partitionColumnType = requireNonNull(partitionColumnType, "partitionColumnType is null");
        this.fileIoProperties = ImmutableMap.copyOf(fileIoProperties);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        TableScan scan = icebergTable.newScan();
        snapshotId.ifPresent(scan::useSnapshot);
        List<ConnectorSplit> splits = new ArrayList<>();

        try (FileIO fileIO = icebergTable.io()) {
            for (ManifestFile manifestFile : scan.snapshot().allManifests(fileIO)) {
                splits.add(new FilesTableSplit(
                        TrinoManifestFile.from(manifestFile),
                        schemaJson,
                        metadataSchemaJson,
                        partitionSpecsByIdJson,
                        partitionColumnType,
                        fileIoProperties));
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
