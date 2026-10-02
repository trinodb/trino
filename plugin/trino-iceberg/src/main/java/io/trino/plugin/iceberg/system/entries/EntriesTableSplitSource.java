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
package io.trino.plugin.iceberg.system.entries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.system.files.TrinoManifestFile;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import org.apache.iceberg.IcebergManifestUtils;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;
import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.apache.iceberg.TableProperties.ENCRYPTION_TABLE_KEY;

public final class EntriesTableSplitSource
        implements ConnectorSplitSource
{
    private final FileIO fileIO;
    private final String schemaJson;
    private final String metadataSchemaJson;
    private final Map<Integer, String> partitionSpecsByIdJson;
    private final Optional<String> encryptionKeyId;
    private final Set<String> seenManifestPaths = new HashSet<>();
    private Iterator<Snapshot> snapshots;
    private Iterator<ManifestFile> manifests = emptyIterator();
    private volatile boolean closed;

    public EntriesTableSplitSource(
            Table icebergTable,
            MetadataTableType metadataTableType,
            String schemaJson,
            String metadataSchemaJson,
            Map<Integer, String> partitionSpecsByIdJson)
    {
        requireNonNull(icebergTable, "icebergTable is null");
        checkArgument(metadataTableType == ALL_ENTRIES || metadataTableType == ENTRIES, "Unexpected metadata table type: %s", metadataTableType);
        this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
        this.metadataSchemaJson = requireNonNull(metadataSchemaJson, "metadataSchemaJson is null");
        this.partitionSpecsByIdJson = ImmutableMap.copyOf(partitionSpecsByIdJson);
        this.encryptionKeyId = Optional.ofNullable(icebergTable.properties().get(ENCRYPTION_TABLE_KEY));
        this.fileIO = icebergTable.io();
        this.snapshots = metadataTableType == ALL_ENTRIES
                ? icebergTable.snapshots().iterator()
                : Optional.ofNullable(icebergTable.currentSnapshot()).map(ImmutableList::of).orElseGet(ImmutableList::of).iterator();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        checkArgument(maxSize > 0, "maxSize must be positive");
        if (closed) {
            return completedFuture(ImmutableList.of());
        }

        try {
            List<ConnectorSplit> splits = new ArrayList<>();
            while (!closed && splits.size() < maxSize) {
                if (!manifests.hasNext()) {
                    manifests = emptyIterator();
                    if (!snapshots.hasNext()) {
                        close();
                        break;
                    }
                    manifests = IcebergManifestUtils.read(fileIO, snapshots.next()).iterator();
                    continue;
                }
                ManifestFile manifest = manifests.next();
                if (seenManifestPaths.add(manifest.path())) {
                    splits.add(new EntriesTableSplit(
                            TrinoManifestFile.from(manifest),
                            schemaJson,
                            metadataSchemaJson,
                            partitionSpecsByIdJson,
                            encryptionKeyId));
                }
            }
            return completedFuture(splits);
        }
        catch (RuntimeException | Error e) {
            close();
            throw e;
        }
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public void close()
    {
        closed = true;
    }
}
