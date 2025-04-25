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
package io.trino.plugin.iceberg.functions.tablefiles;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class TableFilesSplitSource
        implements ConnectorSplitSource
{
    private final String schemaJson;
    private final Map<Integer, String> partitionSpecsByIdJson;
    private final Iterator<ManifestFile> manifestFileItr;

    public TableFilesSplitSource(Table icebergTable, ExecutorService executor)
    {
        this.schemaJson = SchemaParser.toJson(icebergTable.schema());
        this.partitionSpecsByIdJson = icebergTable.specs().entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        kv -> PartitionSpecParser.toJson(kv.getValue())));
        this.manifestFileItr = icebergTable.newScan()
                .planWith(executor)
                .useSnapshot(icebergTable.currentSnapshot().snapshotId())
                .snapshot()
                .allManifests(icebergTable.io()).iterator();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();

        while (manifestFileItr.hasNext() && splits.size() < maxSize) {
            try {
                ManifestFile manifestFile = manifestFileItr.next();
                splits.add(new TableFilesSplit(
                        Base64.getEncoder().encodeToString(ManifestFiles.encode(manifestFile)),
                        schemaJson,
                        partitionSpecsByIdJson));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return completedFuture(new ConnectorSplitBatch(splits, !manifestFileItr.hasNext()));
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public boolean isFinished()
    {
        return manifestFileItr != null && !manifestFileItr.hasNext();
    }
}
