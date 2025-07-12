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

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.type.Type;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FilesTableSplitSource
        implements ConnectorSplitSource
{
    private final String schemaJson;
    private final String metadataSchemaJson;
    private final Map<Integer, String> partitionSpecsByIdJson;
    private final Map<String, Type> columns;
    private final Map<String, String> fileSystemProperties;
    private final Iterator<ManifestFile> manifestFileItr;

    public FilesTableSplitSource(
            Iterator<ManifestFile> manifestFileItr,
            String schemaJson,
            String metadataSchemaJson,
            Map<Integer, String> partitionSpecsByIdJson,
            Map<String, Type> columns,
            Map<String, String> fileSystemProperties)
    {
        this.schemaJson = requireNonNull(schemaJson, "schemaJson is null");
        this.metadataSchemaJson = requireNonNull(metadataSchemaJson, "metadataSchemaJson is null");
        this.partitionSpecsByIdJson = requireNonNull(partitionSpecsByIdJson, "partitionSpecsByIdJson is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.fileSystemProperties = requireNonNull(fileSystemProperties, "fileSystemProperties is null");
        this.manifestFileItr = requireNonNull(manifestFileItr, "manifestFileItr is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();

        while (manifestFileItr.hasNext() && splits.size() < maxSize) {
            try {
                splits.add(new FilesTableSplit(
                        Base64.getEncoder().encodeToString(ManifestFiles.encode(manifestFileItr.next())),
                        schemaJson,
                        metadataSchemaJson,
                        partitionSpecsByIdJson,
                        columns,
                        fileSystemProperties));
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
