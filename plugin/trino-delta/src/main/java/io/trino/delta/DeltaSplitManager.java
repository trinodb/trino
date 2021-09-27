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
package io.trino.delta;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.delta.DeltaExpressionUtils.sanitizePartitionValues;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DeltaSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final DeltaConfig deltaConfig;
    private final DeltaClient deltaClient;
    private final TypeManager typeManager;

    @Inject
    public DeltaSplitManager(
            DeltaConnectorId connectorId,
            DeltaConfig deltaConfig,
            DeltaClient deltaClient,
            TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.deltaConfig = requireNonNull(deltaConfig, "deltaConfig is null");
        this.deltaClient = requireNonNull(deltaClient, "deltaClient is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        DeltaTableHandle tableHandle = (DeltaTableHandle) table;
        return new DeltaSplitSource(session, tableHandle);
    }

    private class DeltaSplitSource
            implements ConnectorSplitSource
    {
        private final DeltaTable deltaTable;
        private final CloseableIterator<AddFile> fileListIterator;
        private final int maxBatchSize;

        DeltaSplitSource(ConnectorSession session, DeltaTableHandle deltaTableHandle)
        {
            this.deltaTable = deltaTableHandle.getDeltaTable();
            this.fileListIterator = DeltaExpressionUtils.iterateWithPartitionPruning(
                    session,
                    deltaClient.listFiles(session, deltaTable),
                    deltaTableHandle.getPredicate(),
                    typeManager);
            this.maxBatchSize = deltaConfig.getMaxSplitsBatchSize();
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            List<ConnectorSplit> splits = new ArrayList<>();
            while (fileListIterator.hasNext() && splits.size() < maxSize && splits.size() < maxBatchSize) {
                AddFile file = fileListIterator.next();
                Path filePath = new Path(deltaTable.getTableLocation(), URI.create(file.getPath()).getPath());
                splits.add(new DeltaSplit(
                        connectorId,
                        deltaTable.getSchemaName(),
                        deltaTable.getTableName(),
                        filePath.toString(),
                        0, /* start */
                        file.getSize() /* split length - read the entire file in one split */,
                        file.getSize(),
                        sanitizePartitionValues(file.getPartitionValues())));
            }

            return completedFuture(new ConnectorSplitBatch(splits, !fileListIterator.hasNext()));
        }

        @Override
        public void close()
        {
            try {
                fileListIterator.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean isFinished()
        {
            return !fileListIterator.hasNext();
        }
    }
}
