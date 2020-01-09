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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class BigQuerySplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(BigQuerySplitManager.class);

    private final BigQueryConfig config;
    private final BigQuery bigquery;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final NodeManager nodeManager;

    @Inject
    public BigQuerySplitManager(
            @Nonnull BigQueryConfig config,
            @Nonnull BigQuery bigquery,
            @Nonnull BigQueryStorageClientFactory bigQueryStorageClientFactory,
            @Nonnull NodeManager nodeManager)
    {
        this.config = config;
        this.bigquery = bigquery;
        this.bigQueryStorageClientFactory = bigQueryStorageClientFactory;
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        log.debug("getSplits(transaction=%s, session=%s, table=%s, splitSchedulingStrategy=%s)", transaction, session, table, splitSchedulingStrategy);
        BigQueryTableHandle bigQueryTableHandle = (BigQueryTableHandle) table;

        TableId tableId = bigQueryTableHandle.getTableId();
        int parallelism = config.getParallelism().orElse(nodeManager.getRequiredWorkerNodes().size());
        ImmutableList<String> requiredColumns = bigQueryTableHandle.getDesiredColumns()
                .orElse(ImmutableList.of()).stream()
                .map(column -> ((BigQueryColumnHandle) column).getName())
                .collect(toImmutableList());

        final ReadSession readSession = new ReadSessionCreator(config, bigquery, bigQueryStorageClientFactory)
                .create(tableId, requiredColumns, null, parallelism);

        ImmutableList<BigQuerySplit> splits = readSession.getStreamsList().stream().map(stream -> new BigQuerySplit(
                stream.getName(),
                readSession.getAvroSchema().getSchema(),
                bigQueryTableHandle.getDesiredColumns().orElse(ImmutableList.of())))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }
}
