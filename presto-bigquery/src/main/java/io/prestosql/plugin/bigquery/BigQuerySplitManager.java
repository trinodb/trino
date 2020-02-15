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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class BigQuerySplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(BigQuerySplitManager.class);

    private final BigQuery bigquery;
    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final OptionalInt parallelism;
    private final ReadSessionCreatorConfig readSessionCreatorConfig;
    private final NodeManager nodeManager;

    @Inject
    public BigQuerySplitManager(
            BigQueryConfig config,
            BigQuery bigquery,
            BigQueryStorageClientFactory bigQueryStorageClientFactory,
            NodeManager nodeManager)
    {
        requireNonNull(config, "config cannot be null");

        this.bigquery = requireNonNull(bigquery, "bigquery cannot be null");
        this.bigQueryStorageClientFactory = requireNonNull(bigQueryStorageClientFactory, "bigQueryStorageClientFactory cannot be null");
        this.parallelism = config.getParallelism();
        this.readSessionCreatorConfig = config.createReadSessionCreatorConfig();
        this.nodeManager = requireNonNull(nodeManager, "nodeManager cannot be null");
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
        List<ColumnHandle> desiredColumns = bigQueryTableHandle.getDesiredColumns().orElse(ImmutableList.of());
        int actualParallelism = parallelism.orElse(nodeManager.getRequiredWorkerNodes().size());
        Optional<String> filter = Optional.empty();
        ImmutableList<BigQuerySplit> splits = //desiredColumns.isEmpty() ?
                //generateEmptyProjection(tableId, actualParallelism, filter) :
                readFromBigQuery(tableId, desiredColumns, actualParallelism, filter);
        return new FixedSplitSource(splits);
    }

    private ImmutableList<BigQuerySplit> generateEmptyProjection(TableId tableId, int actualParallelism, Optional<String> filter)
    {
        log.debug("generateEmptyProjection(tableId=%s, actualParallelism=%s, filter=%s)", tableId, actualParallelism, filter);
        return null;
    }

    private ImmutableList<BigQuerySplit> readFromBigQuery(TableId tableId, List<ColumnHandle> desiredColumns, int actualParallelism, Optional<String> filter)
    {
        log.debug("readFromBigQuery(tableId=%s, desiredColumns=%s, actualParallelism=%s, filter=[%s])", tableId, desiredColumns, actualParallelism, filter);
        ImmutableList<String> desiredColumnsNames = desiredColumns.stream()
                .map(column -> ((BigQueryColumnHandle) column).getName())
                .collect(toImmutableList());

        ReadSession readSession = new ReadSessionCreator(readSessionCreatorConfig, bigquery, bigQueryStorageClientFactory)
                .create(tableId, desiredColumnsNames, filter, actualParallelism);

        return readSession.getStreamsList().stream()
                .map(stream -> new BigQuerySplit(stream.getName(), readSession.getAvroSchema().getSchema(), desiredColumns))
                .collect(toImmutableList());
    }
}
