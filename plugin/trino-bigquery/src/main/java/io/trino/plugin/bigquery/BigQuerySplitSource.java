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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.spi.NodeManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.predicate.TupleDomain;
import jakarta.annotation.Nullable;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.google.cloud.bigquery.TableDefinition.Type.MATERIALIZED_VIEW;
import static com.google.cloud.bigquery.TableDefinition.Type.VIEW;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.bigquery.BigQueryClient.TABLE_TYPES;
import static io.trino.plugin.bigquery.BigQueryClient.selectSql;
import static io.trino.plugin.bigquery.BigQueryErrorCode.BIGQUERY_FAILED_TO_EXECUTE_QUERY;
import static io.trino.plugin.bigquery.BigQueryUtil.buildNativeQuery;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeSchema;

public class BigQuerySplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(BigQuerySplitSource.class);

    private final ConnectorSession session;
    private final BigQueryTableHandle table;
    private final BigQueryClientFactory bigQueryClientFactory;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final boolean viewEnabled;
    private final boolean arrowSerializationEnabled;
    private final Duration viewExpiration;
    private final NodeManager nodeManager;
    private final int maxReadRowsRetries;

    @Nullable
    private List<BigQuerySplit> splits;
    private int offset;

    public BigQuerySplitSource(ConnectorSession session,
            BigQueryTableHandle table,
            BigQueryClientFactory bigQueryClientFactory,
            BigQueryReadClientFactory bigQueryReadClientFactory,
            boolean viewEnabled,
            boolean arrowSerializationEnabled,
            Duration viewExpiration,
            NodeManager nodeManager,
            int maxReadRowsRetries)
    {
        this.session = requireNonNull(session, "session is null");
        this.table = requireNonNull(table, "table is null");
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory cannot be null");
        this.bigQueryReadClientFactory = requireNonNull(bigQueryReadClientFactory, "bigQueryReadClientFactory cannot be null");
        this.viewEnabled = viewEnabled;
        this.arrowSerializationEnabled = arrowSerializationEnabled;
        this.viewExpiration = requireNonNull(viewExpiration, "viewExpiration is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager cannot be null");
        this.maxReadRowsRetries = maxReadRowsRetries;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (splits == null) {
            splits = getSplits(session, table);
        }

        return completedFuture(new ConnectorSplitBatch(prepareNextBatch(maxSize), isFinished()));
    }

    private List<ConnectorSplit> prepareNextBatch(int maxSize)
    {
        requireNonNull(splits, "splits is null");
        int nextOffset = Math.min(splits.size(), offset + maxSize);
        List<ConnectorSplit> results = splits.subList(offset, nextOffset).stream()
                .map(ConnectorSplit.class::cast)
                .collect(toImmutableList());
        offset = nextOffset;
        return results;
    }

    @Override
    public boolean isFinished()
    {
        return splits != null && offset >= splits.size();
    }

    @Override
    public void close()
    {
        splits = null;
    }

    private List<BigQuerySplit> getSplits(
            ConnectorSession session,
            BigQueryTableHandle bigQueryTableHandle)
    {
        TupleDomain<ColumnHandle> tableConstraint = bigQueryTableHandle.constraint();
        Optional<String> filter = BigQueryFilterQueryBuilder.buildFilter(tableConstraint);
        OptionalLong limit = bigQueryTableHandle.limit();

        TableId remoteTableId;
        TableDefinition.Type tableType;
        boolean useStorageApi;

        if (bigQueryTableHandle.isQueryRelation()) {
            BigQueryQueryRelationHandle bigQueryQueryRelationHandle = bigQueryTableHandle.getRequiredQueryRelation();
            List<BigQueryColumnHandle> columns = bigQueryTableHandle.projectedColumns().orElse(ImmutableList.of());
            useStorageApi = bigQueryQueryRelationHandle.isUseStorageApi();

            if (!useStorageApi) {
                log.debug("Using Rest API for running query: %s", bigQueryQueryRelationHandle.getQuery());
                return List.of(BigQuerySplit.forViewStream(columns, filter));
            }
            String query = buildNativeQuery(bigQueryQueryRelationHandle.getQuery(), filter, limit);

            TableId destinationTable = bigQueryQueryRelationHandle.getDestinationTableName().toTableId();
            TableInfo tableInfo = new ViewMaterializationCache.DestinationTableBuilder(bigQueryClientFactory.create(session), viewExpiration, query, destinationTable).get();

            log.debug("Using Storage API for running query: %s", query);
            remoteTableId = tableInfo.getTableId();
            tableType = tableInfo.getDefinition().getType();
        }
        else {
            BigQueryNamedRelationHandle namedRelation = bigQueryTableHandle.getRequiredNamedRelation();
            remoteTableId = namedRelation.getRemoteTableName().toTableId();
            tableType = TableDefinition.Type.valueOf(namedRelation.getType());
            useStorageApi = namedRelation.isUseStorageApi();
        }

        return emptyProjectionIsRequired(bigQueryTableHandle.projectedColumns())
                ? createEmptyProjection(session, tableType, remoteTableId, filter, limit)
                : readFromBigQuery(session, tableType, remoteTableId, bigQueryTableHandle.projectedColumns(), tableConstraint, useStorageApi);
    }

    private static boolean emptyProjectionIsRequired(Optional<List<BigQueryColumnHandle>> projectedColumns)
    {
        return projectedColumns.isPresent() && projectedColumns.get().isEmpty();
    }

    private List<BigQuerySplit> readFromBigQuery(
            ConnectorSession session,
            TableDefinition.Type type,
            TableId remoteTableId,
            Optional<List<BigQueryColumnHandle>> projectedColumns,
            TupleDomain<ColumnHandle> tableConstraint,
            boolean useStorageApi)
    {
        checkArgument(projectedColumns.isPresent() && projectedColumns.get().size() > 0, "Projected column is empty");
        Optional<String> filter = BigQueryFilterQueryBuilder.buildFilter(tableConstraint);

        log.debug("readFromBigQuery(tableId=%s, projectedColumns=%s, filter=[%s])", remoteTableId, projectedColumns, filter);
        List<BigQueryColumnHandle> columns = projectedColumns.get();
        List<String> projectedColumnsNames = getProjectedColumnNames(columns);
        ImmutableList.Builder<BigQueryColumnHandle> projectedColumnHandles = ImmutableList.builder();
        projectedColumnHandles.addAll(columns);

        if (!useStorageApi) {
            return ImmutableList.of(BigQuerySplit.forViewStream(columns, filter));
        }
        if (type == VIEW || type == MATERIALIZED_VIEW) {
            tableConstraint.getDomains().ifPresent(domains -> domains.keySet().stream()
                    .map(BigQueryColumnHandle.class::cast)
                    .filter(column -> !projectedColumnsNames.contains(column.name()))
                    .forEach(projectedColumnHandles::add));
        }
        ReadSession readSession = createReadSession(session, remoteTableId, ImmutableList.copyOf(projectedColumnHandles.build()), filter);

        String schemaString = getSchemaAsString(readSession);
        return readSession.getStreamsList().stream()
                .map(stream -> BigQuerySplit.forStream(stream.getName(), schemaString, columns, OptionalInt.of(stream.getSerializedSize())))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    ReadSession createReadSession(ConnectorSession session, TableId remoteTableId, List<BigQueryColumnHandle> columns, Optional<String> filter)
    {
        ReadSessionCreator readSessionCreator = new ReadSessionCreator(bigQueryClientFactory, bigQueryReadClientFactory, viewEnabled, arrowSerializationEnabled, viewExpiration, maxReadRowsRetries);
        return readSessionCreator.create(session, remoteTableId, columns, filter, nodeManager.getRequiredWorkerNodes().size());
    }

    private static List<String> getProjectedColumnNames(List<BigQueryColumnHandle> columns)
    {
        return columns.stream().map(BigQueryColumnHandle::name).collect(toImmutableList());
    }

    private List<BigQuerySplit> createEmptyProjection(ConnectorSession session, TableDefinition.Type tableType, TableId remoteTableId, Optional<String> filter, OptionalLong limit)
    {
        if (!TABLE_TYPES.containsKey(tableType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported table type: " + tableType);
        }

        // Note that we cannot use row count from TableInfo because for writes via insertAll/streaming API the number is incorrect until the streaming buffer is flushed
        // (and there's no mechanism to trigger an on-demand flush). This can lead to incorrect results for queries with empty projections.
        String sql = "SELECT COUNT(*) FROM (%s)".formatted(selectSql(remoteTableId, "true", filter, limit));

        return createEmptyProjection(session, sql);
    }

    private List<BigQuerySplit> createEmptyProjection(ConnectorSession session, String sql)
    {
        BigQueryClient client = bigQueryClientFactory.create(session);
        log.debug("createEmptyProjection(sql=%s)", sql);
        try {
            TableResult result = client.executeQuery(session, sql);
            long numberOfRows = getOnlyElement(getOnlyElement(result.iterateAll())).getLongValue();

            return ImmutableList.of(BigQuerySplit.emptyProjection(numberOfRows));
        }
        catch (BigQueryException e) {
            throw new TrinoException(BIGQUERY_FAILED_TO_EXECUTE_QUERY, "Failed to compute empty projection", e);
        }
    }

    private String getSchemaAsString(ReadSession readSession)
    {
        if (arrowSerializationEnabled) {
            return deserializeArrowSchema(readSession.getArrowSchema().getSerializedSchema());
        }
        return readSession.getAvroSchema().getSchema();
    }

    private static String deserializeArrowSchema(ByteString serializedSchema)
    {
        try {
            return deserializeSchema(new ReadChannel(new ByteArrayReadableSeekableByteChannel(serializedSchema.toByteArray())))
                    .toJson();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
