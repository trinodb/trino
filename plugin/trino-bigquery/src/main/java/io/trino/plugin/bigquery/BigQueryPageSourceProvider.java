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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class BigQueryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(BigQueryPageSourceProvider.class);

    private final BigQueryClientFactory bigQueryClientFactory;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final BigQueryTypeManager typeManager;
    private final int maxReadRowsRetries;
    private final boolean arrowSerializationEnabled;
    private final ExecutorService executor;
    private final Optional<BigQueryArrowBufferAllocator> arrowBufferAllocator;

    @Inject
    public BigQueryPageSourceProvider(
            BigQueryClientFactory bigQueryClientFactory,
            BigQueryReadClientFactory bigQueryReadClientFactory,
            BigQueryTypeManager typeManager,
            BigQueryConfig config,
            Optional<BigQueryArrowBufferAllocator> arrowBufferAllocator,
            @ForBigQueryPageSource ExecutorService executor)
    {
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory is null");
        this.bigQueryReadClientFactory = requireNonNull(bigQueryReadClientFactory, "bigQueryReadClientFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.maxReadRowsRetries = config.getMaxReadRowsRetries();
        this.arrowSerializationEnabled = config.isArrowSerializationEnabled();
        this.executor = requireNonNull(executor, "executor is null");
        this.arrowBufferAllocator = requireNonNull(arrowBufferAllocator, "arrowBufferAllocator is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        log.debug("createPageSource(transaction=%s, session=%s, split=%s, table=%s, columns=%s)", transaction, session, split, table, columns);
        BigQuerySplit bigQuerySplit = (BigQuerySplit) split;

        Set<String> projectedColumnNames = bigQuerySplit.columns().stream().map(BigQueryColumnHandle::name).collect(Collectors.toSet());
        // because we apply logic (download only parent columns - BigQueryMetadata.projectParentColumns)
        // columns and split columns could differ
        columns.stream()
                .map(BigQueryColumnHandle.class::cast)
                .forEach(column -> checkArgument(projectedColumnNames.contains(column.name()), "projected columns should contain all reader columns"));
        if (bigQuerySplit.representsEmptyProjection()) {
            return new BigQueryEmptyProjectionPageSource(bigQuerySplit.emptyRowsToGenerate());
        }

        // not empty projection
        List<BigQueryColumnHandle> bigQueryColumnHandles = columns.stream()
                .map(BigQueryColumnHandle.class::cast)
                .collect(toImmutableList());

        return createPageSource(session, (BigQueryTableHandle) table, bigQuerySplit, bigQueryColumnHandles);
    }

    private ConnectorPageSource createPageSource(
            ConnectorSession session,
            BigQueryTableHandle table,
            BigQuerySplit split,
            List<BigQueryColumnHandle> columnHandles)
    {
        return switch (split.mode()) {
            case STORAGE -> createStoragePageSource(session, split, columnHandles);
            case QUERY -> createQueryPageSource(session, table, columnHandles, split.filter());
        };
    }

    private ConnectorPageSource createStoragePageSource(ConnectorSession session, BigQuerySplit split, List<BigQueryColumnHandle> columnHandles)
    {
        if (arrowSerializationEnabled) {
            return new BigQueryStorageArrowPageSource(
                    typeManager,
                    bigQueryReadClientFactory.create(session),
                    executor,
                    arrowBufferAllocator.orElseThrow(() -> new IllegalStateException("ArrowBufferAllocator was not bound")),
                    maxReadRowsRetries,
                    split,
                    columnHandles);
        }
        return new BigQueryStorageAvroPageSource(
                bigQueryReadClientFactory.create(session),
                executor,
                typeManager,
                maxReadRowsRetries,
                split,
                columnHandles);
    }

    private ConnectorPageSource createQueryPageSource(ConnectorSession session, BigQueryTableHandle table, List<BigQueryColumnHandle> columnHandles, Optional<String> filter)
    {
        return new BigQueryQueryPageSource(
                session,
                typeManager,
                bigQueryClientFactory.create(session),
                executor,
                table,
                columnHandles,
                filter);
    }
}
