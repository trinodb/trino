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

import io.airlift.log.Logger;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.createDisposition;
import static io.trino.plugin.bigquery.BigQuerySessionProperties.isQueryResultsCacheEnabled;
import static java.util.Objects.requireNonNull;

public class BigQueryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(BigQueryPageSourceProvider.class);

    private final BigQueryClientFactory bigQueryClientFactory;
    private final BigQueryReadClientFactory bigQueryReadClientFactory;
    private final int maxReadRowsRetries;
    private final boolean arrowSerializationEnabled;

    @Inject
    public BigQueryPageSourceProvider(BigQueryClientFactory bigQueryClientFactory, BigQueryReadClientFactory bigQueryReadClientFactory, BigQueryConfig config)
    {
        this.bigQueryClientFactory = requireNonNull(bigQueryClientFactory, "bigQueryClientFactory is null");
        this.bigQueryReadClientFactory = requireNonNull(bigQueryReadClientFactory, "bigQueryReadClientFactory is null");
        this.maxReadRowsRetries = config.getMaxReadRowsRetries();
        this.arrowSerializationEnabled = config.isArrowSerializationEnabled();
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

        // We expect columns list requested here to match list passed to ConnectorMetadata.applyProjection.
        checkArgument(bigQuerySplit.getColumns().isEmpty() || bigQuerySplit.getColumns().equals(columns),
                "Requested columns %s do not match list in split %s", columns, bigQuerySplit.getColumns());

        if (bigQuerySplit.representsEmptyProjection()) {
            return new BigQueryEmptyProjectionPageSource(bigQuerySplit.getEmptyRowsToGenerate());
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
        return switch (split.getMode()) {
            case STORAGE -> createStoragePageSource(session, split, columnHandles);
            case QUERY -> createQueryPageSource(session, table, columnHandles, split.getFilter());
        };
    }

    private ConnectorPageSource createStoragePageSource(ConnectorSession session, BigQuerySplit split, List<BigQueryColumnHandle> columnHandles)
    {
        if (arrowSerializationEnabled) {
            return new BigQueryStorageArrowPageSource(
                    bigQueryReadClientFactory.create(session),
                    maxReadRowsRetries,
                    split,
                    columnHandles);
        }
        return new BigQueryStorageAvroPageSource(
                bigQueryReadClientFactory.create(session),
                maxReadRowsRetries,
                split,
                columnHandles);
    }

    private ConnectorPageSource createQueryPageSource(ConnectorSession session, BigQueryTableHandle table, List<BigQueryColumnHandle> columnHandles, Optional<String> filter)
    {
        return new BigQueryQueryPageSource(
                bigQueryClientFactory.create(session),
                table,
                columnHandles.stream().map(BigQueryColumnHandle::getName).collect(toImmutableList()),
                columnHandles.stream().map(BigQueryColumnHandle::getTrinoType).collect(toImmutableList()),
                filter,
                isQueryResultsCacheEnabled(session),
                createDisposition(session));
    }
}
