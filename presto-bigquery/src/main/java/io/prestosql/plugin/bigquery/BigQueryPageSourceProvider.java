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

import io.airlift.log.Logger;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.predicate.TupleDomain;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class BigQueryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger log = Logger.get(BigQueryPageSourceProvider.class);

    private final BigQueryStorageClientFactory bigQueryStorageClientFactory;
    private final int maxReadRowsRetries;

    @Inject
    public BigQueryPageSourceProvider(BigQueryStorageClientFactory bigQueryStorageClientFactory, BigQueryConfig config)
    {
        this.bigQueryStorageClientFactory = requireNonNull(bigQueryStorageClientFactory, "bigQueryStorageClientFactory is null");
        this.maxReadRowsRetries = requireNonNull(config, "config is null").getMaxReadRowsRetries();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        log.debug("createPageSource(transaction=%s, session=%s, split=%s, table=%s, columns=%s)", transaction, session, split, table, columns);
        BigQuerySplit bigQuerySplit = (BigQuerySplit) split;
        if (bigQuerySplit.representsEmptyProjection()) {
            return new BigQueryEmptyProjectionPageSource(bigQuerySplit.getEmptyRowsToGenerate());
        }

        // not empty projection
        List<BigQueryColumnHandle> bigQueryColumnHandles = columns.stream()
                .map(BigQueryColumnHandle.class::cast)
                .collect(toImmutableList());

        return new BigQueryResultPageSource(bigQueryStorageClientFactory, maxReadRowsRetries, bigQuerySplit, bigQueryColumnHandles);
    }
}
