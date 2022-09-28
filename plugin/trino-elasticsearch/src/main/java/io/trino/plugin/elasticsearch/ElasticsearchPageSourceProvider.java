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
package io.trino.plugin.elasticsearch;

import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
import static java.util.Objects.requireNonNull;

public class ElasticsearchPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final ElasticsearchClient client;
    private final TypeManager typeManager;

    @Inject
    public ElasticsearchPageSourceProvider(ElasticsearchClient client, TypeManager typeManager)
    {
        this.client = requireNonNull(client, "client is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
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
        requireNonNull(split, "split is null");
        requireNonNull(table, "table is null");

        ElasticsearchTableHandle elasticsearchTable = (ElasticsearchTableHandle) table;
        ElasticsearchSplit elasticsearchSplit = (ElasticsearchSplit) split;

        if (elasticsearchTable.getType().equals(QUERY)) {
            return new PassthroughQueryPageSource(client, elasticsearchTable);
        }

        if (columns.isEmpty()) {
            return new CountQueryPageSource(client, elasticsearchTable, elasticsearchSplit);
        }

        return new ScanQueryPageSource(
                client,
                typeManager,
                elasticsearchTable,
                elasticsearchSplit,
                columns.stream()
                        .map(ElasticsearchColumnHandle.class::cast)
                        .collect(toImmutableList()));
    }
}
