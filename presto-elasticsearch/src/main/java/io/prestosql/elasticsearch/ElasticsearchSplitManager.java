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
package io.prestosql.elasticsearch;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplitManager
        implements ConnectorSplitManager
{
    private final ElasticsearchClient client;

    @Inject
    public ElasticsearchSplitManager(ElasticsearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) table;
        ElasticsearchTableDescription tableDescription = client.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        verifyNotNull(table, "Table no longer exists: %s", tableHandle.toString());

        List<ElasticsearchSplit> splits = client.getSearchShards(tableDescription.getIndex()).stream()
                .map(shard -> new ElasticsearchSplit(
                        tableDescription.getIndex(),
                        tableDescription.getType(),
                        shard.getId(),
                        shard.getHost(),
                        shard.getPort()))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }
}
