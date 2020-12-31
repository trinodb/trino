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

import com.google.common.collect.ImmutableList;
import io.prestosql.elasticsearch.client.ElasticsearchClient;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.elasticsearch.ElasticsearchTableHandle.Type.QUERY;
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
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        ElasticsearchTableHandle tableHandle = (ElasticsearchTableHandle) table;

        if (tableHandle.getType().equals(QUERY)) {
            return new FixedSplitSource(ImmutableList.of(new ElasticsearchSplit(tableHandle.getIndex(), 0, Optional.empty())));
        }
        else {
            List<ElasticsearchSplit> splits = client.getSearchShards(tableHandle.getIndex()).stream()
                    .map(shard -> new ElasticsearchSplit(shard.getIndex(), shard.getId(), shard.getAddress()))
                    .collect(toImmutableList());

            return new FixedSplitSource(splits);
        }
    }
}
