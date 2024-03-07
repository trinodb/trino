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
package io.trino.plugin.opensearch;

import com.google.inject.Inject;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.client.Shard;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.opensearch.action.search.CreatePitResponse;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OpenSearchSplitManager
        implements ConnectorSplitManager
{
    private final OpenSearchClient client;
    private final OpenSearchConfig config;

    @Inject
    public OpenSearchSplitManager(OpenSearchClient client, OpenSearchConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        this.config = config;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        OpenSearchTableHandle tableHandle = (OpenSearchTableHandle) table;

        if (tableHandle.getType().equals(OpenSearchTableHandle.Type.QUERY)) {
            return new FixedSplitSource(new OpenSearchSplit(tableHandle.getIndex(), 0, Optional.empty(), 0, null));
        }

        System.out.println("config in split manager: " + config.toString());

        List<Shard> shards = client.getSearchShards(tableHandle.getIndex());
        String pitId;
        List<OpenSearchSplit> splits;
        if (config.getSearchType() == OpenSearchConfig.SearchType.PIT) {
            CreatePitResponse pitResponse = client.createPITRequest(tableHandle.getIndex());
            pitId = pitResponse.getId();
            splits = shards.stream().map(
                            shard -> new OpenSearchSplit(shard.getIndex(), shard.getId(), shard.getAddress(), shards.size() * 2, pitId))
                    .collect(toImmutableList());
            splits = shards.stream()
                    .flatMap(shard -> Stream.of(
                            new OpenSearchSplit(shard.getIndex(), shard.getId(), shard.getAddress(), shards.size() * 2, pitId),
                            new OpenSearchSplit(shard.getIndex(), shard.getId() + shards.size(), shard.getAddress(), shards.size() * 2, pitId)
                    )).collect(toImmutableList());
//            for (int i = shards.size(); i < shards.size() * 2; i++) {
//                splits.add(new OpenSearchSplit(tableHandle.getIndex(), i, Optional.empty(), shards.size() * 2, pitId));
//            }
        }
        else {
            splits = shards.stream().map(
                            shard -> new OpenSearchSplit(shard.getIndex(), shard.getId(), shard.getAddress(), shards.size(), null))
                    .collect(toImmutableList());
        }

        System.out.println("splits: " + splits.toString());
        System.out.println("Splits Count: " + splits.size());

        return new FixedSplitSource(splits);
    }
}
