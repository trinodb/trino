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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OpenSearchSplitManager
        implements ConnectorSplitManager
{
    private final OpenSearchClient client;

    @Inject
    public OpenSearchSplitManager(OpenSearchClient client)
    {
        this.client = requireNonNull(client, "client is null");
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

        if (tableHandle.type().equals(OpenSearchTableHandle.Type.QUERY)) {
            return new FixedSplitSource(new OpenSearchSplit(tableHandle.index(), 0, Optional.empty()));
        }
        List<OpenSearchSplit> splits = client.getSearchShards(tableHandle.index()).stream()
                .map(shard -> new OpenSearchSplit(shard.index(), shard.id(), shard.address()))
                .collect(toImmutableList());

        return new FixedSplitSource(splits);
    }
}
