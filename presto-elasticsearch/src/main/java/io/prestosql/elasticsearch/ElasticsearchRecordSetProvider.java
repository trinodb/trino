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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.TransportAddress;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.prestosql.elasticsearch.ElasticsearchClient.createTransportClient;
import static io.prestosql.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CONNECTION_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ElasticsearchConfig config;
    private final LoadingCache<HostAndPort, TransportClient> clients = CacheBuilder.newBuilder()
            .build(CacheLoader.from(this::initializeClient));

    @Inject
    public ElasticsearchRecordSetProvider(ElasticsearchConfig config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");
        requireNonNull(table, "table is null");
        ElasticsearchSplit elasticsearchSplit = (ElasticsearchSplit) split;
        ElasticsearchTableHandle elasticsearchTable = (ElasticsearchTableHandle) table;
        ImmutableList.Builder<ElasticsearchColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((ElasticsearchColumnHandle) handle);
        }

        try {
            TransportClient client = clients.getUnchecked(HostAndPort.fromParts(elasticsearchSplit.getSearchNode(), elasticsearchSplit.getPort()));
            return new ElasticsearchRecordSet(client, elasticsearchSplit, elasticsearchTable, config, handles.build());
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private TransportClient initializeClient(HostAndPort address)
    {
        try {
            return createTransportClient(config, new TransportAddress(InetAddress.getByName(address.getHost()), address.getPort()));
        }
        catch (UnknownHostException e) {
            throw new PrestoException(ELASTICSEARCH_CONNECTION_ERROR, format("Failed to resolve search node (%s)", address), e);
        }
    }

    @PreDestroy
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            clients.asMap()
                    .values()
                    .forEach(closer::register);
        }
    }
}
