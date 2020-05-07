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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.elasticsearch.client.ElasticsearchClient;
import io.prestosql.elasticsearch.decoders.CoderFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class ElasticsearchPageSink
        implements ConnectorPageSink
{
    private final ConnectorSession connectorSession;
    private final ElasticsearchClient clientSession;
    private final String index;
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    public ElasticsearchPageSink(
            ConnectorSession connectorSession,
            ElasticsearchClient clientSession,
            String index,
            List<String> columnNames,
            List<Type> columnTypes)
    {
        this.connectorSession = connectorSession;
        this.clientSession = clientSession;
        this.index = index;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        ImmutableList.Builder<ImmutableMap<String, Optional<Object>>> bulk = ImmutableList.builder();
        for (int position = 0; position < page.getPositionCount(); position++) {
            ImmutableMap.Builder<String, Optional<Object>> values = ImmutableMap.builder();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                String fieldName = columnNames.get(channel);
                Object fieldValue = columnValue(page, position, channel);
                values.put(fieldName, Optional.ofNullable(fieldValue));
            }
            bulk.add(values.build());
        }
        try {
            clientSession.saveIndexBulk(index, bulk.build());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return NOT_BLOCKED;
    }

    private Object columnValue(Page page, int position, int channel)
    {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(channel);

        return CoderFactory.columnValue(connectorSession, type, block, position);

//        return CoderFactory.createDecoder(connectorSession, type)
//                .encode(type.getObjectValue(connectorSession, block, position));
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        // Ignore
    }
}
