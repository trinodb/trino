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
package io.prestosql.pinot;

import io.prestosql.pinot.client.PinotClient;
import io.prestosql.pinot.client.PinotClient.BrokerResultRow;
import io.prestosql.pinot.decoders.Decoder;
import io.prestosql.pinot.decoders.DecoderFactory;
import io.prestosql.pinot.query.PinotQuery;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.PageBuilderStatus;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PinotBrokerPageSource
        implements ConnectorPageSource
{
    private final PinotQuery query;
    private final PinotClient pinotClient;
    private final ConnectorSession session;
    private final List<PinotColumnHandle> columnHandles;
    private final List<Decoder> decoders;
    private final BlockBuilder[] columnBuilders;

    private boolean finished;
    private long readTimeNanos;
    private long completedBytes;

    private Iterator<BrokerResultRow> resultIterator;

    public PinotBrokerPageSource(
            ConnectorSession session,
            PinotQuery query,
            List<PinotColumnHandle> columnHandles,
            PinotClient pinotClient)
    {
        this.query = requireNonNull(query, "broker is null");
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.session = requireNonNull(session, "session is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.decoders = createDecoders(columnHandles);

        this.columnBuilders = columnHandles.stream()
                .map(PinotColumnHandle::getDataType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);
    }

    private static List<Decoder> createDecoders(List<PinotColumnHandle> columnHandles)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        return columnHandles.stream()
                .map(PinotColumnHandle::getDataType)
                .map(DecoderFactory::createDecoder)
                .collect(toImmutableList());
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        if (resultIterator == null) {
            long start = System.nanoTime();
            resultIterator = pinotClient.createResultIterator(session, query, columnHandles);
            readTimeNanos = System.nanoTime() - start;
        }

        if (!resultIterator.hasNext()) {
            finished = true;
            return null;
        }
        long size = 0;
        int rowCount = 0;
        while (size < PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES && resultIterator.hasNext()) {
            rowCount++;
            BrokerResultRow row = resultIterator.next();
            for (int i = 0; i < decoders.size(); i++) {
                int fieldIndex = i;
                decoders.get(i).decode(() -> row.getField(fieldIndex), columnBuilders[i]);
            }
            size = Arrays.stream(columnBuilders)
                    .mapToLong(BlockBuilder::getSizeInBytes)
                    .sum();
        }
        completedBytes += size;
        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }
        if (decoders.isEmpty()) {
            return new Page(rowCount);
        }
        return new Page(blocks);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
