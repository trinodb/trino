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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.plugin.iceberg.IcebergColumnHandle.DATA_CHANGE_TYPE_ID;
import static io.trino.spi.function.table.TableFunctionProcessorState.Blocked.blocked;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;

public class CopyOnWriteTableChangesFunctionProcessor
        implements TableFunctionSplitProcessor
{
    private final TableFunctionSplitProcessor delegate;

    public CopyOnWriteTableChangesFunctionProcessor(
            ConnectorSession session,
            TableChangesFunctionHandle functionHandle,
            TableChangesSplit split,
            IcebergPageSourceProvider icebergPageSourceProvider,
            ExecutorService executor)
    {
        requireNonNull(session, "session is null");
        requireNonNull(functionHandle, "functionHandle is null");
        requireNonNull(split, "split is null");
        requireNonNull(icebergPageSourceProvider, "icebergPageSourceProvider is null");

        List<ConnectorSplit> splits = split.splits();
        checkArgument(!splits.isEmpty(), "splits is empty");

        if (splits.size() == 1) {
            this.delegate = new InternalTableChangesFunctionProcessor(session, functionHandle, (TableChangesInternalSplit) getOnlyElement(splits), icebergPageSourceProvider);
        }
        else {
            this.delegate = new RemoveCarryoverRowsTableFunctionSplitProcessor(session, functionHandle, splits, icebergPageSourceProvider, executor);
        }
    }

    @Override
    public TableFunctionProcessorState process()
    {
        return delegate.process();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    private static class RemoveCarryoverRowsTableFunctionSplitProcessor
            implements TableFunctionSplitProcessor
    {
        private static final Slice INSERT = Slices.utf8Slice("insert");
        private static final Slice DELETE = Slices.utf8Slice("delete");

        private final Map<Row, AtomicInteger> rows = new ConcurrentHashMap<>();
        private final int dataChangeTypeChannel;
        private final int[] channels;
        private final Type[] types;

        private final CompletableFuture<Void> processFuture;
        private boolean finished;

        public RemoveCarryoverRowsTableFunctionSplitProcessor(
                ConnectorSession session,
                TableChangesFunctionHandle functionHandle,
                List<ConnectorSplit> splits,
                IcebergPageSourceProvider icebergPageSourceProvider,
                ExecutorService executor)
        {
            this.dataChangeTypeChannel = dataChangeTypeChannel(functionHandle.columns());
            this.channels = IntStream.range(0, functionHandle.columns().size())
                    .filter(channel -> channel != dataChangeTypeChannel)
                    .toArray();
            this.types = dataTypes(functionHandle.columns());
            ImmutableList.Builder<Future<?>> futures = ImmutableList.builder();
            for (ConnectorSplit split : splits) {
                TableChangesInternalSplit internalSplit = (TableChangesInternalSplit) split;
                InternalTableChangesFunctionProcessor processor = new InternalTableChangesFunctionProcessor(session, functionHandle, internalSplit, icebergPageSourceProvider);
                futures.add(executor.submit(() -> processAndRemoveCarryoverRows(processor)));
            }

            processFuture = runAsync(
                    () -> {
                        for (Future<?> future : futures.build()) {
                            getFutureValue(future);
                        }
                    },
                    newDirectExecutorService());
        }

        private static int dataChangeTypeChannel(List<IcebergColumnHandle> columns)
        {
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getId() == DATA_CHANGE_TYPE_ID) {
                    return i;
                }
            }
            throw new IllegalArgumentException("Data change type id column is missing from: " + columns);
        }

        private static Type[] dataTypes(List<IcebergColumnHandle> columns)
        {
            return columns.stream()
                    .filter(column -> column.getId() != DATA_CHANGE_TYPE_ID)
                    .map(IcebergColumnHandle::getType)
                    .toArray(Type[]::new);
        }

        private void processAndRemoveCarryoverRows(TableFunctionSplitProcessor processor)
        {
            TableFunctionProcessorState processState = processor.process();
            if (processState == FINISHED) {
                return;
            }

            verify(processState instanceof TableFunctionProcessorState.Processed, "Unexpected state %s", processState);
            TableFunctionProcessorState.Processed processedState = (TableFunctionProcessorState.Processed) processState;
            Page result = processedState.getResult();
            if (result == null || result.getPositionCount() == 0) {
                processAndRemoveCarryoverRows(processor);
                return;
            }

            Block dataChangeTypeBlock = result.getBlock(dataChangeTypeChannel);
            Page dataPage = result.getColumns(channels);
            for (int position = 0; position < result.getPositionCount(); position++) {
                Row row = new Row(types, dataPage, position);
                rows.computeIfAbsent(row, _ -> new AtomicInteger());
                rows.get(row).addAndGet(computeDelta(dataChangeTypeBlock, position));
            }

            processAndRemoveCarryoverRows(processor);
        }

        private static class Row
        {
            private final Object[] values;

            public Row(Type[] types, Page page, int position)
            {
                requireNonNull(page, "page is null");
                checkArgument(types.length == page.getChannelCount(), "mismatched types for page");
                values = new Object[types.length];
                for (int i = 0; i < values.length; i++) {
                    Type type = types[i];

                    Class<?> javaType = type.getJavaType();
                    if (javaType == boolean.class) {
                        values[i] = type.getBoolean(page.getBlock(i), position);
                    }
                    else if (javaType == long.class) {
                        values[i] = type.getLong(page.getBlock(i), position);
                    }
                    else if (javaType == double.class) {
                        values[i] = type.getDouble(page.getBlock(i), position);
                    }
                    else if (javaType == Slice.class) {
                        values[i] = type.getSlice(page.getBlock(i), position);
                    }
                    else {
                        values[i] = type.getObject(page.getBlock(i), position);
                    }
                }
            }

            public int size()
            {
                return values.length;
            }

            public Object getValue(int index)
            {
                return values[index];
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                Row row = (Row) o;
                return Objects.deepEquals(values, row.values);
            }

            @Override
            public int hashCode()
            {
                return Arrays.hashCode(values);
            }
        }

        private static int computeDelta(Block dataChangeTypeBlock, int position)
        {
            Slice slice = VARCHAR.getSlice(dataChangeTypeBlock, position);

            if (INSERT.equals(slice)) {
                return 1;
            }
            if (DELETE.equals(slice)) {
                return -1;
            }

            throw new IllegalStateException("Unexpected data change type: " + slice.toStringUtf8());
        }

        @Override
        public TableFunctionProcessorState process()
        {
            if (!processFuture.isDone()) {
                return blocked(processFuture);
            }

            if (finished) {
                return FINISHED;
            }

            // extract page
            PageBuilder pageBuilder = new PageBuilder(Arrays.asList(types));
            int expectedEntries = rows.values().stream()
                    .mapToInt(AtomicInteger::get)
                    .map(Math::abs)
                    .sum();
            BlockBuilder dataChangeTypeBlockBuilder = VARCHAR.createBlockBuilder(null, expectedEntries);
            for (Map.Entry<Row, AtomicInteger> entry : rows.entrySet()) {
                Row row = entry.getKey();
                AtomicInteger count = entry.getValue();
                if (count.get() == 0) {
                    continue;
                }

                Slice dataChange = count.get() > 0 ? INSERT : DELETE;
                int positionCount = Math.abs(count.get());

                verify(row.size() == types.length);

                pageBuilder.declarePositions(positionCount);
                for (int channel = 0; channel < types.length; channel++) {
                    Type type = types[channel];
                    pageBuilder.getBlockBuilder(channel)
                            .appendBlockRange(RunLengthEncodedBlock.create(type, row.getValue(channel), positionCount), 0, positionCount);
                }
                dataChangeTypeBlockBuilder.appendBlockRange(RunLengthEncodedBlock.create(VARCHAR, dataChange, positionCount), 0, positionCount);
            }

            rows.clear();
            Page page = pageBuilder.build();

            verify(dataChangeTypeChannel < types.length, "Unexpected data change type channel: %s, rest columns size: %s", dataChangeTypeChannel, types.length);
            Block[] blocks = new Block[types.length + 1];
            for (int i = 0; i < types.length; i++) {
                if (i < dataChangeTypeChannel) {
                    blocks[i] = page.getBlock(i);
                }
                else if (i == dataChangeTypeChannel) {
                    blocks[i] = dataChangeTypeBlockBuilder.build();
                    blocks[i + 1] = page.getBlock(i);
                }
                else {
                    blocks[i + 1] = page.getBlock(i);
                }
            }
            finished = true;
            return produced(new Page(blocks));
        }

        @Override
        public void close()
        {
            rows.clear();
            processFuture.cancel(true);
        }
    }
}
