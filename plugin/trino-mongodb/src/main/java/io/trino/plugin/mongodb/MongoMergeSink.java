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
package io.trino.plugin.mongodb;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.mongodb.client.model.Filters.in;
import static io.trino.plugin.mongodb.MongoMetadata.MERGE_ROW_ID_BASE_NAME;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.$internal.guava.collect.ImmutableList.toImmutableList;

public class MongoMergeSink
        implements ConnectorMergeSink
{
    private static final String SET = "$set";

    private final int columnCount;

    private final ConnectorPageSink insertSink;
    private final ConnectorPageSink deleteSink;

    private final Map<Integer, Supplier<ConnectorPageSink>> updateSinkSuppliers;
    private final Map<Integer, int[]> updateCaseChannels;

    public MongoMergeSink(
            MongoSession mongoSession,
            RemoteTableName remoteTableName,
            List<MongoColumnHandle> columns,
            Map<Integer, Collection<ColumnHandle>> updateCaseColumns,
            MongoColumnHandle mergeColumnHandle,
            String implicitPrefix,
            Optional<String> pageSinkIdColumnName,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(mongoSession, "mongoSession is null");
        requireNonNull(remoteTableName, "remoteTableName is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(updateCaseColumns, "updateCaseColumns is null");
        requireNonNull(pageSinkId, "pageSinkId is null");
        requireNonNull(mergeColumnHandle, "mergeColumnHandle is null");

        this.columnCount = columns.size();

        this.insertSink = new MongoPageSink(mongoSession, remoteTableName, columns, implicitPrefix, pageSinkIdColumnName, pageSinkId);
        this.deleteSink = new MongoDeleteSink(mongoSession, remoteTableName, ImmutableList.of(mergeColumnHandle), implicitPrefix, pageSinkIdColumnName, pageSinkId);

        ImmutableMap.Builder<Integer, Supplier<ConnectorPageSink>> updateSinksBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, int[]> updateCaseChannelsBuilder = ImmutableMap.builder();
        for (Map.Entry<Integer, Collection<ColumnHandle>> entry : updateCaseColumns.entrySet()) {
            int caseNumber = entry.getKey();
            List<MongoColumnHandle> updateColumns = entry.getValue().stream()
                    .map(MongoColumnHandle.class::cast)
                    .collect(toImmutableList());
            Set<Integer> columnChannels = updateColumns.stream()
                    .map(columns::indexOf)
                    .collect(toImmutableSet());
            Supplier<ConnectorPageSink> updateSupplier = Suppliers.memoize(() -> createUpdateSink(
                    mongoSession,
                    remoteTableName,
                    updateColumns,
                    mergeColumnHandle,
                    implicitPrefix,
                    pageSinkIdColumnName,
                    pageSinkId));
            updateSinksBuilder.put(caseNumber, updateSupplier);
            updateCaseChannelsBuilder.put(caseNumber, columnChannels.stream().mapToInt(Integer::intValue).sorted().toArray());
        }
        this.updateSinkSuppliers = updateSinksBuilder.buildOrThrow();
        this.updateCaseChannels = updateCaseChannelsBuilder.buildOrThrow();
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkArgument(page.getChannelCount() == 3 + columnCount, "The page size should be 3 + columnCount (%s), but is %s", columnCount, page.getChannelCount());
        int positionCount = page.getPositionCount();
        Block operationBlock = page.getBlock(columnCount);
        Block rowId = page.getBlock(columnCount + 2);

        int[] dataChannel = IntStream.range(0, columnCount + 1).toArray();
        dataChannel[columnCount] = columnCount + 2;
        Page dataPage = page.getColumns(dataChannel);

        int[] insertPositions = new int[positionCount];
        int insertPositionCount = 0;
        int[] deletePositions = new int[positionCount];
        int deletePositionCount = 0;

        Block updateCaseBlock = page.getBlock(columnCount + 1);
        Map<Integer, int[]> updatePositions = new HashMap<>();
        Map<Integer, Integer> updatePositionCounts = new HashMap<>();

        for (int position = 0; position < positionCount; position++) {
            int operation = TINYINT.getByte(operationBlock, position);
            switch (operation) {
                case INSERT_OPERATION_NUMBER -> {
                    insertPositions[insertPositionCount] = position;
                    insertPositionCount++;
                }
                case DELETE_OPERATION_NUMBER -> {
                    deletePositions[deletePositionCount] = position;
                    deletePositionCount++;
                }
                case UPDATE_OPERATION_NUMBER -> {
                    int caseNumber = INTEGER.getInt(updateCaseBlock, position);
                    int updatePositionCount = updatePositionCounts.getOrDefault(caseNumber, 0);
                    updatePositions.computeIfAbsent(caseNumber, _ -> new int[positionCount])[updatePositionCount] = position;
                    updatePositionCounts.put(caseNumber, updatePositionCount + 1);
                }
                default -> throw new IllegalStateException("Unexpected value: " + operation);
            }
        }

        if (deletePositionCount > 0) {
            Block positions = rowId.getPositions(deletePositions, 0, deletePositionCount);
            deleteSink.appendPage(new Page(deletePositionCount, positions));
        }

        for (Map.Entry<Integer, Integer> entry : updatePositionCounts.entrySet()) {
            int caseNumber = entry.getKey();
            int updatePositionCount = entry.getValue();
            if (updatePositionCount > 0) {
                checkArgument(updatePositions.containsKey(caseNumber), "Unexpected case number %s", caseNumber);
                int[] positions = updatePositions.get(caseNumber);
                int[] updateAssignmentChannels = updateCaseChannels.get(caseNumber);
                Block[] updateBlocks = new Block[updateAssignmentChannels.length + 1];
                for (int channel = 0; channel < updateAssignmentChannels.length; channel++) {
                    updateBlocks[channel] = dataPage.getBlock(updateAssignmentChannels[channel]).getPositions(positions, 0, updatePositionCount);
                }
                updateBlocks[updateAssignmentChannels.length] = rowId.getPositions(positions, 0, updatePositionCount);

                updateSinkSuppliers.get(caseNumber).get().appendPage(new Page(updatePositionCount, updateBlocks));
            }
        }

        if (insertPositionCount > 0) {
            // Insert page should not include _id column by default, unless the insert columns include it explicitly
            Page insertPage = dataPage.getColumns(Arrays.copyOf(dataChannel, columnCount));
            insertSink.appendPage(insertPage.getPositions(insertPositions, 0, insertPositionCount));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        CompletableFuture<Collection<Slice>> finish = insertSink.finish();
        deleteSink.finish();
        updateSinkSuppliers.values().stream().map(Supplier::get).forEach(ConnectorPageSink::finish);

        return finish;
    }

    @Override
    public void abort()
    {
        insertSink.abort();
        deleteSink.abort();
        updateSinkSuppliers.values().stream().map(Supplier::get).forEach(ConnectorPageSink::abort);
    }

    private static ConnectorPageSink createUpdateSink(
            MongoSession mongoSession,
            RemoteTableName remoteTableName,
            Collection<MongoColumnHandle> columns,
            MongoColumnHandle mergeColumnHandle,
            String implicitPrefix,
            Optional<String> pageSinkIdColumnName,
            ConnectorPageSinkId pageSinkId)
    {
        // Update should always include id column explicitly
        List<MongoColumnHandle> updateColumns = ImmutableList.<MongoColumnHandle>builderWithExpectedSize(columns.size() + 1)
                .addAll(columns)
                .add(mergeColumnHandle)
                .build();
        return new MongoUpdateSink(mongoSession, remoteTableName, updateColumns, implicitPrefix, pageSinkIdColumnName, pageSinkId);
    }

    private static class MongoUpdateSink
            implements ConnectorPageSink
    {
        private final MongoPageSink delegate;
        private final MongoSession mongoSession;
        private final RemoteTableName remoteTableName;

        public MongoUpdateSink(
                MongoSession mongoSession,
                RemoteTableName remoteTableName,
                List<MongoColumnHandle> columns,
                String implicitPrefix,
                Optional<String> pageSinkIdColumnName,
                ConnectorPageSinkId pageSinkId)
        {
            this.delegate = new MongoPageSink(mongoSession, remoteTableName, columns, implicitPrefix, pageSinkIdColumnName, pageSinkId);
            this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
            this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            MongoCollection<Document> collection = mongoSession.getCollection(remoteTableName);
            List<WriteModel<Document>> bulkWrites = new ArrayList<>();
            for (Document document : delegate.buildBatchDocumentsFromPage(page)) {
                Document filter = new Document(MERGE_ROW_ID_BASE_NAME, document.get(MERGE_ROW_ID_BASE_NAME));
                bulkWrites.add(new UpdateOneModel<>(filter, new Document(SET, document)));
            }
            collection.bulkWrite(bulkWrites, new BulkWriteOptions().ordered(false));
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return delegate.finish();
        }

        @Override
        public void abort()
        {
            delegate.abort();
        }
    }

    private static class MongoDeleteSink
            implements ConnectorPageSink
    {
        private final MongoPageSink delegate;
        private final MongoSession mongoSession;
        private final RemoteTableName remoteTableName;

        public MongoDeleteSink(
                MongoSession mongoSession,
                RemoteTableName remoteTableName,
                List<MongoColumnHandle> columns,
                String implicitPrefix,
                Optional<String> pageSinkIdColumnName,
                ConnectorPageSinkId pageSinkId)
        {
            this.delegate = new MongoPageSink(mongoSession, remoteTableName, columns, implicitPrefix, pageSinkIdColumnName, pageSinkId);
            this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
            this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            MongoCollection<Document> collection = mongoSession.getCollection(remoteTableName);
            ImmutableList.Builder<Object> idsToDeleteBuilder = ImmutableList.builder();
            for (Document document : delegate.buildBatchDocumentsFromPage(page)) {
                idsToDeleteBuilder.add(document.get(MERGE_ROW_ID_BASE_NAME));
            }
            collection.deleteMany(in(MERGE_ROW_ID_BASE_NAME, idsToDeleteBuilder.build()));
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            return delegate.finish();
        }

        @Override
        public void abort()
        {
            delegate.abort();
        }
    }
}
