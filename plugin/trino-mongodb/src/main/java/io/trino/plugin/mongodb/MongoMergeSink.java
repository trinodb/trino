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

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.mongodb.client.model.Filters.in;
import static io.trino.plugin.mongodb.MongoMetadata.MERGE_ROW_ID_BASE_NAME;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

public class MongoMergeSink
        implements ConnectorMergeSink
{
    private static final String SET = "$set";

    private final MongoPageSink insertSink;
    private final MongoPageSink updateSink;
    private final MongoPageSink deleteSink;
    private final int columnCount;

    public MongoMergeSink(
            MongoSession mongoSession,
            RemoteTableName remoteTableName,
            List<MongoColumnHandle> columns,
            MongoColumnHandle mergeColumnHandle,
            Optional<MongoOutputTableHandle> deleteOutputTableHandle,
            Optional<MongoOutputTableHandle> updateOutputTableHandle,
            String implicitPrefix,
            Optional<String> pageSinkIdColumnName,
            ConnectorPageSinkId pageSinkId)
    {
        requireNonNull(mongoSession, "mongoSession is null");
        requireNonNull(remoteTableName, "remoteTableName is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(pageSinkId, "pageSinkId is null");
        requireNonNull(mergeColumnHandle, "mergeColumnHandle is null");

        this.insertSink = new MongoPageSink(mongoSession, remoteTableName, columns, implicitPrefix, pageSinkIdColumnName, pageSinkId);
        this.deleteSink = createDeleteSink(mongoSession, deleteOutputTableHandle, remoteTableName, mergeColumnHandle, implicitPrefix, pageSinkIdColumnName, pageSinkId);

        // Update should always include id column explicitly
        List<MongoColumnHandle> updateColumns = ImmutableList.<MongoColumnHandle>builderWithExpectedSize(columns.size() + 1)
                .addAll(columns)
                .add(mergeColumnHandle)
                .build();
        this.updateSink = createUpdateSink(mongoSession, updateOutputTableHandle, updateColumns, remoteTableName, implicitPrefix, pageSinkIdColumnName, pageSinkId);

        this.columnCount = columns.size();
    }

    private MongoPageSink createDeleteSink(
            MongoSession mongoSession,
            Optional<MongoOutputTableHandle> outputTableHandle,
            RemoteTableName remoteTableName,
            MongoColumnHandle mergeColumnHandle,
            String implicitPrefix,
            Optional<String> pageSinkIdColumnName,
            ConnectorPageSinkId pageSinkId)
    {
        if (outputTableHandle.isEmpty()) {
            return new MongoDeleteSink(mongoSession, remoteTableName, ImmutableList.of(mergeColumnHandle), implicitPrefix, pageSinkIdColumnName, pageSinkId);
        }

        MongoOutputTableHandle mongoOutputTableHandle = outputTableHandle.get();
        checkState(mongoOutputTableHandle.getTemporaryRemoteTableName().isPresent(), "temporary table not exist");

        return new MongoPageSink(mongoSession,
                mongoOutputTableHandle.getTemporaryRemoteTableName().get(),
                mongoOutputTableHandle.columns(),
                implicitPrefix,
                pageSinkIdColumnName,
                pageSinkId);
    }

    private MongoPageSink createUpdateSink(
            MongoSession mongoSession,
            Optional<MongoOutputTableHandle> outputTableHandle,
            List<MongoColumnHandle> columnHandles,
            RemoteTableName remoteTableName,
            String implicitPrefix,
            Optional<String> pageSinkIdColumnName,
            ConnectorPageSinkId pageSinkId)
    {
        if (outputTableHandle.isEmpty()) {
            return new MongoUpdateSink(mongoSession, remoteTableName, columnHandles, implicitPrefix, pageSinkIdColumnName, pageSinkId);
        }

        MongoOutputTableHandle mongoOutputTableHandle = outputTableHandle.get();
        checkState(mongoOutputTableHandle.getTemporaryRemoteTableName().isPresent(), "temporary table not exist");

        return new MongoPageSink(
                mongoSession,
                mongoOutputTableHandle.getTemporaryRemoteTableName().get(),
                mongoOutputTableHandle.columns(),
                implicitPrefix,
                pageSinkIdColumnName,
                pageSinkId);
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkArgument(page.getChannelCount() == 2 + columnCount, "The page size should be 2 + columnCount (%s), but is %s", columnCount, page.getChannelCount());

        Block operationBlock = page.getBlock(columnCount);
        Block rowId = page.getBlock(columnCount + 1);

        int[] dataChannel = IntStream.range(0, columnCount + 1).toArray();
        dataChannel[columnCount] = columnCount + 1;
        Page dataPage = page.getColumns(dataChannel);

        int positionCount = page.getPositionCount();
        int[] insertPositions = new int[positionCount];
        int insertPositionCount = 0;
        int[] deletePositions = new int[positionCount];
        int deletePositionCount = 0;
        int[] updatePositions = new int[positionCount];
        int updatePositionCount = 0;

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
                    updatePositions[updatePositionCount] = position;
                    updatePositionCount++;
                }
                default -> throw new IllegalStateException("Unexpected value: " + operation);
            }
        }

        if (deletePositionCount > 0) {
            Block positions = rowId.getPositions(deletePositions, 0, deletePositionCount);
            deleteSink.appendPage(new Page(deletePositionCount, positions));
        }

        if (updatePositionCount > 0) {
            updateSink.appendPage(dataPage.getPositions(updatePositions, 0, updatePositionCount));
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
        return insertSink.finish();
    }

    private static class MongoUpdateSink
            extends MongoPageSink
    {
        private final MongoSession mongoSession;
        private final RemoteTableName remoteTableName;

        public MongoUpdateSink(MongoSession mongoSession, RemoteTableName remoteTableName, List<MongoColumnHandle> columns, String implicitPrefix, Optional<String> pageSinkIdColumnName, ConnectorPageSinkId pageSinkId)
        {
            super(mongoSession, remoteTableName, columns, implicitPrefix, pageSinkIdColumnName, pageSinkId);
            this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
            this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            MongoCollection<Document> collection = mongoSession.getCollection(remoteTableName);
            List<WriteModel<Document>> bulkWrites = new ArrayList<>();
            for (Document document : buildBatchDocumentsFromPage(page)) {
                Document filter = new Document(MERGE_ROW_ID_BASE_NAME, document.get(MERGE_ROW_ID_BASE_NAME));
                bulkWrites.add(new UpdateOneModel<>(filter, new Document(SET, document)));
            }
            collection.bulkWrite(bulkWrites, new BulkWriteOptions().ordered(false));
            return NOT_BLOCKED;
        }
    }

    private static class MongoDeleteSink
            extends MongoPageSink
    {
        private final MongoSession mongoSession;
        private final RemoteTableName remoteTableName;

        public MongoDeleteSink(MongoSession mongoSession, RemoteTableName remoteTableName, List<MongoColumnHandle> columns, String implicitPrefix, Optional<String> pageSinkIdColumnName, ConnectorPageSinkId pageSinkId)
        {
            super(mongoSession, remoteTableName, columns, implicitPrefix, pageSinkIdColumnName, pageSinkId);
            this.mongoSession = requireNonNull(mongoSession, "mongoSession is null");
            this.remoteTableName = requireNonNull(remoteTableName, "remoteTableName is null");
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            MongoCollection<Document> collection = mongoSession.getCollection(remoteTableName);
            ImmutableList.Builder<Object> idsToDeleteBuilder = ImmutableList.builder();
            for (Document document : buildBatchDocumentsFromPage(page)) {
                idsToDeleteBuilder.add(document.get(MERGE_ROW_ID_BASE_NAME));
            }
            collection.deleteMany(in(MERGE_ROW_ID_BASE_NAME, idsToDeleteBuilder.build()));
            return NOT_BLOCKED;
        }
    }
}
