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
package io.trino.plugin.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JdbcMergeSink
        implements ConnectorMergeSink
{
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final int columnCount;
    private final String mergeRowIdConjuncts;
    private final List<String> mergeRowIdFieldNames;
    private final List<Type> mergeRowIdFieldTypes;
    private final List<Integer> reservedChannelsForUpdate;

    private final ConnectorPageSinkId pageSinkId;
    private final ConnectorPageSink insertSink;
    private final ConnectorPageSink updateSink;
    private final ConnectorPageSink deleteSink;

    public JdbcMergeSink(
            ConnectorSession session,
            JdbcClient jdbcClient,
            RemoteQueryModifier remoteQueryModifier,
            ConnectorMergeTableHandle mergeHandle,
            ConnectorPageSinkId pageSinkId)
    {
        JdbcMergeTableHandle jdbcMergeTableHandle = (JdbcMergeTableHandle) mergeHandle;
        JdbcOutputTableHandle jdbcOutputTableHandle = jdbcMergeTableHandle.getOutputTableHandle();
        this.catalogName = jdbcOutputTableHandle.getCatalogName();
        this.schemaName = jdbcOutputTableHandle.getSchemaName();
        this.tableName = jdbcOutputTableHandle.getTableName();
        this.columnCount = jdbcOutputTableHandle.getColumnNames().size();

        ImmutableList.Builder<String> mergeRowIdFieldNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> mergeRowIdFieldTypesBuilder = ImmutableList.builder();
        RowType mergeRowIdColumnType = (RowType) jdbcMergeTableHandle.mergeRowIdColumnHandle().getColumnType();
        for (RowType.Field field : mergeRowIdColumnType.getFields()) {
            checkArgument(field.getName().isPresent(), "Merge row id column field must have name");
            mergeRowIdFieldNamesBuilder.add(field.getName().get());
            mergeRowIdFieldTypesBuilder.add(field.getType());
        }
        this.mergeRowIdFieldNames = mergeRowIdFieldNamesBuilder.build();
        this.mergeRowIdFieldTypes = mergeRowIdFieldTypesBuilder.build();
        verify(!mergeRowIdFieldNames.isEmpty() && mergeRowIdFieldNames.size() == mergeRowIdFieldTypes.size());
        this.mergeRowIdConjuncts = buildMergeRowIdConjuncts(session, jdbcClient);
        this.reservedChannelsForUpdate = getReservedChannelsForUpdate(jdbcOutputTableHandle);
        verify(!reservedChannelsForUpdate.isEmpty(), "Update primary key itself is not supported");

        this.pageSinkId = pageSinkId;
        this.insertSink = createInsertSink(session, jdbcOutputTableHandle, jdbcClient, pageSinkId, remoteQueryModifier);
        this.updateSink = createUpdateSink(session, jdbcOutputTableHandle, jdbcClient, pageSinkId, remoteQueryModifier);
        this.deleteSink = createDeleteSink(session, jdbcMergeTableHandle.getDeleteTableHandle(), jdbcClient, pageSinkId, remoteQueryModifier);
    }

    protected String buildMergeRowIdConjuncts(ConnectorSession session, JdbcClient jdbcClient)
    {
        return jdbcClient.buildMergeRowIdConjuncts(session, mergeRowIdFieldNames, mergeRowIdFieldTypes);
    }

    public List<Integer> getReservedChannelsForUpdate(JdbcOutputTableHandle outputTableHandle)
    {
        Set<String> excludedColumnNames = ImmutableSet.copyOf(mergeRowIdFieldNames);
        List<String> allDataColumns = outputTableHandle.getColumnNames();
        ImmutableList.Builder<Integer> reservedChannelsBuilder = ImmutableList.builder();
        for (int channel = 0; channel < allDataColumns.size(); channel++) {
            String column = allDataColumns.get(channel);
            if (!excludedColumnNames.contains(column)) {
                reservedChannelsBuilder.add(channel);
            }
        }

        return reservedChannelsBuilder.build();
    }

    protected ConnectorPageSink createInsertSink(
            ConnectorSession session,
            JdbcOutputTableHandle jdbcOutputTableHandle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier)
    {
        return new JdbcPageSink(session, jdbcOutputTableHandle, jdbcClient, pageSinkId, remoteQueryModifier);
    }

    protected ConnectorPageSink createUpdateSink(
            ConnectorSession session,
            JdbcOutputTableHandle jdbcOutputTableHandle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier)
    {
        ImmutableList.Builder<String> updateColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> updateColumnTypes = ImmutableList.builder();
        for (int channel : reservedChannelsForUpdate) {
            updateColumnNames.add(jdbcOutputTableHandle.getColumnNames().get(channel));
            updateColumnTypes.add(jdbcOutputTableHandle.getColumnTypes().get(channel));
        }
        updateColumnNames.addAll(mergeRowIdFieldNames);
        updateColumnTypes.addAll(mergeRowIdFieldTypes);

        JdbcOutputTableHandle updateOutputTableHandle = new JdbcOutputTableHandle(
                catalogName,
                schemaName,
                tableName,
                updateColumnNames.build(),
                updateColumnTypes.build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new UpdateSink(session, updateOutputTableHandle, jdbcClient, pageSinkId, remoteQueryModifier);
    }

    protected ConnectorPageSink createDeleteSink(
            ConnectorSession session,
            Optional<JdbcOutputTableHandle> jdbcDeleteOutputTableHandle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier)
    {
        if (jdbcDeleteOutputTableHandle.isPresent()) {
            return new JdbcPageSink(session, jdbcDeleteOutputTableHandle.get(), jdbcClient, pageSinkId, remoteQueryModifier);
        }

        JdbcOutputTableHandle deleteOutputTableHandle = new JdbcOutputTableHandle(
                catalogName,
                schemaName,
                tableName,
                mergeRowIdFieldNames,
                mergeRowIdFieldTypes,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return new DeleteSink(session, deleteOutputTableHandle, jdbcClient, pageSinkId, remoteQueryModifier);
    }

    private class UpdateSink
            extends JdbcPageSink
    {
        public UpdateSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient, ConnectorPageSinkId pageSinkId, RemoteQueryModifier remoteQueryModifier)
        {
            super(session, handle, jdbcClient, pageSinkId, remoteQueryModifier);
        }

        @Override
        protected String getSinkSql(JdbcClient jdbcClient, JdbcOutputTableHandle outputTableHandle, List<WriteFunction> columnWriters)
        {
            List<String> columnNames = outputTableHandle.getColumnNames();
            checkArgument(columnNames.size() > mergeRowIdFieldNames.size(), "Update primary key itself is not supported");
            checkArgument(columnNames.size() == columnWriters.size(), "handle and columnWriters mismatch: %s, %s", outputTableHandle, columnWriters);

            ImmutableList.Builder<String> updateConjunctsBuilder = ImmutableList.builder();
            for (int i = 0; i < columnWriters.size() - mergeRowIdFieldNames.size(); i++) {
                updateConjunctsBuilder.add(jdbcClient.quoted(columnNames.get(i)) + " = " + columnWriters.get(i).getBindExpression());
            }
            return "UPDATE %s.%s SET %s WHERE %s".formatted(schemaName, tableName, Joiner.on(", ").join(updateConjunctsBuilder.build()), mergeRowIdConjuncts);
        }
    }

    private class DeleteSink
            extends JdbcPageSink
    {
        public DeleteSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient, ConnectorPageSinkId pageSinkId, RemoteQueryModifier remoteQueryModifier)
        {
            super(session, handle, jdbcClient, pageSinkId, remoteQueryModifier);
        }

        @Override
        protected String getSinkSql(JdbcClient jdbcClient, JdbcOutputTableHandle outputTableHandle, List<WriteFunction> columnWriters)
        {
            return "DELETE FROM %s.%s WHERE %s".formatted(schemaName, tableName, mergeRowIdConjuncts);
        }
    }

    @Override
    public void storeMergedRows(Page page)
    {
        checkArgument(page.getChannelCount() == 2 + columnCount, "The page size should be 2 + columnCount (%s), but is %s", columnCount, page.getChannelCount());
        int positionCount = page.getPositionCount();
        Block operationBlock = page.getBlock(columnCount);

        int[] dataChannel = IntStream.range(0, columnCount).toArray();
        Page dataPage = page.getColumns(dataChannel);

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

        List<Block> rowIdFields = RowBlock.getRowFieldsFromBlock(page.getBlock(columnCount + 1));
        appendInsertPage(dataPage, insertPositions, insertPositionCount);
        appendDeletePage(rowIdFields, deletePositions, deletePositionCount);
        appendUpdatePage(dataPage, rowIdFields, updatePositions, updatePositionCount);
    }

    public void appendUpdatePage(Page dataPage, List<Block> rowIdFields, int[] updatePositions, int updatePositionCount)
    {
        if (updatePositionCount > 0) {
            dataPage = dataPage.getColumns(reservedChannelsForUpdate.stream().mapToInt(Integer::intValue).toArray());
            int columnCount = dataPage.getChannelCount();
            Block[] updateBlocks = new Block[columnCount + rowIdFields.size()];
            for (int channel = 0; channel < columnCount; channel++) {
                updateBlocks[channel] = dataPage.getBlock(channel).getPositions(updatePositions, 0, updatePositionCount);
            }
            for (int field = 0; field < rowIdFields.size(); field++) {
                updateBlocks[field + columnCount] = rowIdFields.get(field).getPositions(updatePositions, 0, updatePositionCount);
            }

            updateSink.appendPage(new Page(updatePositionCount, updateBlocks));
        }
    }

    public void appendDeletePage(List<Block> rowIdFields, int[] deletePositions, int deletePositionCount)
    {
        if (deletePositionCount > 0) {
            Block[] deleteBlocks = new Block[rowIdFields.size()];
            for (int field = 0; field < rowIdFields.size(); field++) {
                deleteBlocks[field] = rowIdFields.get(field).getPositions(deletePositions, 0, deletePositionCount);
            }

            deleteSink.appendPage(new Page(deletePositionCount, deleteBlocks));
        }
    }

    public void appendInsertPage(Page dataPage, int[] insertPositions, int insertPositionCount)
    {
        if (insertPositionCount > 0) {
            insertSink.appendPage(dataPage.getPositions(insertPositions, 0, insertPositionCount));
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        insertSink.finish();
        deleteSink.finish();
        updateSink.finish();

        // pass the successful page sink id
        Slice value = Slices.allocate(Long.BYTES);
        value.setLong(0, pageSinkId.getId());
        return completedFuture(ImmutableList.of(value));
    }

    @Override
    public void abort()
    {
        insertSink.abort();
        deleteSink.abort();
        updateSink.abort();
    }
}
