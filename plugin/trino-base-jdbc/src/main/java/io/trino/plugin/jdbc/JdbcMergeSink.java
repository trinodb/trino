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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JdbcMergeSink
        implements ConnectorMergeSink
{
    private final int columnCount;
    private final int[] updateAssignmentChannels;
    private final ConnectorPageSinkId pageSinkId;

    private final ConnectorPageSink insertSink;
    private final ConnectorPageSink updateSink;
    private final ConnectorPageSink deleteSink;

    public JdbcMergeSink(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeTableHandle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier queryModifier,
            QueryBuilder queryBuilder)
    {
        requireNonNull(session, "session is null");
        requireNonNull(mergeTableHandle, "mergeTableHandle is null");
        requireNonNull(jdbcClient, "jdbcClient is null");
        requireNonNull(queryModifier, "queryModifier is null");
        requireNonNull(queryBuilder, "queryBuilder is null");

        JdbcMergeTableHandle mergeHandle = (JdbcMergeTableHandle) mergeTableHandle;
        JdbcOutputTableHandle outputHandle = mergeHandle.getOutputTableHandle();
        List<JdbcColumnHandle> primaryKeys = mergeHandle.getPrimaryKeys();
        checkArgument(!primaryKeys.isEmpty(), "primary keys not exists");

        this.columnCount = outputHandle.getColumnNames().size();

        this.updateAssignmentChannels = getUpdateAssignmentChannels(outputHandle, primaryKeys);

        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");

        // The TupleDomain for building the conjuncts of the primary keys
        ImmutableMap.Builder<ColumnHandle, Domain> primaryKeysDomainBuilder = ImmutableMap.builder();
        // This value is to build the TupleDomain, but it won't affect the query field in result of the `QueryBuilder#prepareDeleteQuery` and `QueryBuilder#prepareUpdateQuery`
        Domain dummy = Domain.singleValue(BIGINT, 0L);
        for (JdbcColumnHandle columnHandle : primaryKeys) {
            primaryKeysDomainBuilder.put(columnHandle, dummy);
        }
        TupleDomain<ColumnHandle> primaryKeysDomain = TupleDomain.withColumnDomains(primaryKeysDomainBuilder.buildOrThrow());
        JdbcNamedRelationHandle relation = mergeHandle.getTableHandle().getRequiredNamedRelation();
        this.insertSink = new JdbcPageSink(session, outputHandle, jdbcClient, pageSinkId, queryModifier, JdbcClient::buildInsertSql);
        this.updateSink = createUpdateSink(session, relation, primaryKeysDomain, primaryKeys, jdbcClient, pageSinkId, queryModifier, queryBuilder);
        this.deleteSink = createDeleteSink(session, relation, primaryKeysDomain, primaryKeys, jdbcClient, pageSinkId, queryModifier, queryBuilder);
    }

    private static int[] getUpdateAssignmentChannels(JdbcOutputTableHandle outputTableHandle, List<JdbcColumnHandle> primaryKeys)
    {
        Set<String> primaryKeyNames = primaryKeys.stream()
                .map(JdbcColumnHandle::getColumnName)
                .collect(toImmutableSet());
        List<String> allDataColumns = outputTableHandle.getColumnNames();

        ImmutableList.Builder<Integer> updateAssignmentChannelBuilder = ImmutableList.builder();
        for (int channel = 0; channel < allDataColumns.size(); channel++) {
            String column = allDataColumns.get(channel);
            if (!primaryKeyNames.contains(column)) {
                updateAssignmentChannelBuilder.add(channel);
            }
        }
        return updateAssignmentChannelBuilder.build().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private static ConnectorPageSink createUpdateSink(
            ConnectorSession session,
            JdbcNamedRelationHandle relation,
            TupleDomain<ColumnHandle> domain,
            List<JdbcColumnHandle> primaryKeys,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier,
            QueryBuilder queryBuilder)
    {
        List<JdbcColumnHandle> columns = jdbcClient.getColumns(session, new JdbcTableHandle(relation.getSchemaTableName(), relation.getRemoteTableName(), Optional.empty()));

        ImmutableList.Builder<JdbcAssignmentItem> assignmentItemBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        QueryParameter dummy = new QueryParameter(BIGINT, Optional.empty());
        for (JdbcColumnHandle columnHandle : columns) {
            if (!primaryKeys.contains(columnHandle)) {
                columnNamesBuilder.add(columnHandle.getColumnName());
                columnTypesBuilder.add(columnHandle.getColumnType());
                assignmentItemBuilder.add(new JdbcAssignmentItem(columnHandle, dummy));
            }
        }
        for (JdbcColumnHandle columnHandle : primaryKeys) {
            columnNamesBuilder.add(columnHandle.getColumnName());
            columnTypesBuilder.add(columnHandle.getColumnType());
        }

        return new JdbcPageSink(
                session,
                new JdbcOutputTableHandle(
                        relation.getRemoteTableName(),
                        columnNamesBuilder.build(),
                        columnTypesBuilder.build(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                jdbcClient,
                pageSinkId,
                remoteQueryModifier,
                updateSqlProvider(session, relation, domain, assignmentItemBuilder.build(), jdbcClient, queryBuilder));
    }

    private static SinkSqlProvider updateSqlProvider(
            ConnectorSession session,
            JdbcNamedRelationHandle relation,
            TupleDomain<ColumnHandle> domain,
            List<JdbcAssignmentItem> assignmentItems,
            JdbcClient jdbcClient,
            QueryBuilder queryBuilder)
    {
        try (Connection connection = jdbcClient.getConnection(session)) {
            return (_, _, _) -> queryBuilder.prepareUpdateQuery(
                            jdbcClient,
                            session,
                            connection,
                            relation,
                            domain,
                            Optional.empty(),
                            assignmentItems)
                    .query();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static ConnectorPageSink createDeleteSink(
            ConnectorSession session,
            JdbcNamedRelationHandle relation,
            TupleDomain<ColumnHandle> domain,
            List<JdbcColumnHandle> primaryKeys,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier,
            QueryBuilder queryBuilder)
    {
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        for (JdbcColumnHandle columnHandle : primaryKeys) {
            columnNamesBuilder.add(columnHandle.getColumnName());
            columnTypesBuilder.add(columnHandle.getColumnType());
        }
        return new JdbcPageSink(
                session,
                new JdbcOutputTableHandle(
                        relation.getRemoteTableName(),
                        columnNamesBuilder.build(),
                        columnTypesBuilder.build(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                jdbcClient,
                pageSinkId,
                remoteQueryModifier,
                deleteSqlProvider(session, relation, domain, jdbcClient, queryBuilder));
    }

    private static SinkSqlProvider deleteSqlProvider(
            ConnectorSession session,
            JdbcNamedRelationHandle relation,
            TupleDomain<ColumnHandle> domain,
            JdbcClient jdbcClient,
            QueryBuilder queryBuilder)
    {
        try (Connection connection = jdbcClient.getConnection(session)) {
            return (_, _, _) -> queryBuilder.prepareDeleteQuery(
                            jdbcClient,
                            session,
                            connection,
                            relation,
                            domain,
                            Optional.empty())
                    .query();
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
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

        if (insertPositionCount > 0) {
            insertSink.appendPage(dataPage.getPositions(insertPositions, 0, insertPositionCount));
        }

        List<Block> rowIdFields = RowBlock.getRowFieldsFromBlock(page.getBlock(columnCount + 1));
        if (deletePositionCount > 0) {
            Block[] deleteBlocks = new Block[rowIdFields.size()];
            for (int field = 0; field < rowIdFields.size(); field++) {
                deleteBlocks[field] = rowIdFields.get(field).getPositions(deletePositions, 0, deletePositionCount);
            }
            deleteSink.appendPage(new Page(deletePositionCount, deleteBlocks));
        }

        if (updatePositionCount > 0) {
            Block[] updateBlocks = new Block[updateAssignmentChannels.length + rowIdFields.size()];
            for (int channel = 0; channel < updateAssignmentChannels.length; channel++) {
                updateBlocks[channel] = dataPage.getBlock(updateAssignmentChannels[channel]).getPositions(updatePositions, 0, updatePositionCount);
            }
            for (int field = 0; field < rowIdFields.size(); field++) {
                updateBlocks[updateAssignmentChannels.length + field] = rowIdFields.get(field).getPositions(updatePositions, 0, updatePositionCount);
            }
            updateSink.appendPage(new Page(updatePositionCount, updateBlocks));
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
