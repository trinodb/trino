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

import com.google.common.base.Suppliers;
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
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class JdbcMergeSink
        implements ConnectorMergeSink
{
    private final int columnCount;
    private final ConnectorPageSinkId pageSinkId;

    private final ConnectorPageSink insertSink;
    private final ConnectorPageSink deleteSink;

    private final Map<Integer, Supplier<ConnectorPageSink>> updateSinkSuppliers;
    private final Map<Integer, int[]> updateCaseChannels;

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

        this.pageSinkId = requireNonNull(pageSinkId, "pageSinkId is null");

        // The TupleDomain for building the conjuncts of the primary keys
        ImmutableMap.Builder<ColumnHandle, Domain> primaryKeysDomainBuilder = ImmutableMap.builder();
        // The value won't be used as the real value will be passed to storeMergedRows
        Domain dummy = Domain.singleValue(BIGINT, 0L);
        for (JdbcColumnHandle columnHandle : primaryKeys) {
            primaryKeysDomainBuilder.put(columnHandle, dummy);
        }
        TupleDomain<ColumnHandle> primaryKeysDomain = TupleDomain.withColumnDomains(primaryKeysDomainBuilder.buildOrThrow());
        JdbcNamedRelationHandle relation = mergeHandle.getTableHandle().getRequiredNamedRelation();
        this.insertSink = new JdbcPageSink(session, outputHandle, jdbcClient, pageSinkId, queryModifier, JdbcClient::buildInsertSql);
        this.deleteSink = createDeleteSink(session, relation, primaryKeysDomain, primaryKeys, jdbcClient, pageSinkId, queryModifier, queryBuilder);

        Map<Integer, Collection<ColumnHandle>> updateCaseColumns = mergeHandle.getUpdateCaseColumns();
        List<JdbcColumnHandle> columns = mergeHandle.getDataColumns();

        ImmutableMap.Builder<Integer, Supplier<ConnectorPageSink>> updateSinksBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Integer, int[]> updateCaseChannelsBuilder = ImmutableMap.builder();
        for (Map.Entry<Integer, Collection<ColumnHandle>> entry : updateCaseColumns.entrySet()) {
            int caseNumber = entry.getKey();
            Set<Integer> columnChannels = entry.getValue().stream()
                    .map(JdbcColumnHandle.class::cast)
                    .map(columns::indexOf)
                    .collect(toImmutableSet());
            Supplier<ConnectorPageSink> updateSupplier = Suppliers.memoize(() -> createUpdateSink(
                    session,
                    relation,
                    primaryKeysDomain,
                    primaryKeys,
                    jdbcClient,
                    pageSinkId,
                    queryModifier,
                    queryBuilder,
                    columns,
                    columnChannels));
            updateSinksBuilder.put(caseNumber, updateSupplier);
            updateCaseChannelsBuilder.put(caseNumber, columnChannels.stream().mapToInt(Integer::intValue).sorted().toArray());
        }
        this.updateSinkSuppliers = updateSinksBuilder.buildOrThrow();
        this.updateCaseChannels = updateCaseChannelsBuilder.buildOrThrow();
    }

    private static ConnectorPageSink createUpdateSink(
            ConnectorSession session,
            JdbcNamedRelationHandle relation,
            TupleDomain<ColumnHandle> domain,
            List<JdbcColumnHandle> primaryKeys,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier,
            QueryBuilder queryBuilder,
            List<JdbcColumnHandle> columns,
            Set<Integer> updateChannels)
    {
        ImmutableList.Builder<JdbcAssignmentItem> assignmentItemBuilder = ImmutableList.builder();
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        // The value won't be used as the real value will be passed to storeMergedRows
        QueryParameter dummy = new QueryParameter(BIGINT, Optional.empty());
        for (int channel = 0; channel < columns.size(); channel++) {
            JdbcColumnHandle columnHandle = columns.get(channel);
            if (updateChannels.contains(channel)) {
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
        checkArgument(page.getChannelCount() == 3 + columnCount, "The page size should be 3 + columnCount (%s), but is %s", columnCount, page.getChannelCount());
        int positionCount = page.getPositionCount();
        Block operationBlock = page.getBlock(columnCount);

        int[] dataChannel = IntStream.range(0, columnCount).toArray();
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

        if (insertPositionCount > 0) {
            insertSink.appendPage(dataPage.getPositions(insertPositions, 0, insertPositionCount));
        }

        List<Block> rowIdFields = RowBlock.getRowFieldsFromBlock(page.getBlock(columnCount + 2));
        if (deletePositionCount > 0) {
            deleteSink.appendPage(new Page(deletePositionCount, extractRowIdBlocks(rowIdFields, deletePositionCount, deletePositions)));
        }

        for (Map.Entry<Integer, Integer> entry : updatePositionCounts.entrySet()) {
            int caseNumber = entry.getKey();
            int updatePositionCount = entry.getValue();
            if (updatePositionCount > 0) {
                checkArgument(updatePositions.containsKey(caseNumber), "Unexpected case number %s", caseNumber);
                int[] positions = updatePositions.get(caseNumber);
                int[] updateAssignmentChannels = updateCaseChannels.get(caseNumber);
                Block[] updateBlocks = new Block[updateAssignmentChannels.length + rowIdFields.size()];
                for (int channel = 0; channel < updateAssignmentChannels.length; channel++) {
                    updateBlocks[channel] = dataPage.getBlock(updateAssignmentChannels[channel]).getPositions(positions, 0, updatePositionCount);
                }
                System.arraycopy(extractRowIdBlocks(rowIdFields, updatePositionCount, positions), 0, updateBlocks, updateAssignmentChannels.length, rowIdFields.size());

                updateSinkSuppliers.get(caseNumber).get().appendPage(new Page(updatePositionCount, updateBlocks));
            }
        }
    }

    private static Block[] extractRowIdBlocks(List<Block> rowIdFields, int positionCount, int[] positions)
    {
        Block[] blocks = new Block[rowIdFields.size()];
        for (int field = 0; field < rowIdFields.size(); field++) {
            blocks[field] = rowIdFields.get(field).getPositions(positions, 0, positionCount);
        }
        return blocks;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        insertSink.finish();
        deleteSink.finish();
        updateSinkSuppliers.values().stream().map(Supplier::get).forEach(ConnectorPageSink::finish);

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
        updateSinkSuppliers.values().stream().map(Supplier::get).forEach(ConnectorPageSink::abort);
    }
}
