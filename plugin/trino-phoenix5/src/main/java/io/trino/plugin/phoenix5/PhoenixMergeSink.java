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
package io.trino.plugin.phoenix5;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSink;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.SinkSqlProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.phoenix.util.SchemaUtil;

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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY;
import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY_COLUMN_HANDLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.phoenix.util.SchemaUtil.getEscapedArgument;

public class PhoenixMergeSink
        implements ConnectorMergeSink
{
    private final boolean hasRowKey;
    private final int columnCount;

    private final ConnectorPageSink insertSink;
    private final Map<Integer, Supplier<ConnectorPageSink>> updateSinkSuppliers;
    private final ConnectorPageSink deleteSink;

    private final Map<Integer, Set<Integer>> updateCaseChannels;

    public PhoenixMergeSink(
            ConnectorSession session,
            ConnectorMergeTableHandle mergeHandle,
            PhoenixClient phoenixClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier,
            QueryBuilder queryBuilder)
    {
        PhoenixMergeTableHandle phoenixMergeTableHandle = (PhoenixMergeTableHandle) mergeHandle;
        PhoenixOutputTableHandle phoenixOutputTableHandle = phoenixMergeTableHandle.phoenixOutputTableHandle();
        this.hasRowKey = phoenixOutputTableHandle.rowkeyColumn().isPresent();
        this.columnCount = phoenixOutputTableHandle.getColumnNames().size();

        this.insertSink = new JdbcPageSink(session, phoenixOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier, JdbcClient::buildInsertSql);

        ImmutableList.Builder<String> mergeRowIdFieldNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> mergeRowIdFieldTypesBuilder = ImmutableList.builder();
        RowType mergeRowIdColumnType = (RowType) phoenixMergeTableHandle.mergeRowIdColumnHandle().getColumnType();
        for (RowType.Field field : mergeRowIdColumnType.getFields()) {
            checkArgument(field.getName().isPresent(), "Merge row id column field must have name");
            mergeRowIdFieldNamesBuilder.add(getEscapedArgument(field.getName().get()));
            mergeRowIdFieldTypesBuilder.add(field.getType());
        }
        List<String> mergeRowIdFieldNames = mergeRowIdFieldNamesBuilder.build();
        List<String> dataColumnNames = phoenixOutputTableHandle.getColumnNames().stream()
                .map(SchemaUtil::getEscapedArgument)
                .collect(toImmutableList());
        Set<Integer> mergeRowIdChannels = mergeRowIdFieldNames.stream()
                .map(dataColumnNames::indexOf)
                .collect(toImmutableSet());

        Map<Integer, Set<Integer>> updateCaseChannels = new HashMap<>();
        for (Map.Entry<Integer, Set<Integer>> entry : phoenixMergeTableHandle.updateCaseColumns().entrySet()) {
            updateCaseChannels.put(entry.getKey(), entry.getValue());
            if (!hasRowKey) {
                checkArgument(!mergeRowIdChannels.isEmpty() && !mergeRowIdChannels.contains(-1), "No primary keys found");
                updateCaseChannels.get(entry.getKey()).addAll(mergeRowIdChannels);
            }
        }
        this.updateCaseChannels = ImmutableMap.copyOf(updateCaseChannels);

        ImmutableMap.Builder<Integer, Supplier<ConnectorPageSink>> updateSinksBuilder = ImmutableMap.builder();
        for (Map.Entry<Integer, Set<Integer>> entry : this.updateCaseChannels.entrySet()) {
            int caseNumber = entry.getKey();
            Supplier<ConnectorPageSink> updateSupplier = Suppliers.memoize(() -> createUpdateSink(session, phoenixOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier, entry.getValue()));
            updateSinksBuilder.put(caseNumber, updateSupplier);
        }
        this.updateSinkSuppliers = updateSinksBuilder.buildOrThrow();

        this.deleteSink = createDeleteSink(session, mergeRowIdFieldTypesBuilder.build(), phoenixClient, phoenixMergeTableHandle, mergeRowIdFieldNames, pageSinkId, remoteQueryModifier, queryBuilder);
    }

    private static ConnectorPageSink createUpdateSink(
            ConnectorSession session,
            PhoenixOutputTableHandle phoenixOutputTableHandle,
            PhoenixClient phoenixClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier,
            Set<Integer> updateChannels)
    {
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        for (int channel = 0; channel < phoenixOutputTableHandle.getColumnNames().size(); channel++) {
            if (updateChannels.contains(channel)) {
                columnNamesBuilder.add(phoenixOutputTableHandle.getColumnNames().get(channel));
                columnTypesBuilder.add(phoenixOutputTableHandle.getColumnTypes().get(channel));
            }
        }
        if (phoenixOutputTableHandle.rowkeyColumn().isPresent()) {
            columnNamesBuilder.add(ROWKEY);
            columnTypesBuilder.add(ROWKEY_COLUMN_HANDLE.getColumnType());
        }

        PhoenixOutputTableHandle updateOutputTableHandle = new PhoenixOutputTableHandle(
                phoenixOutputTableHandle.getRemoteTableName(),
                columnNamesBuilder.build(),
                columnTypesBuilder.build(),
                Optional.empty(),
                Optional.empty());
        return new JdbcPageSink(session, updateOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier, JdbcClient::buildInsertSql);
    }

    private static ConnectorPageSink createDeleteSink(
            ConnectorSession session,
            List<Type> mergeRowIdFieldTypes,
            PhoenixClient phoenixClient,
            PhoenixMergeTableHandle tableHandle,
            List<String> mergeRowIdFieldNames,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier,
            QueryBuilder queryBuilder)
    {
        checkArgument(mergeRowIdFieldNames.size() == mergeRowIdFieldTypes.size(), "Wrong merge row column, columns and types size not match");
        JdbcOutputTableHandle deleteOutputTableHandle = new PhoenixOutputTableHandle(
                tableHandle.phoenixOutputTableHandle().getRemoteTableName(),
                mergeRowIdFieldNames,
                mergeRowIdFieldTypes,
                Optional.empty(),
                Optional.empty());

        return new JdbcPageSink(session, deleteOutputTableHandle, phoenixClient, pageSinkId, remoteQueryModifier, deleteSinkProvider(session, tableHandle, phoenixClient, queryBuilder));
    }

    private static SinkSqlProvider deleteSinkProvider(
            ConnectorSession session,
            PhoenixMergeTableHandle handle,
            JdbcClient jdbcClient,
            QueryBuilder queryBuilder)
    {
        try (Connection connection = jdbcClient.getConnection(session)) {
            return (_, _, _) -> queryBuilder.prepareDeleteQuery(
                            jdbcClient,
                            session,
                            connection,
                            handle.tableHandle().getRequiredNamedRelation(),
                            handle.primaryKeysDomain(),
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
            Block[] deleteBlocks = new Block[rowIdFields.size()];
            for (int field = 0; field < rowIdFields.size(); field++) {
                deleteBlocks[field] = rowIdFields.get(field).getPositions(deletePositions, 0, deletePositionCount);
            }
            deleteSink.appendPage(new Page(deletePositionCount, deleteBlocks));
        }

        for (Map.Entry<Integer, Integer> entry : updatePositionCounts.entrySet()) {
            int caseNumber = entry.getKey();
            int updatePositionCount = entry.getValue();
            if (updatePositionCount > 0) {
                checkArgument(updatePositions.containsKey(caseNumber), "Unexpected case number %s", caseNumber);

                Page updatePage = dataPage
                        .getColumns(updateCaseChannels.get(caseNumber).stream().mapToInt(Integer::intValue).sorted().toArray())
                        .getPositions(updatePositions.get(caseNumber), 0, updatePositionCount);
                if (hasRowKey) {
                    updatePage = updatePage.appendColumn(rowIdFields.get(0).getPositions(updatePositions.get(caseNumber), 0, updatePositionCount));
                }

                updateSinkSuppliers.get(caseNumber).get().appendPage(updatePage);
            }
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        insertSink.finish();
        deleteSink.finish();
        updateSinkSuppliers.values().stream().map(Supplier::get).forEach(ConnectorPageSink::finish);
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        insertSink.abort();
        deleteSink.abort();
        updateSinkSuppliers.values().stream().map(Supplier::get).forEach(ConnectorPageSink::abort);
    }
}
