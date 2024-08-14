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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMergeSink;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcPageSink;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY;
import static io.trino.plugin.phoenix5.PhoenixClient.ROWKEY_COLUMN_HANDLE;

public class PhoenixMergeSink
        extends JdbcMergeSink
{
    private final boolean hasRowKey;

    public PhoenixMergeSink(PhoenixClient phoenixClient, RemoteQueryModifier remoteQueryModifier, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        super(session, phoenixClient, remoteQueryModifier, mergeHandle, pageSinkId);
        PhoenixMergeTableHandle mergeTableHandle = (PhoenixMergeTableHandle) mergeHandle;
        this.hasRowKey = mergeTableHandle.isHasRowKey();
    }

    @Override
    protected ConnectorPageSink createUpdateSink(
            ConnectorSession session,
            JdbcOutputTableHandle jdbcOutputTableHandle,
            JdbcClient jdbcClient,
            ConnectorPageSinkId pageSinkId,
            RemoteQueryModifier remoteQueryModifier)
    {
        PhoenixOutputTableHandle outputTableHandle = (PhoenixOutputTableHandle) jdbcOutputTableHandle;
        ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypesBuilder = ImmutableList.builder();
        columnNamesBuilder.addAll(outputTableHandle.getColumnNames());
        columnTypesBuilder.addAll(outputTableHandle.getColumnTypes());
        if (outputTableHandle.rowkeyColumn().isPresent()) {
            columnNamesBuilder.add(ROWKEY);
            columnTypesBuilder.add(ROWKEY_COLUMN_HANDLE.getColumnType());
        }

        PhoenixOutputTableHandle updateOutputTableHandle = new PhoenixOutputTableHandle(
                outputTableHandle.getSchemaName(),
                outputTableHandle.getTableName(),
                columnNamesBuilder.build(),
                columnTypesBuilder.build(),
                Optional.empty(),
                Optional.empty());
        return new JdbcPageSink(session, updateOutputTableHandle, jdbcClient, pageSinkId, remoteQueryModifier);
    }

    @Override
    protected void appendUpdatePage(Page dataPage, List<Block> rowIdFields, int[] updatePositions, int updatePositionCount)
    {
        if (!hasRowKey) {
            // Upsert directly
            appendInsertPage(dataPage, updatePositions, updatePositionCount);
            return;
        }

        super.appendUpdatePage(dataPage, rowIdFields, updatePositions, updatePositionCount);
    }
}
