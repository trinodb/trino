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
import com.google.inject.Inject;
import io.trino.plugin.jdbc.MergeJdbcPageSource.ColumnAdaptation;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.jdbc.DefaultJdbcMetadata.MERGE_ROW_ID;
import static io.trino.plugin.jdbc.MergeJdbcPageSource.MergedRowAdaptation;
import static io.trino.plugin.jdbc.MergeJdbcPageSource.SourceColumn;
import static java.util.Objects.requireNonNull;

public class JdbcPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final JdbcClient jdbcClient;
    private final ConnectorRecordSetProvider recordSetProvider;

    @Inject
    public JdbcPageSourceProvider(JdbcClient jdbcClient, ConnectorRecordSetProvider recordSetProvider)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        if (table instanceof JdbcProcedureHandle procedureHandle) {
            return new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, split, procedureHandle, columns));
        }

        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        Optional<JdbcColumnHandle> mergeRowId = columns.stream()
                .map(JdbcColumnHandle.class::cast)
                .filter(column -> column.getColumnName().equalsIgnoreCase(MERGE_ROW_ID))
                .collect(toOptional());
        if (mergeRowId.isEmpty()) {
            return new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, split, tableHandle, columns));
        }

        return createMergePageSource(transaction, session, split, columns, tableHandle, mergeRowId);
    }

    private MergeJdbcPageSource createMergePageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            List<ColumnHandle> columns,
            JdbcTableHandle tableHandle,
            Optional<JdbcColumnHandle> mergeRowId)
    {
        List<JdbcColumnHandle> primaryKeys = jdbcClient.getPrimaryKeys(session, tableHandle.getRequiredNamedRelation().getRemoteTableName());
        List<JdbcColumnHandle> scanColumns = getScanColumns(session, jdbcClient, tableHandle, primaryKeys);

        ImmutableList.Builder<ColumnAdaptation> columnAdaptationsBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columns) {
            if (columnHandle.equals(mergeRowId.get())) {
                columnAdaptationsBuilder.add(buildMergeIdColumnAdaptation(scanColumns, primaryKeys));
            }
            else {
                columnAdaptationsBuilder.add(new SourceColumn(scanColumns.indexOf(columnHandle)));
            }
        }

        JdbcTableHandle newTableHandle = new JdbcTableHandle(
                tableHandle.getRelationHandle(),
                tableHandle.getConstraint(),
                tableHandle.getConstraintExpressions(),
                tableHandle.getSortOrder(),
                tableHandle.getLimit(),
                Optional.of(scanColumns),
                tableHandle.getOtherReferencedTables(),
                tableHandle.getNextSyntheticColumnId(),
                tableHandle.getAuthorization(),
                tableHandle.getUpdateAssignments());
        return new MergeJdbcPageSource(
                new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, split, newTableHandle, scanColumns)),
                columnAdaptationsBuilder.build());
    }

    private static List<JdbcColumnHandle> getScanColumns(
            ConnectorSession session,
            JdbcClient jdbcClient,
            JdbcTableHandle tableHandle,
            List<JdbcColumnHandle> primaryKeys)
    {
        List<JdbcColumnHandle> allTableColumns = jdbcClient.getColumns(session, tableHandle.getRequiredNamedRelation().getSchemaTableName(), tableHandle.getRequiredNamedRelation().getRemoteTableName());

        ImmutableList.Builder<JdbcColumnHandle> scanColumnsBuilder = ImmutableList.builder();
        scanColumnsBuilder.addAll(allTableColumns);
        // Add merge row id fields
        for (JdbcColumnHandle primaryKey : primaryKeys) {
            if (!allTableColumns.contains(primaryKey)) {
                scanColumnsBuilder.add(primaryKey);
            }
        }
        return scanColumnsBuilder.build();
    }

    private static ColumnAdaptation buildMergeIdColumnAdaptation(List<JdbcColumnHandle> scanColumns, List<JdbcColumnHandle> primaryKeys)
    {
        List<Integer> mergeRowIdSourceChannels = primaryKeys.stream()
                .map(scanColumns::indexOf)
                .peek(channel -> checkArgument(channel >= 0, "There are primary keys not exist in scan columns"))
                .collect(toImmutableList());
        return new MergedRowAdaptation(mergeRowIdSourceChannels);
    }
}
