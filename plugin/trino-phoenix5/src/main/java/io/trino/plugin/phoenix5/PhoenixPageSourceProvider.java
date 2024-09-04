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
import com.google.inject.Inject;
import io.trino.plugin.jdbc.ForRecordCursor;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.type.RowType;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.indexOf;
import static com.google.common.collect.MoreCollectors.toOptional;
import static io.trino.plugin.phoenix5.PhoenixClient.MERGE_ROW_ID_COLUMN_NAME;
import static io.trino.plugin.phoenix5.PhoenixPageSource.ColumnAdaptation;

public class PhoenixPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final JdbcRecordSetProvider recordSetProvider;
    private final PhoenixClient phoenixClient;

    @Inject
    public PhoenixPageSourceProvider(PhoenixClient phoenixClient, @ForRecordCursor ExecutorService executor)
    {
        this.recordSetProvider = new JdbcRecordSetProvider(phoenixClient, executor);
        this.phoenixClient = phoenixClient;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns, DynamicFilter dynamicFilter)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        Optional<JdbcColumnHandle> mergeRowId = columns.stream()
                .map(JdbcColumnHandle.class::cast)
                .filter(column -> column.getColumnName().equalsIgnoreCase(MERGE_ROW_ID_COLUMN_NAME))
                .collect(toOptional());
        if (mergeRowId.isEmpty()) {
            return new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, split, tableHandle, columns));
        }

        List<JdbcColumnHandle> dataColumns = phoenixClient.getColumns(
                session,
                new JdbcTableHandle(tableHandle.getRequiredNamedRelation().getSchemaTableName(), tableHandle.getRequiredNamedRelation().getRemoteTableName(), Optional.empty()));

        ImmutableList.Builder<ColumnAdaptation> columnAdaptationBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : columns) {
            JdbcColumnHandle column = (JdbcColumnHandle) columnHandle;
            if (column.getColumnName().equalsIgnoreCase(MERGE_ROW_ID_COLUMN_NAME)) {
                columnAdaptationBuilder.add(buildMergeIdColumnAdaptation(dataColumns, mergeRowId.get()));
            }
            else {
                columnAdaptationBuilder.add(ColumnAdaptation.sourceColumn(dataColumns.indexOf(column)));
            }
        }

        tableHandle = new JdbcTableHandle(
                tableHandle.getRelationHandle(),
                tableHandle.getConstraint(),
                tableHandle.getConstraintExpressions(),
                tableHandle.getSortOrder(),
                tableHandle.getLimit(),
                Optional.of(dataColumns),
                tableHandle.getOtherReferencedTables(),
                tableHandle.getNextSyntheticColumnId(),
                tableHandle.getAuthorization(),
                tableHandle.getUpdateAssignments());

        return new PhoenixPageSource(
                new RecordPageSource(recordSetProvider.getRecordSet(transaction, session, split, tableHandle, ImmutableList.copyOf(dataColumns))),
                columnAdaptationBuilder.build());
    }

    private ColumnAdaptation buildMergeIdColumnAdaptation(List<JdbcColumnHandle> scanColumns, JdbcColumnHandle mergeRowIdColumn)
    {
        RowType columnType = (RowType) mergeRowIdColumn.getColumnType();
        List<Integer> mergeRowIdSourceChannels = columnType.getFields().stream()
                .map(RowType.Field::getName)
                .map(Optional::get)
                .map(fieldName -> indexOf(scanColumns.iterator(), handle -> handle.getColumnName().equals(fieldName)))
                .peek(fieldIndex -> checkArgument(fieldIndex != -1, "Merge row id field must exist in scanned columns"))
                .collect(toImmutableList());
        return ColumnAdaptation.mergedRowColumns(mergeRowIdSourceChannels);
    }
}
