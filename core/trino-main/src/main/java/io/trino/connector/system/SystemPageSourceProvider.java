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
package io.trino.connector.system;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.FullConnectorSession;
import io.trino.plugin.base.MappedPageSource;
import io.trino.plugin.base.MappedRecordSet;
import io.trino.security.AccessControl;
import io.trino.security.InjectedConnectorAccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.connector.SystemTable.Distribution.ALL_NODES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SystemPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final SystemTablesProvider tables;
    private final AccessControl accessControl;
    private final String catalogName;

    public SystemPageSourceProvider(SystemTablesProvider tables, AccessControl accessControl, String catalogName)
    {
        this.tables = requireNonNull(tables, "tables is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
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
        requireNonNull(columns, "columns is null");
        SystemTransactionHandle systemTransaction = (SystemTransactionHandle) transaction;
        SystemSplit systemSplit = (SystemSplit) split;
        SchemaTableName tableName = ((SystemTableHandle) table).schemaTableName();
        SystemTable systemTable = tables.getSystemTable(session, tableName)
                // table might disappear in the meantime
                .orElseThrow(() -> new TrinoException(NOT_FOUND, format("Table '%s' not found", tableName)));

        List<ColumnMetadata> tableColumns = systemTable.getTableMetadata().getColumns();

        Map<String, Integer> columnsByName = new HashMap<>();
        for (int i = 0; i < tableColumns.size(); i++) {
            ColumnMetadata column = tableColumns.get(i);
            if (columnsByName.put(column.getName(), i) != null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Duplicate column name: " + column.getName());
            }
        }

        ImmutableList.Builder<Integer> userToSystemFieldIndex = ImmutableList.builder();
        ImmutableSet.Builder<Integer> requiredColumns = ImmutableSet.builder();
        for (ColumnHandle column : columns) {
            String columnName = ((SystemColumnHandle) column).columnName();

            Integer index = columnsByName.get(columnName);
            if (index == null) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Column does not exist: %s.%s", tableName, columnName));
            }

            userToSystemFieldIndex.add(index);
            requiredColumns.add(index);
        }

        TupleDomain<ColumnHandle> constraint = systemSplit.getConstraint();
        if (constraint.isNone()) {
            return new EmptyPageSource();
        }
        TupleDomain<Integer> newConstraint = systemSplit.getConstraint().transformKeys(columnHandle ->
                columnsByName.get(((SystemColumnHandle) columnHandle).columnName()));

        ConnectorAccessControl accessControl1 = new InjectedConnectorAccessControl(
                accessControl,
                new SecurityContext(
                        systemTransaction.getTransactionId(),
                        ((FullConnectorSession) session).getSession().getIdentity(),
                        QueryId.valueOf(session.getQueryId()),
                        session.getStart()),
                catalogName);
        try {
            // Do not pass access control for tables that execute on workers
            if (systemTable.getDistribution().equals(ALL_NODES)) {
                return new MappedPageSource(
                        systemTable.pageSource(
                                systemTransaction.getConnectorTransactionHandle(),
                                session,
                                newConstraint),
                        userToSystemFieldIndex.build());
            }
            return new MappedPageSource(
                    systemTable.pageSource(
                            systemTransaction.getConnectorTransactionHandle(),
                            session,
                            newConstraint,
                            accessControl1),
                    userToSystemFieldIndex.build());
        }
        catch (UnsupportedOperationException e) {
            return new RecordPageSource(new MappedRecordSet(
                    toRecordSet(
                            systemTransaction.getConnectorTransactionHandle(),
                            systemTable,
                            session,
                            newConstraint,
                            requiredColumns.build(),
                            systemSplit,
                            accessControl1),
                    userToSystemFieldIndex.build()));
        }
    }

    private static RecordSet toRecordSet(
            ConnectorTransactionHandle sourceTransaction,
            SystemTable table,
            ConnectorSession session,
            TupleDomain<Integer> constraint,
            Set<Integer> requiredColumns,
            ConnectorSplit split,
            ConnectorAccessControl accessControl1)
    {
        return new RecordSet()
        {
            private final List<Type> types = table.getTableMetadata().getColumns().stream()
                    .map(ColumnMetadata::getType)
                    .collect(toImmutableList());

            @Override
            public List<Type> getColumnTypes()
            {
                return types;
            }

            @Override
            public RecordCursor cursor()
            {
                // Do not pass access control for tables that execute on workers
                if (table.getDistribution().equals(ALL_NODES)) {
                    return table.cursor(sourceTransaction, session, constraint, requiredColumns, split);
                }
                return table.cursor(sourceTransaction, session, constraint, requiredColumns, split, accessControl1);
            }
        };
    }
}
