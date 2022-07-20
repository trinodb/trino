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
package io.trino.plugin.mysql;

import io.trino.plugin.jdbc.ForRecordCursor;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcRecordSetProvider;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MySqlRecordSetProvider
        extends JdbcRecordSetProvider
{
    private final VersioningService versioningService;

    @Inject
    public MySqlRecordSetProvider(MySqlClient mySqlClient, @ForRecordCursor ExecutorService executor, VersioningService versioningService)
    {
        super(mySqlClient, executor);
        this.versioningService = requireNonNull(versioningService, "versioningService is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        if (handle.isStrictVersioning()) {
            List<String> columnNames = columns.stream()
                    .map(JdbcColumnHandle.class::cast)
                    .map(JdbcColumnHandle::getColumnName)
                    .collect(toImmutableList());
            List<Type> types = columns.stream()
                    .map(JdbcColumnHandle.class::cast)
                    .map(JdbcColumnHandle::getColumnType)
                    .collect(toImmutableList());
            return new InMemoryRecordSet(types, versioningService.getRecords(handle, types, columnNames));
        }

        return super.getRecordSet(transaction, session, split, table, columns);
    }
}
