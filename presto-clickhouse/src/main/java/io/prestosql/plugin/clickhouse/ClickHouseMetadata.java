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
package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcMetadataConfig;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

public class ClickHouseMetadata
        extends JdbcMetadata
{
    public static final String DEFAULT_SCHEMA = "default";
    private final ClickHouseClient clickhouseClient;
    private static final Logger log = Logger.get(ClickHouseClient.class);

    @Inject
    public ClickHouseMetadata(ClickHouseClient clickhouseclient, JdbcMetadataConfig metadataConfig)
    {
        super(clickhouseclient, metadataConfig.isAllowDropTable());
        this.clickhouseClient = requireNonNull(clickhouseclient, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(clickhouseClient.getSchemaNames(JdbcIdentity.from(session)));
    }

    @Override
    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return clickhouseClient.getTableHandle(JdbcIdentity.from(session), schemaTableName)
                .map(tableHandle -> new JdbcTableHandle(
                        schemaTableName,
                        tableHandle.getCatalogName(),
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName()))
                .orElse(null);
    }

    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        JdbcTableHandle handle = (JdbcTableHandle) table;
        List<ColumnMetadata> columnMetadata = clickhouseClient.getColumns(session, handle).stream()
                .map(JdbcColumnHandle::getColumnMetadata)
                .collect(toImmutableList());
        return new ConnectorTableMetadata(handle.getSchemaTableName(), columnMetadata);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        return createTable(session, tableMetadata);
    }

    private JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        Map<String, Object> tableProperties = tableMetadata.getProperties();
        Optional<String> schema = Optional.of(schemaTableName.getSchemaName());
        String table = schemaTableName.getTableName();

//        if (!ClickHouseClient.getScheåmaNames(JdbcIdentity.from(session)).contains(schema.orElse(null))) {
//            throw new SchemaNotFoundException(schema.orElse(null));
//        }

        LinkedList<ColumnMetadata> tableColumns = new LinkedList<>(tableMetadata.getColumns());

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<String> columnList = ImmutableList.builder();

        for (ColumnMetadata column : tableColumns) {
            String columnName = column.getName();
            columnNames.add(columnName);
            columnTypes.add(column.getType());
            String typeStatement = clickhouseClient.toWriteMapping(session, column.getType()).getDataType();

            columnList.add(format("%s %s", columnName, typeStatement));
        }

        ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
        ClickHouseTableProperties.getEngine(tableProperties).ifPresent(value -> tableOptions.add(ClickHouseTableProperties.ENGINE + "=" + value));

        String sql = format(
                "CREATE TABLE %s (%s) %s",
                schema + "." + table,
                join(", ", columnList.build()),
                join(", ", tableOptions.build()));

        log.info("Fun[createTable] sql: {}", sql);
        clickhouseClient.execute(session, sql);
        return new JdbcOutputTableHandle(
                "clickhouse",
                schema.orElse("default"),
                table,
                columnList.build(),
                columnTypes.build(),
                Optional.empty(),
                "");
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session, ConnectorInsertTableHandle insertHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return Optional.empty();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        return clickhouseClient.getTableNames(JdbcIdentity.from(session), schemaName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JdbcTableHandle jdbcTableHandle = (JdbcTableHandle) tableHandle;

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (JdbcColumnHandle column : clickhouseClient.getColumns(session, jdbcTableHandle)) {
            columnHandles.put(column.getColumnMetadata().getName(), column);
        }
        return columnHandles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((JdbcColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        List<SchemaTableName> tables = prefix.toOptionalSchemaTableName()
                .<List<SchemaTableName>>map(ImmutableList::of)
                .orElseGet(() -> listTables(session, prefix.getSchema()));
        for (SchemaTableName tableName : tables) {
            try {
                clickhouseClient.getTableHandle(JdbcIdentity.from(session), tableName)
                        .ifPresent(tableHandle -> columns.put(tableName, getTableMetadata(session, tableHandle).getColumns()));
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }
}
