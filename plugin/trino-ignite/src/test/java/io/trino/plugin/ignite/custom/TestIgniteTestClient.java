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
package io.trino.plugin.ignite.custom;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.ignite.IgniteJdbcClient;
import io.trino.plugin.ignite.IgniteTableProperties;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.WriteFunction;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;

public class TestIgniteTestClient
        extends IgniteJdbcClient
{
    private static final String DUMMY_ID = "ignite_dummy_id";

    @Inject
    public TestIgniteTestClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, IdentifierMapping identifierMapping, RemoteQueryModifier queryModifier)
    {
        super(config, connectionFactory, queryBuilder, identifierMapping, queryModifier);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        try {
            return createTable(session, tableMetadata, tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected String createTableSql(RemoteTableName remoteTableName, List<String> columns, ConnectorTableMetadata tableMetadata)
    {
        // Append dummy id to the test table
        ImmutableMap.Builder<String, Object> newProperties = ImmutableMap.builder();
        for (var property : tableMetadata.getProperties().entrySet()) {
            if (property.getKey().equalsIgnoreCase(IgniteTableProperties.PRIMARY_KEY_PROPERTY)) {
                newProperties.put(property.getKey(), append(IgniteTableProperties.getPrimaryKey(tableMetadata.getProperties()), DUMMY_ID));
            }
            else {
                newProperties.put(property.getKey(), property.getValue());
            }
        }

        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(tableMetadata.getTable(), append(tableMetadata.getColumns(), new ColumnMetadata(DUMMY_ID, DummyIdType.DUMMY_ID)), newProperties.buildOrThrow(), tableMetadata.getComment());

        return super.createTableSql(remoteTableName, append(columns, quoted(DUMMY_ID) + " VARCHAR NOT NULL"), connectorTableMetadata);
    }

    @Override
    protected void copyTableSchema(ConnectorSession session, Connection connection, String newTableName, List<JdbcColumnHandle> columns, JdbcTableHandle tableHandle)
    {
        super.copyTableSchema(session, connection, newTableName, append(columns, dummyIdJdbcColumnHandle()), tableHandle);
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters)
    {
        return super.buildInsertSql(appendDummyIdInJdbcOutputTableHandle(handle), append(columnWriters, dummyIdWriteFunction()));
    }

    @Override
    public void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        super.finishInsertTable(session, appendDummyIdInJdbcOutputTableHandle(handle), pageSinkIds);
    }

    @Override
    public void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle, Set<Long> pageSinkIds)
    {
        // skip execution
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == DummyIdType.DUMMY_ID) {
            type = VarcharType.VARCHAR;
        }
        return super.toWriteMapping(session, type);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        JdbcTableHandle newTable = new JdbcTableHandle(table.getRelationHandle(),
                table.getConstraint(),
                table.getConstraintExpressions(),
                table.getSortOrder(),
                table.getLimit(),
                Optional.of(hideDummyId(columns)),
                table.getOtherReferencedTables(),
                table.getNextSyntheticColumnId(),
                table.getAuthorization());

        return super.buildSql(session, connection, split, newTable, columns);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle table)
    {
        // Make dummy id transparent to upstream column test
        return hideDummyId(super.getColumns(session, table));
    }

    private <T> List<T> append(List<T> originalData, T dummy)
    {
        ImmutableList.Builder<T> appended = ImmutableList.builder();
        if (!originalData.isEmpty()) {
            appended.addAll(originalData);
        }
        appended.add(dummy);
        return appended.build();
    }

    private JdbcColumnHandle dummyIdJdbcColumnHandle()
    {
        return JdbcColumnHandle.builder().setColumnName(DUMMY_ID).setColumnType(VarcharType.VARCHAR).setJdbcTypeHandle(JDBC_VARCHAR).setNullable(false).build();
    }

    private WriteFunction dummyIdWriteFunction()
    {
        return new WriteFunction()
        {
            @Override
            public Class<?> getJavaType()
            {
                return String.class;
            }

            @Override
            public String getBindExpression()
            {
                return "CAST(UUID() AS VARCHAR)";
            }
        };
    }

    private JdbcOutputTableHandle appendDummyIdInJdbcOutputTableHandle(JdbcOutputTableHandle handle)
    {
        return new JdbcOutputTableHandle(
                handle.getCatalogName(),
                handle.getSchemaName(),
                handle.getTableName(),
                append(handle.getColumnNames(), DUMMY_ID),
                append(handle.getColumnTypes(), VarcharType.VARCHAR),
                handle.getJdbcColumnTypes().isPresent() ? Optional.of(append(handle.getJdbcColumnTypes().get(), JDBC_VARCHAR)) : handle.getJdbcColumnTypes(),
                handle.getTemporaryTableName(),
                handle.getPageSinkIdColumnName());
    }

    private List<JdbcColumnHandle> hideDummyId(List<JdbcColumnHandle> columns)
    {
        return columns.stream().filter(column -> !column.getColumnName().equalsIgnoreCase(DUMMY_ID)).collect(toImmutableList());
    }

    public static class DummyIdType
            extends AbstractIntType
    {
        public static final DummyIdType DUMMY_ID = new DummyIdType();

        protected DummyIdType()
        {
            super(new TypeSignature(StandardTypes.VARCHAR));
        }

        @Override
        public TypeId getTypeId()
        {
            return super.getTypeId();
        }

        @Override
        public String getBaseName()
        {
            return super.getBaseName();
        }

        @Override
        public Object getObjectValue(ConnectorSession session, Block block, int position)
        {
            return null;
        }

        @Override
        public Optional<Range> getRange()
        {
            return super.getRange();
        }

        @Override
        public Optional<Object> getPreviousValue(Object value)
        {
            return super.getPreviousValue(value);
        }

        @Override
        public Optional<Object> getNextValue(Object value)
        {
            return super.getNextValue(value);
        }

        @Override
        public Optional<Stream<?>> getDiscreteValues(Range range)
        {
            return super.getDiscreteValues(range);
        }
    }
}
