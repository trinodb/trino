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
package io.prestosql.plugin.salesforce;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.BlockReadFunction;
import io.prestosql.plugin.jdbc.BlockWriteFunction;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.SliceReadFunction;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.plugin.jdbc.ColumnMapping.blockMapping;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;

public class SalesforceClient
        extends BaseJdbcClient
{
    private final SalesforceConfig salesforceConfig;

    @Inject
    public SalesforceClient(BaseJdbcConfig baseConfig, SalesforceConfig salesforceConfig, ConnectionFactory connectionFactory)
    {
        super(baseConfig, "", connectionFactory);

        this.salesforceConfig = salesforceConfig;
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql);

        statement.setFetchSize(
                salesforceConfig.getFetchSize().orElse(2000)
        );

        return statement;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    @Override
    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return "salesforce";
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        int columnSize = typeHandle.getColumnSize();

        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        if (jdbcTypeName.equals("multipicklist")) {
            VarcharType type = createVarcharType(typeHandle.getColumnSize());

            if (typeHandle.getColumnSize() == 0 || typeHandle.getColumnSize() > VarcharType.MAX_LENGTH) {
                type = createUnboundedVarcharType();
            }

            return Optional.of(blockMapping(new ArrayType(type), multiPicklistReadFunction(type), multiPicklistWriteFunction(type)));
        }

        switch (typeHandle.getJdbcType()) {
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (columnSize == 0 || typeHandle.getColumnSize() > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharColumnMapping(createVarcharType(columnSize)));
            case Types.NUMERIC:
            case Types.DECIMAL:
                return Optional.of(doubleColumnMapping());
            case Types.OTHER:
                return Optional.of(ColumnMapping.sliceMapping(VarcharType.VARCHAR, otherReadFunction(), varcharWriteFunction()));
            default:
                return super.toPrestoType(session, connection, typeHandle);
        }
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        return new SoqlQueryBuilder(identifierQuote).buildSql(this, session, connection, table.getCatalogName(), table.getSchemaName(), table.getTableName(), columns, table.getConstraint(), split.getAdditionalPredicate(), tryApplyLimit(table.getLimit()));
    }

    private static BlockReadFunction multiPicklistReadFunction(Type type)
    {
        return (resultSet, columnIndex) -> {
            BlockBuilder builder = createUnboundedVarcharType().createBlockBuilder(null, 1);

            for (String value : resultSet.getString(columnIndex).split(";")) {
                type.writeSlice(builder, utf8Slice(value));
            }

            return builder.build();
        };
    }

    private static BlockWriteFunction multiPicklistWriteFunction(Type type)
    {
        return (statement, index, value) -> {
            // Not implemented
        };
    }

    public static SliceReadFunction otherReadFunction()
    {
        return (resultSet, columnIndex) -> utf8Slice(resultSet.getObject(columnIndex).toString());
    }
}
