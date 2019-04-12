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
package io.prestosql.plugin.sqlserver;

import com.google.common.base.Joiner;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;

public class SqlServerClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");

    // SqlServer supports 2100 parameters in prepared statement, let's create a space for about 4 big IN predicates
    private static final int SQL_SERVER_MAX_LIST_EXPRESSIONS = 500;

    // TODO improve this by calling Domain#simplify
    private static final UnaryOperator<Domain> DISABLE_UNSUPPORTED_PUSHDOWN = domain -> {
        if (domain.getValues().getRanges().getRangeCount() <= SQL_SERVER_MAX_LIST_EXPRESSIONS) {
            return domain;
        }
        return Domain.all(domain.getType());
    };

    @Inject
    public SqlServerClient(BaseJdbcConfig config)
    {
        super(config, "\"", new DriverConnectionFactory(new SQLServerDriver(), config));
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        String sql = format(
                "sp_rename %s, %s",
                singleQuote(catalogName, schemaName, tableName),
                singleQuote(newTable.getTableName()));
        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format(
                    "sp_rename %s, %s, 'COLUMN'",
                    singleQuote(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(), jdbcColumn.getColumnName()),
                    singleQuote(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle type)
    {
        // TODO implement proper type mapping
        return super.toPrestoType(session, type)
                .map(columnMapping -> new ColumnMapping(
                        columnMapping.getType(),
                        columnMapping.getReadFunction(),
                        columnMapping.getWriteFunction(),
                        DISABLE_UNSUPPORTED_PUSHDOWN));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("bit", booleanWriteFunction());
        }

        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            String dataType;
            if (varcharType.isUnbounded() || varcharType.getBoundedLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nvarchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            String dataType;
            if (charType.getLength() > 4000) {
                dataType = "nvarchar(max)";
            }
            else {
                dataType = "nchar(" + charType.getLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, charWriteFunction());
        }

        // TODO implement proper type mapping
        return super.toWriteMapping(session, type);
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> {
            String start = "SELECT ";
            checkArgument(sql.startsWith(start));
            return "SELECT TOP " + limit + " " + sql.substring(start.length());
        });
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    private static String singleQuote(String... objects)
    {
        return singleQuote(DOT_JOINER.join(objects));
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
    }
}
