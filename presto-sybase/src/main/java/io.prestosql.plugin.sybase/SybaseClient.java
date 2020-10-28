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
package io.prestosql.plugin.sybase;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcExpression;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.PredicatePushdownController;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.plugin.jdbc.expression.ImplementAvgDecimal;
import io.prestosql.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.prestosql.plugin.jdbc.expression.ImplementCount;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.plugin.jdbc.expression.ImplementMinMax;
import io.prestosql.plugin.jdbc.expression.ImplementSum;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class SybaseClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");

    // Sybase supports 2100 parameters in prepared statement, let's create a space for about 4 big IN predicates
    private static final int SYBASE_MAX_LIST_EXPRESSIONS = 500;

    public static final PredicatePushdownController DISABLE_UNSUPPORTED_PUSHDOWN = domain -> {
        if (domain.getValues().getRanges().getRangeCount() <= SYBASE_MAX_LIST_EXPRESSIONS) {
            return new PredicatePushdownController.DomainPushdownResult(domain, domain);
        }

        return new PredicatePushdownController.DomainPushdownResult(Domain.all(domain.getType()), domain);
    };

    private final AggregateFunctionRewriter aggregateFunctionRewriter;

    @Inject
    public SybaseClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), 0, 0, Optional.empty(), Optional.empty());
        JdbcTypeHandle intTypeHandle = new JdbcTypeHandle(Types.INTEGER, Optional.of("int"), 10, 0, Optional.empty(), Optional.empty());

        this.aggregateFunctionRewriter = new AggregateFunctionRewriter(
                this::quoted,
                ImmutableSet.<AggregateFunctionRule>builder()
                        .add(new ImplementCountAll(bigintTypeHandle))
                        .add(new ImplementCount(bigintTypeHandle))
                        .add(new ImplementMinMax())
                        .add(new ImplementSum(SybaseClient::toTypeHandle))
                        .add(new ImplementSum(SybaseClient::toIntTypeHandle))
                        .add(new ImplementAvgFloatingPoint())
                        .add(new ImplementAvgDecimal())
                        .add(new ImplementAvgInt())
                        .add(new ImplementAvgBigint())
                        .add(new ImplementSybaseDBStdev())
                        .add(new ImplementSybaseDBStddevPop())
                        .add(new ImplementSybaseDBVariance())
                        .add(new ImplementSybaseDBVariancePop())
                        .build());
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), decimalType.getPrecision(), decimalType.getScale(), Optional.empty(), Optional.empty()));
    }

    private static Optional<JdbcTypeHandle> toIntTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.INTEGER, Optional.of("int"), 10, 0, Optional.empty(), Optional.empty()));
    }

    private static String singleQuote(String... objects)
    {
        return singleQuote(DOT_JOINER.join(objects));
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
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
    protected void copyTableSchema(Connection connection, String catalogName, String schemaName, String tableName, String newTableName, List<String> columnNames)
    {
        String sql = format(
                "SELECT %s INTO %s FROM %s WHERE 0 = 1",
                columnNames.stream()
                        .map(this::quoted)
                        .collect(joining(", ")),
                quoted(catalogName, schemaName, newTableName),
                quoted(catalogName, schemaName, tableName));
        execute(connection, sql);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        // TODO implement proper type mapping
        return super.toPrestoType(session, connection, typeHandle)
                .map(columnMapping -> {
                    if (columnMapping.getType() instanceof IntegerType) {
                        return new ColumnMapping(
                                BigintType.BIGINT,
                                columnMapping.getReadFunction(),
                                columnMapping.getWriteFunction(),
                                DISABLE_UNSUPPORTED_PUSHDOWN);
                    }
                    else {
                        return new ColumnMapping(
                                columnMapping.getType(),
                                columnMapping.getReadFunction(),
                                columnMapping.getWriteFunction(),
                                DISABLE_UNSUPPORTED_PUSHDOWN);
                    }
                });
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        connection.close();
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
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }
}
