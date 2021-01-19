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
package io.prestosql.plugin.genericjdbc;

import com.google.common.base.Joiner;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.function.UnaryOperator;

import static io.prestosql.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

public class GenericJdbcClient
        extends BaseJdbcClient
{
    private static final Joiner DOT_JOINER = Joiner.on(".");

    // Sybase supports 2100 parameters in prepared statement, let's create a space for about 4 big IN predicates
    private static final int MAX_LIST_EXPRESSIONS = 500;

    // TODO improve this by calling Domain#simplify
    private static final UnaryOperator<Domain> DISABLE_UNSUPPORTED_PUSHDOWN = domain -> {
        if (domain.getValues().getRanges().getRangeCount() <= MAX_LIST_EXPRESSIONS) {
            return domain;
        }
        return Domain.all(domain.getType());
    };

    //not sure if getIdentifierQuoteString from jdbc driver is accurate instead...
    @Inject
    public GenericJdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(config, (config.getConnectionUrl().startsWith("jdbc:impala") || config.getConnectionUrl().startsWith("jdbc:hive2") || config.getConnectionUrl().startsWith("jdbc:mysql")) ? "`" : "\"", connectionFactory);
    }

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
        try {
            if (connection.getMetaData().getURL() != null && connection.getMetaData().getURL().startsWith("jdbc:impala") && (typeHandle.toString().contains("jdbcTypeName=ARRAY") || typeHandle.toString().contains("jdbcTypeName=MAP") || typeHandle.toString().contains("jdbcTypeName=STRUCT"))) {
                return Optional.empty();
            }
        }
        catch (Exception e) {
            //not all drivers have getURL...like SparkThrift
        }

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        // TODO implement proper type mapping
        return super.toPrestoType(session, connection, typeHandle)
                .map(columnMapping -> new ColumnMapping(
                        columnMapping.getType(),
                        columnMapping.getReadFunction(),
                        columnMapping.getWriteFunction(),
                        DISABLE_PUSHDOWN));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("bit", booleanWriteFunction());
        }

        if (type instanceof VarcharType) {
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

    private static String singleQuote(String... objects)
    {
        return singleQuote(DOT_JOINER.join(objects));
    }

    private static String singleQuote(String literal)
    {
        return "\'" + literal + "\'";
    }
}
