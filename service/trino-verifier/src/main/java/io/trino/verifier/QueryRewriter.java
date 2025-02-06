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
package io.trino.verifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.tree.LikeClause.PropertiesOption.INCLUDING;
import static io.trino.sql.tree.SaveMode.IGNORE;
import static io.trino.verifier.QueryType.READ;
import static io.trino.verifier.VerifyCommand.statementToQueryType;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class QueryRewriter
{
    private static final Set<Integer> APPROXIMATE_TYPES = ImmutableSet.of(Types.REAL, Types.FLOAT, Types.DOUBLE);

    private final SqlParser parser;
    private final String gatewayUrl;
    private final QualifiedName rewritePrefix;
    private final Optional<String> catalogOverride;
    private final Optional<String> schemaOverride;
    private final Optional<String> usernameOverride;
    private final Optional<String> passwordOverride;
    private final int doublePrecision;
    private final Duration timeout;

    public QueryRewriter(SqlParser parser, String gatewayUrl, QualifiedName rewritePrefix, Optional<String> catalogOverride, Optional<String> schemaOverride, Optional<String> usernameOverride, Optional<String> passwordOverride, int doublePrecision, Duration timeout)
    {
        this.parser = requireNonNull(parser, "parser is null");
        this.gatewayUrl = requireNonNull(gatewayUrl, "gatewayUrl is null");
        this.rewritePrefix = requireNonNull(rewritePrefix, "rewritePrefix is null");
        this.catalogOverride = requireNonNull(catalogOverride, "catalogOverride is null");
        this.schemaOverride = requireNonNull(schemaOverride, "schemaOverride is null");
        this.usernameOverride = requireNonNull(usernameOverride, "usernameOverride is null");
        this.passwordOverride = requireNonNull(passwordOverride, "passwordOverride is null");
        this.doublePrecision = doublePrecision;
        this.timeout = requireNonNull(timeout, "timeout is null");
    }

    public Query shadowQuery(Query query)
            throws QueryRewriteException, SQLException
    {
        if (statementToQueryType(parser, query.getQuery()) == READ) {
            return query;
        }
        if (!query.getPreQueries().isEmpty()) {
            throw new QueryRewriteException("Cannot rewrite queries that use pre-queries");
        }
        if (!query.getPostQueries().isEmpty()) {
            throw new QueryRewriteException("Cannot rewrite queries that use post-queries");
        }

        Statement statement = parser.createStatement(query.getQuery());
        try (Connection connection = DriverManager.getConnection(gatewayUrl, usernameOverride.orElse(query.getUsername()), passwordOverride.orElse(query.getPassword()))) {
            trySetConnectionProperties(query, connection);
            if (statement instanceof CreateTableAsSelect) {
                return rewriteCreateTableAsSelect(connection, query, (CreateTableAsSelect) statement);
            }
            if (statement instanceof Insert) {
                return rewriteInsertQuery(connection, query, (Insert) statement);
            }
        }

        throw new QueryRewriteException("Unsupported query type: " + statement.getClass());
    }

    private Query rewriteCreateTableAsSelect(Connection connection, Query query, CreateTableAsSelect statement)
            throws SQLException, QueryRewriteException
    {
        QualifiedName temporaryTableName = generateTemporaryTableName(statement.getName());
        Statement rewritten = new CreateTableAsSelect(temporaryTableName, statement.getQuery(), statement.getSaveMode(), statement.getProperties(), statement.isWithData(), statement.getColumnAliases(), Optional.empty());
        String createTableAsSql = formatSql(rewritten);
        String checksumSql = checksumSql(getColumns(connection, statement), temporaryTableName);
        String dropTableSql = dropTableSql(temporaryTableName);
        return new Query(query.getCatalog(), query.getSchema(), ImmutableList.of(createTableAsSql), checksumSql, ImmutableList.of(dropTableSql), query.getUsername(), query.getPassword(), query.getSessionProperties());
    }

    private Query rewriteInsertQuery(Connection connection, Query query, Insert statement)
            throws SQLException, QueryRewriteException
    {
        QualifiedName temporaryTableName = generateTemporaryTableName(statement.getTarget());
        Statement createTemporaryTable = new CreateTable(temporaryTableName, ImmutableList.of(new LikeClause(statement.getTarget(), Optional.of(INCLUDING))), IGNORE, ImmutableList.of(), Optional.empty());
        String createTemporaryTableSql = formatSql(createTemporaryTable);
        String insertSql = formatSql(new Insert(new Table(temporaryTableName), statement.getColumns(), statement.getQuery()));
        String checksumSql = checksumSql(getColumnsForTable(connection, query.getCatalog(), query.getSchema(), statement.getTarget().toString()), temporaryTableName);
        String dropTableSql = dropTableSql(temporaryTableName);
        return new Query(query.getCatalog(), query.getSchema(), ImmutableList.of(createTemporaryTableSql, insertSql), checksumSql, ImmutableList.of(dropTableSql), query.getUsername(), query.getPassword(), query.getSessionProperties());
    }

    private QualifiedName generateTemporaryTableName(QualifiedName originalName)
    {
        List<Identifier> parts = new ArrayList<>();
        int originalSize = originalName.getOriginalParts().size();
        int prefixSize = rewritePrefix.getOriginalParts().size();
        if (originalSize > prefixSize) {
            parts.addAll(originalName.getOriginalParts().subList(0, originalSize - prefixSize));
        }
        parts.addAll(rewritePrefix.getOriginalParts());
        parts.set(parts.size() - 1, new Identifier(createTemporaryTableName()));
        return QualifiedName.of(parts);
    }

    private void trySetConnectionProperties(Query query, Connection connection)
            throws SQLException
    {
        // Required for jdbc drivers that do not implement all/some of these functions (eg. impala jdbc driver)
        // For these drivers, set the database default values in the query database
        try {
            connection.setClientInfo("ApplicationName", "verifier-rewrite");
            connection.setCatalog(catalogOverride.orElse(query.getCatalog()));
            connection.setSchema(schemaOverride.orElse(query.getSchema()));
        }
        catch (SQLClientInfoException _) {
            // Do nothing
        }
    }

    private String createTemporaryTableName()
    {
        return rewritePrefix.getSuffix() + UUID.randomUUID().toString().replace("-", "");
    }

    private List<Column> getColumnsForTable(Connection connection, String catalog, String schema, String table)
            throws SQLException
    {
        ResultSet columns = connection.getMetaData().getColumns(catalog, escapeLikeExpression(connection, schema), escapeLikeExpression(connection, table), null);
        ImmutableList.Builder<Column> columnBuilder = ImmutableList.builder();
        while (columns.next()) {
            String name = columns.getString("COLUMN_NAME");
            int type = columns.getInt("DATA_TYPE");
            columnBuilder.add(new Column(name, APPROXIMATE_TYPES.contains(type)));
        }
        return columnBuilder.build();
    }

    private List<Column> getColumns(Connection connection, CreateTableAsSelect createTableAsSelect)
            throws SQLException
    {
        io.trino.sql.tree.Query createSelectClause = createTableAsSelect.getQuery();

        // Rewrite the query to select zero rows, so that we can get the column names and types
        QueryBody innerQuery = createSelectClause.getQueryBody();
        io.trino.sql.tree.Query zeroRowsQuery;
        if (innerQuery instanceof QuerySpecification querySpecification) {
            innerQuery = new QuerySpecification(
                    querySpecification.getSelect(),
                    querySpecification.getFrom(),
                    querySpecification.getWhere(),
                    querySpecification.getGroupBy(),
                    querySpecification.getHaving(),
                    querySpecification.getWindows(),
                    querySpecification.getOrderBy(),
                    querySpecification.getOffset(),
                    Optional.of(new Limit(new LongLiteral("0"))));

            zeroRowsQuery = new io.trino.sql.tree.Query(ImmutableList.of(), ImmutableList.of(), createSelectClause.getWith(), innerQuery, Optional.empty(), Optional.empty(), Optional.empty());
        }
        else {
            zeroRowsQuery = new io.trino.sql.tree.Query(ImmutableList.of(), ImmutableList.of(), createSelectClause.getWith(), innerQuery, Optional.empty(), Optional.empty(), Optional.of(new Limit(new LongLiteral("0"))));
        }

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        try (java.sql.Statement jdbcStatement = connection.createStatement()) {
            ExecutorService executor = newSingleThreadExecutor();
            TimeLimiter limiter = SimpleTimeLimiter.create(executor);
            java.sql.Statement limitedStatement = limiter.newProxy(jdbcStatement, java.sql.Statement.class, timeout.toMillis(), TimeUnit.MILLISECONDS);
            try (ResultSet resultSet = limitedStatement.executeQuery(formatSql(zeroRowsQuery))) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String name = metaData.getColumnName(i);
                    int type = metaData.getColumnType(i);
                    columns.add(new Column(name, APPROXIMATE_TYPES.contains(type)));
                }
            }
            catch (UncheckedTimeoutException e) {
                throw new SQLException("SQL statement execution timed out", e);
            }
            finally {
                executor.shutdownNow();
            }
        }

        return columns.build();
    }

    private String checksumSql(List<Column> columns, QualifiedName table)
            throws QueryRewriteException
    {
        if (columns.isEmpty()) {
            throw new QueryRewriteException("Table " + table + " has no columns");
        }
        ImmutableList.Builder<SelectItem> selectItems = ImmutableList.builder();
        for (Column column : columns) {
            Expression expression = new Identifier(column.getName());
            if (column.isApproximateType()) {
                expression = new FunctionCall(QualifiedName.of("round"), ImmutableList.of(expression, new LongLiteral(Integer.toString(doublePrecision))));
            }
            selectItems.add(new SingleColumn(new FunctionCall(QualifiedName.of("checksum"), ImmutableList.of(expression))));
        }

        Select select = new Select(false, selectItems.build());
        return formatSql(new QuerySpecification(select, Optional.of(new Table(table)), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty()));
    }

    private static String dropTableSql(QualifiedName table)
    {
        return formatSql(new DropTable(new NodeLocation(1, 1), table, true));
    }

    private static String escapeLikeExpression(Connection connection, String value)
            throws SQLException
    {
        String escapeString = connection.getMetaData().getSearchStringEscape();
        return value.replace(escapeString, escapeString + escapeString).replace("_", escapeString + "_").replace("%", escapeString + "%");
    }

    public static class QueryRewriteException
            extends Exception
    {
        public QueryRewriteException(String message)
        {
            super(message);
        }
    }

    private static class Column
    {
        private final String name;
        private final boolean approximateType;

        private Column(String name, boolean approximateType)
        {
            this.name = name;
            this.approximateType = approximateType;
        }

        public String getName()
        {
            return name;
        }

        public boolean isApproximateType()
        {
            return approximateType;
        }
    }
}
