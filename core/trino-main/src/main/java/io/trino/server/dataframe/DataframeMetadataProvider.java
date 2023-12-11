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
package io.trino.server.dataframe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.dataframe.DataframeException;
import com.starburstdata.dataframe.analyzer.TrinoMetadata;
import com.starburstdata.dataframe.expression.Attribute;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.sql.SqlFormatter;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.Field;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Statement;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.dataframe.DataframeException.ErrorCode.ANALYSIS_ERROR;
import static com.starburstdata.dataframe.DataframeException.ErrorCode.SQL_ERROR;
import static com.starburstdata.dataframe.analyzer.AnalyzerUtils.COMMA;
import static com.starburstdata.dataframe.analyzer.AnalyzerUtils.DOUBLE_QUOTE;
import static com.starburstdata.dataframe.analyzer.AnalyzerUtils.generateId;
import static com.starburstdata.dataframe.analyzer.AnalyzerUtils.limitStatement;
import static com.starburstdata.dataframe.analyzer.AnalyzerUtils.quoteName;
import static com.starburstdata.dataframe.analyzer.AnalyzerUtils.sortStatement;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.QueryType.DESCRIBE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DataframeMetadataProvider
        implements TrinoMetadata
{
    private static final Logger log = Logger.get(DataframeMetadataProvider.class);
    private final Session session;
    private final AnalyzerFactory analyzerFactory;
    private final SqlParser sqlParser;
    private final DataTypeMapper dataTypeMapper;

    public DataframeMetadataProvider(Session session, AnalyzerFactory analyzerFactory, SqlParser sqlParser, DataTypeMapper dataTypeMapper)
    {
        this.session = requireNonNull(session, "session is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.dataTypeMapper = requireNonNull(dataTypeMapper, "dataTypeMapper is null");
    }

    @Override
    public boolean tableExists(String tableName)
    {
        try {
            describe("SELECT 1 from " + tableName);
        }
        catch (TrinoException trinoException) {
            if (trinoException.getErrorCode() == TABLE_NOT_FOUND.toErrorCode()) {
                return false;
            }
            log.error(trinoException, "Error occurred during analysis");
            throw new DataframeException(trinoException.getMessage(), SQL_ERROR);
        }
        catch (Exception exception) {
            log.error(exception, "Unexpected error");
            throw new DataframeException(exception.getMessage(), ANALYSIS_ERROR);
        }
        return true;
    }

    @Override
    public Optional<List<Attribute>> resolveOutput(String sql)
    {
        return resolve(sql).map(analysis -> analysis.getRootScope().getRelationType().getVisibleFields().stream()
                .map(field -> new Attribute(
                        generateId(),
                        quoteName(DOUBLE_QUOTE + field.getName().orElseThrow(() -> new IllegalStateException("Dataframe columns should always be named"))
                                .toLowerCase(ENGLISH)
                                .replace(DOUBLE_QUOTE, DOUBLE_QUOTE + DOUBLE_QUOTE) + DOUBLE_QUOTE),
                        dataTypeMapper.fromTrinoType(field.getType()),
                        true))
                .toList());
    }

    @Override
    public String aliasOutput(String sql)
    {
        return resolve(sql).map(analysis -> {
            Collection<Field> visibleFields = analysis.getRootScope().getRelationType().getVisibleFields();
            if (analysis.getStatement() instanceof Query query &&
                    visibleFields.stream().anyMatch(field -> field.getName().isEmpty())) {
                // We found unnamed columns, let's rewrite the query
                QueryBody queryBody = query.getQueryBody();
                if (queryBody instanceof QuerySpecification querySpecification) {
                    // First we rewrite any single column unnamed expression
                    String rewrittenQueryString = SqlFormatter.formatSql(new Query(
                            query.getFunctions(),
                            query.getWith(),
                            new QuerySpecification(
                                    new Select(
                                            querySpecification.getSelect().isDistinct(),
                                            querySpecification.getSelect().getSelectItems().stream()
                                                    .map(selectItem -> {
                                                        if (selectItem instanceof AllColumns ignored) {
                                                            return selectItem;
                                                        }
                                                        if (selectItem instanceof SingleColumn singleColumn) {
                                                            if (singleColumn.getAlias().isPresent()) {
                                                                return selectItem;
                                                            }
                                                            return new SingleColumn(singleColumn.getExpression(), new Identifier(aliasColumn(singleColumn)));
                                                        }
                                                        throw new IllegalArgumentException("Could not map selectItem: " + selectItem);
                                                    }).collect(toImmutableList())),
                                    querySpecification.getFrom(),
                                    querySpecification.getWhere(),
                                    querySpecification.getGroupBy(),
                                    querySpecification.getHaving(),
                                    querySpecification.getWindows(),
                                    querySpecification.getOrderBy(),
                                    querySpecification.getOffset(),
                                    querySpecification.getLimit()),
                            query.getOrderBy(),
                            query.getOffset(),
                            query.getLimit()));

                    return resolve(rewrittenQueryString).map(rewrittenQueryAnalysis -> {
                        // now we take care of the remaining unnamed fields
                        Collection<Field> visibleFieldsRewrittenQuery = rewrittenQueryAnalysis.getRootScope().getRelationType().getVisibleFields();
                        if (rewrittenQueryAnalysis.getStatement() instanceof Query ignored &&
                                visibleFieldsRewrittenQuery.stream().anyMatch(field -> field.getName().isEmpty())) {
                            ImmutableList.Builder<String> identifiers = ImmutableList.builder();
                            int counter = 0;
                            for (Field field : visibleFieldsRewrittenQuery) {
                                identifiers.add(quoteName(field.getName().orElse("_" + ++counter)));
                            }
                            // TODO: this seems a bug in Trino
                            return "SELECT * FROM (" + rewrittenQueryString + ") t(" + String.join(COMMA, identifiers.build()) + ")";
                        }
                        return rewrittenQueryString;
                    }).orElse(rewrittenQueryString);
                }
            }
            return SqlFormatter.formatSql(analysis.getStatement());
        }).orElse(sql);
    }

    @Override
    public String limit(String sql, String limit, String offset)
    {
        String naiveQuery = limitStatement(limit, offset, sql);
        // check if the query is actually sorted or already contains a limit/offset
        Optional<Analysis> optionalAnalysis = resolve(sql);
        if (optionalAnalysis.isPresent()) {
            Analysis analysis = optionalAnalysis.get();
            if (analysis.getStatement() instanceof Query query) {
                QueryBody queryBody = query.getQueryBody();
                LongLiteral desiredOffset = new LongLiteral(offset);
                Offset offsetExpression = new Offset(desiredOffset);
                LongLiteral desiredLimit = new LongLiteral(limit);
                Limit limitExpression = new Limit(desiredLimit);
                if (queryBody instanceof QuerySpecification querySpecification && querySpecification.getLimit().isPresent()) {
                    if (query.getOffset().isPresent() || querySpecification.getOffset().isPresent()) {
                        // Don't rewrite queries with offset present
                        return naiveQuery;
                    }

                    if (querySpecification.getLimit().get() instanceof Limit originalLimit
                            && originalLimit.getRowCount() instanceof LongLiteral longLiteral
                            && desiredLimit.getValue().compareTo(longLiteral.getValue()) > 0) {
                        // No need to limit the query
                        return sql;
                    }

                    if (query.getLimit().isPresent()
                            && query.getLimit().get() instanceof Limit originalLimit
                            && originalLimit.getRowCount() instanceof LongLiteral longLiteral
                            && desiredLimit.getValue().compareTo(longLiteral.getValue()) > 0) {
                        // No need to limit the query
                        return sql;
                    }

                    return SqlFormatter.formatSql(new Query(
                            query.getFunctions(),
                            query.getWith(),
                            new QuerySpecification(
                                    querySpecification.getSelect(),
                                    querySpecification.getFrom(),
                                    querySpecification.getWhere(),
                                    querySpecification.getGroupBy(),
                                    querySpecification.getHaving(),
                                    querySpecification.getWindows(),
                                    querySpecification.getOrderBy(),
                                    Optional.of(offsetExpression),
                                    Optional.of(limitExpression)),
                            query.getOrderBy(),
                            query.getOffset(),
                            query.getLimit()));
                }

                // rewrite the query with the desired limit and offset
                return SqlFormatter.formatSql(new Query(
                        query.getFunctions(),
                        query.getWith(),
                        query.getQueryBody(),
                        query.getOrderBy(),
                        Optional.of(offsetExpression),
                        Optional.of(limitExpression)));
            }
        }
        return naiveQuery;
    }

    @Override
    public String sort(String sql, List<String> order)
    {
        String naiveSort = sortStatement(order, sql);
        // Let's try to apply the sort onto the existing query
        Analysis naivePlanAnalysis = resolve(naiveSort).orElseThrow(() -> new IllegalStateException("Could not analyze query: " + naiveSort));
        Analysis originalPlanAnalysis = resolve(sql).orElseThrow(() -> new IllegalStateException("Could not analyze query: " + sql));
        if (naivePlanAnalysis.getStatement() instanceof Query naiveQuery && originalPlanAnalysis.getStatement() instanceof Query originalQuery) {
            QueryBody naiveQueryQueryBody = naiveQuery.getQueryBody();
            QueryBody originalQueryBody = originalQuery.getQueryBody();
            if (naiveQueryQueryBody instanceof QuerySpecification naiveQuerySpecification && originalQueryBody instanceof QuerySpecification originalQuerySpecification) {
                return SqlFormatter.formatSql(new Query(
                        originalQuery.getFunctions(),
                        originalQuery.getWith(),
                        new QuerySpecification(
                                originalQuerySpecification.getSelect(),
                                originalQuerySpecification.getFrom(),
                                originalQuerySpecification.getWhere(),
                                originalQuerySpecification.getGroupBy(),
                                originalQuerySpecification.getHaving(),
                                originalQuerySpecification.getWindows(),
                                naiveQuerySpecification.getOrderBy(),
                                originalQuerySpecification.getOffset(),
                                originalQuerySpecification.getLimit()),
                        originalQuery.getOrderBy(),
                        originalQuery.getOffset(),
                        originalQuery.getLimit()));
            }
            // rewrite the query with the desired sort
            return SqlFormatter.formatSql(new Query(
                    originalQuery.getFunctions(),
                    originalQuery.getWith(),
                    originalQuery.getQueryBody(),
                    naiveQuery.getOrderBy(),
                    originalQuery.getOffset(),
                    originalQuery.getLimit()));
        }
        throw new IllegalArgumentException("Query can't be sorted: " + sql);
    }

    private static String aliasColumn(SingleColumn singleColumn)
    {
//        Auto aliasing columns based on the expression or name of the column.
//        When values are cast, the alias shouldn't be modified
        Expression expression = singleColumn.getExpression();
        if (expression instanceof Cast cast) {
            return cast.getExpression().toString().replace(DOUBLE_QUOTE, "");
        }
        return expression.toString().replace(DOUBLE_QUOTE, "");
    }

    private Optional<Analysis> resolve(String sqlStatement)
    {
        try {
            return describe(sqlStatement);
        }
        catch (ParsingException | TrinoException trinoException) {
            log.error(trinoException, "Error occurred during analysis");
            throw new DataframeException(trinoException.getMessage(), SQL_ERROR);
        }
        catch (Exception exception) {
            log.error(exception, "Unexpected error");
            throw new DataframeException(exception.getMessage(), ANALYSIS_ERROR);
        }
    }

    private Optional<Analysis> describe(String sqlStatement)
    {
        Statement statement = this.sqlParser.createStatement(sqlStatement);
        if (statement instanceof Query) {
            Analyzer analyzer = analyzerFactory.createAnalyzer(session, ImmutableList.of(), ImmutableMap.of(), WarningCollector.NOOP, createPlanOptimizersStatsCollector());
            return Optional.of(analyzer.analyze(statement, DESCRIBE));
        }
        return Optional.empty();
    }
}
