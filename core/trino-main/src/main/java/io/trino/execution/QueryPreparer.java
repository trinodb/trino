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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.TrinoException;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Statement;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.trino.execution.ParameterExtractor.getParameterCount;
import static io.trino.spi.StandardErrorCode.INVALID_PARAMETER_USAGE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.analyzer.ConstantExpressionVerifier.verifyExpressionIsConstant;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.util.StatementUtils.getQueryType;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;

public class QueryPreparer
{
    private final SqlParser sqlParser;

    @Inject
    public QueryPreparer(SqlParser sqlParser)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public PreparedQuery prepareQuery(Session session, String query)
            throws ParsingException, TrinoException
    {
        Statement wrappedStatement = sqlParser.createStatement(query, createParsingOptions(session));
        return prepareQuery(session, wrappedStatement);
    }

    public PreparedQuery prepareQuery(Session session, Statement wrappedStatement)
            throws ParsingException, TrinoException
    {
        Statement statement = wrappedStatement;
        Optional<String> prepareSql = Optional.empty();
        if (statement instanceof Execute) {
            prepareSql = Optional.of(session.getPreparedStatementFromExecute((Execute) statement));
            statement = sqlParser.createStatement(prepareSql.get(), createParsingOptions(session));
        }

        if (statement instanceof ExplainAnalyze) {
            Statement innerStatement = ((ExplainAnalyze) statement).getStatement();
            Optional<QueryType> innerQueryType = getQueryType(innerStatement);
            if (innerQueryType.isEmpty() || innerQueryType.get() == QueryType.DATA_DEFINITION) {
                throw new TrinoException(NOT_SUPPORTED, "EXPLAIN ANALYZE doesn't support statement type: " + innerStatement.getClass().getSimpleName());
            }
        }
        List<Expression> parameters = ImmutableList.of();
        if (wrappedStatement instanceof Execute) {
            parameters = ((Execute) wrappedStatement).getParameters();
        }
        validateParameters(statement, parameters);
        return new PreparedQuery(statement, parameters, prepareSql);
    }

    private static void validateParameters(Statement node, List<Expression> parameterValues)
    {
        int parameterCount = getParameterCount(node);
        if (parameterValues.size() != parameterCount) {
            throw semanticException(INVALID_PARAMETER_USAGE, node, "Incorrect number of parameters: expected %s but found %s", parameterCount, parameterValues.size());
        }
        for (Expression expression : parameterValues) {
            verifyExpressionIsConstant(emptySet(), expression);
        }
    }

    public static class PreparedQuery
    {
        private final Statement statement;
        private final List<Expression> parameters;
        private final Optional<String> prepareSql;

        public PreparedQuery(Statement statement, List<Expression> parameters, Optional<String> prepareSql)
        {
            this.statement = requireNonNull(statement, "statement is null");
            this.parameters = ImmutableList.copyOf(requireNonNull(parameters, "parameters is null"));
            this.prepareSql = requireNonNull(prepareSql, "prepareSql is null");
        }

        public Statement getStatement()
        {
            return statement;
        }

        public List<Expression> getParameters()
        {
            return parameters;
        }

        public Optional<String> getPrepareSql()
        {
            return prepareSql;
        }
    }
}
