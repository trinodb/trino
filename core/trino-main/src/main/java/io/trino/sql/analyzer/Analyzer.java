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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.security.AccessControl;
import io.trino.spi.security.GroupProvider;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class Analyzer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;
    private final List<Expression> parameters;
    private final Map<NodeRef<Parameter>, Expression> parameterLookup;
    private final WarningCollector warningCollector;
    private final StatsCalculator statsCalculator;

    public Analyzer(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            GroupProvider groupProvider,
            AccessControl accessControl,
            Optional<QueryExplainer> queryExplainer,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            StatsCalculator statsCalculator)
    {
        this.session = requireNonNull(session, "session is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
        this.parameters = parameters;
        this.parameterLookup = parameterLookup;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    public Analysis analyze(Statement statement)
    {
        return analyze(statement, false);
    }

    public Analysis analyze(Statement statement, boolean isDescribe)
    {
        Statement rewrittenStatement = StatementRewrite.rewrite(session, metadata, sqlParser, queryExplainer, statement, parameters, parameterLookup, groupProvider, accessControl, warningCollector, statsCalculator);
        Analysis analysis = new Analysis(rewrittenStatement, parameterLookup, isDescribe);
        StatementAnalyzer analyzer = new StatementAnalyzer(analysis, metadata, sqlParser, groupProvider, accessControl, session, warningCollector, CorrelationSupport.ALLOWED);
        analyzer.analyze(rewrittenStatement, Optional.empty());

        // check column access permissions for each table
        analysis.getTableColumnReferences().forEach((accessControlInfo, tableColumnReferences) ->
                tableColumnReferences.forEach((tableName, columns) ->
                        accessControlInfo.getAccessControl().checkCanSelectFromColumns(
                                accessControlInfo.getSecurityContext(session.getRequiredTransactionId(), session.getQueryId()),
                                tableName,
                                columns)));
        return analysis;
    }

    static void verifyNoAggregateWindowOrGroupingFunctions(Metadata metadata, Expression predicate, String clause)
    {
        List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), metadata);

        List<Expression> windowExpressions = extractWindowExpressions(ImmutableList.of(predicate));

        List<GroupingOperation> groupingOperations = extractExpressions(ImmutableList.of(predicate), GroupingOperation.class);

        List<Expression> found = ImmutableList.copyOf(Iterables.concat(
                aggregates,
                windowExpressions,
                groupingOperations));

        if (!found.isEmpty()) {
            throw semanticException(EXPRESSION_NOT_SCALAR, predicate, "%s cannot contain aggregations, window functions or grouping operations: %s", clause, found);
        }
    }
}
