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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.Session;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.FunctionResolver;
import io.trino.security.AccessControl;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.EXPRESSION_NOT_SCALAR;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractWindowExpressions;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.util.Objects.requireNonNull;

public class Analyzer
{
    private final AnalyzerFactory analyzerFactory;
    private final StatementAnalyzerFactory statementAnalyzerFactory;
    private final Session session;
    private final List<Expression> parameters;
    private final Map<NodeRef<Parameter>, Expression> parameterLookup;
    private final WarningCollector warningCollector;
    private final PlanOptimizersStatsCollector planOptimizersStatsCollector;
    private final Tracer tracer;
    private final StatementRewrite statementRewrite;

    Analyzer(
            Session session,
            AnalyzerFactory analyzerFactory,
            StatementAnalyzerFactory statementAnalyzerFactory,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            Tracer tracer,
            StatementRewrite statementRewrite)
    {
        this.session = requireNonNull(session, "session is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.parameters = ImmutableList.copyOf(parameters);
        this.parameterLookup = ImmutableMap.copyOf(parameterLookup);
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        this.planOptimizersStatsCollector = requireNonNull(planOptimizersStatsCollector, "planOptimizersStatsCollector is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.statementRewrite = requireNonNull(statementRewrite, "statementRewrite is null");
    }

    public Analysis analyze(Statement statement)
    {
        Span span = tracer.spanBuilder("analyzer")
                .setParent(Context.current().with(session.getQuerySpan()))
                .startSpan();
        try (var _ = scopedSpan(span)) {
            return analyze(statement, OTHERS);
        }
    }

    public Analysis analyze(Statement statement, QueryType queryType)
    {
        Statement rewrittenStatement = statementRewrite.rewrite(analyzerFactory, session, statement, parameters, parameterLookup, warningCollector, planOptimizersStatsCollector);
        Analysis analysis = new Analysis(rewrittenStatement, parameterLookup, queryType);
        StatementAnalyzer analyzer = statementAnalyzerFactory.createStatementAnalyzer(analysis, session, warningCollector, CorrelationSupport.ALLOWED);

        try (var _ = scopedSpan(tracer, "analyze")) {
            analyzer.analyze(rewrittenStatement);
        }

        try (var _ = scopedSpan(tracer, "access-control")) {
            // check column access permissions for each table
            analysis.getTableColumnReferences().forEach((accessControlInfo, tableColumnReferences) ->
                    tableColumnReferences.forEach((tableName, columns) ->
                            accessControlInfo.getAccessControl().checkCanSelectFromColumns(
                                    accessControlInfo.getSecurityContext(session.getRequiredTransactionId(), session.getQueryId(), session.getStart()),
                                    tableName,
                                    columns)));
        }

        return analysis;
    }

    static void verifyNoAggregateWindowOrGroupingFunctions(Session session, FunctionResolver functionResolver, AccessControl accessControl, Expression predicate, String clause)
    {
        List<FunctionCall> aggregates = extractAggregateFunctions(ImmutableList.of(predicate), session, functionResolver, accessControl);

        List<Expression> windowExpressions = extractWindowExpressions(ImmutableList.of(predicate), session, functionResolver, accessControl);

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
