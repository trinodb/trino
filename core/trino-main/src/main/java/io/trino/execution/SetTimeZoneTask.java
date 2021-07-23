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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.cost.StatsCalculator;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.security.GroupProvider;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.CorrelationSupport;
import io.trino.sql.analyzer.ExpressionAnalysis;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.RelationId;
import io.trino.sql.analyzer.RelationType;
import io.trino.sql.analyzer.Scope;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.ExpressionInterpreter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionRewriter;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.SetTimeZone;
import io.trino.transaction.TransactionManager;
import io.trino.type.IntervalDayTimeType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.SystemSessionProperties.TIME_ZONE_ID;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.util.Failures.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SetTimeZoneTask
        implements DataDefinitionTask<SetTimeZone>
{
    private final SqlParser sqlParser;
    private final GroupProvider groupProvider;
    private final StatsCalculator statsCalculator;

    @Inject
    public SetTimeZoneTask(SqlParser sqlParser, GroupProvider groupProvider, StatsCalculator statsCalculator)
    {
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public String getName()
    {
        return "SET TIME ZONE";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetTimeZone statement,
            TransactionManager transactionManager,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        String timeZoneId = statement.getTimeZone()
                .map(timeZone -> getTimeZoneId(timeZone, statement, metadata, accessControl, stateMachine, parameters, warningCollector))
                .orElse(TimeZone.getDefault().getID());
        stateMachine.addSetSessionProperties(TIME_ZONE_ID, timeZoneId);

        return immediateVoidFuture();
    }

    private String getTimeZoneId(
            Expression expression,
            SetTimeZone statement,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        Analysis analysis = analyzeStatement(statement, metadata, accessControl, stateMachine, parameters, session);
        Scope scope = Scope.builder()
                .withRelationType(RelationId.anonymous(), new RelationType())
                .build();
        ExpressionAnalysis expressionAnalysis = ExpressionAnalyzer.analyzeExpression(
                session,
                metadata,
                groupProvider,
                accessControl,
                sqlParser,
                scope,
                analysis,
                expression,
                warningCollector,
                CorrelationSupport.ALLOWED);
        Expression rewrittenExpression = rewriteExpression(expression, analysis);
        Type type = expressionAnalysis.getType(expression);
        if (!(type instanceof VarcharType || type instanceof IntervalDayTimeType)) {
            throw new TrinoException(TYPE_MISMATCH, format("Expected expression of varchar or interval day-time type, but '%s' has %s type", expression, type.getDisplayName()));
        }

        Map<NodeRef<Expression>, Type> expressionTypes = ImmutableMap.<NodeRef<Expression>, Type>builder()
                .put(NodeRef.of(rewrittenExpression), type)
                .build();
        ExpressionInterpreter interpreter = new ExpressionInterpreter(rewrittenExpression, metadata, session, expressionTypes);
        Object timeZoneValue = interpreter.evaluate();

        TimeZoneKey timeZoneKey;
        if (timeZoneValue instanceof Slice) {
            timeZoneKey = getTimeZoneKey(((Slice) timeZoneValue).toStringUtf8());
        }
        else if (timeZoneValue instanceof Long) {
            timeZoneKey = getTimeZoneKeyForOffset(getZoneOffsetMinutes((Long) timeZoneValue));
        }
        else {
            throw new IllegalStateException(format("Time Zone expression '%s' not supported", expression));
        }
        return timeZoneKey.getId();
    }

    private Analysis analyzeStatement(
            SetTimeZone statement,
            Metadata metadata,
            AccessControl accessControl,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            Session session)
    {
        return new Analyzer(
                session,
                metadata,
                sqlParser,
                groupProvider,
                accessControl,
                Optional.empty(),
                parameters,
                parameterExtractor(statement, parameters),
                stateMachine.getWarningCollector(),
                statsCalculator)
                .analyze(statement);
    }

    private static Expression rewriteExpression(Expression expression, Analysis analysis)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                ResolvedFunction resolvedFunction = analysis.getResolvedFunction(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);

                FunctionCall rewritten = treeRewriter.defaultRewrite(node, context);
                rewritten = new FunctionCall(
                        rewritten.getLocation(),
                        resolvedFunction.toQualifiedName(),
                        rewritten.getWindow(),
                        rewritten.getFilter(),
                        rewritten.getOrderBy(),
                        rewritten.isDistinct(),
                        rewritten.getNullTreatment(),
                        rewritten.getProcessingMode(),
                        rewritten.getArguments());
                return rewritten;
            }
        }, expression, null);
    }

    private static long getZoneOffsetMinutes(long interval)
    {
        checkCondition((interval % 60_000L) == 0L, INVALID_LITERAL, "Invalid time zone offset interval: interval contains seconds");
        return interval / 60_000L;
    }
}
