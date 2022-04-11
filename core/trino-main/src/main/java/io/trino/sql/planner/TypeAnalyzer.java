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
package io.trino.sql.planner;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.ExpressionAnalyzer;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeRef;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeExpressions;
import static io.trino.sql.analyzer.QueryType.OTHERS;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static java.util.Objects.requireNonNull;

/**
 * This class is to facilitate obtaining the type of an expression and its subexpressions
 * during planning (i.e., when interacting with IR expression). It will eventually get
 * removed when we split the AST from the IR and we encode the type directly into IR expressions.
 */
public class TypeAnalyzer
{
    private final PlannerContext plannerContext;
    private final StatementAnalyzerFactory statementAnalyzerFactory;

    private final NonEvictableCache<QueryId, QueryScopedCachedTypeAnalyzer> typeAnalyzersCache = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    // Try to evict queries cache as soon as possible to keep cache relatively small
                    .expireAfterAccess(15, TimeUnit.SECONDS)
                    .maximumSize(256)
                    .softValues()
                    .recordStats());

    @Inject
    public TypeAnalyzer(PlannerContext plannerContext, StatementAnalyzerFactory statementAnalyzerFactory)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
    }

    public Map<NodeRef<Expression>, Type> getTypes(Session session, TypeProvider inputTypes, Iterable<Expression> expressions)
    {
        try {
            return typeAnalyzersCache.get(session.getQueryId(), () -> new QueryScopedCachedTypeAnalyzer(plannerContext, statementAnalyzerFactory))
                    .getTypes(session, inputTypes, ImmutableList.copyOf(expressions));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<NodeRef<Expression>, Type> getTypes(Session session, TypeProvider inputTypes, Expression expression)
    {
        return getTypes(session, inputTypes, ImmutableList.of(expression));
    }

    public Type getType(Session session, TypeProvider inputTypes, Expression expression)
    {
        return getTypes(session, inputTypes, expression).get(NodeRef.of(expression));
    }

    public static TypeAnalyzer createTestingTypeAnalyzer(PlannerContext plannerContext)
    {
        return new TypeAnalyzer(
                plannerContext,
                createTestingStatementAnalyzerFactory(
                        plannerContext,
                        new AllowAllAccessControl(),
                        new TablePropertyManager(),
                        new AnalyzePropertyManager()));
    }

    private static class QueryScopedCachedTypeAnalyzer
    {
        private final Cache<NodeRef<Expression>, Type> typesCache = buildNonEvictableCache(CacheBuilder.newBuilder());
        private PlannerContext plannerContext;
        private StatementAnalyzerFactory statementAnalyzerFactory;

        private QueryScopedCachedTypeAnalyzer(PlannerContext plannerContext, StatementAnalyzerFactory statementAnalyzerFactory)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        }

        private Map<NodeRef<Expression>, Type> getTypes(Session session, TypeProvider inputTypes, List<Expression> expressions)
        {
            List<NodeRef<Expression>> expressionsToResolve = collectExpressions(expressions);
            Map<NodeRef<Expression>, Type> cachedTypes = typesCache.getAllPresent(expressionsToResolve);

            // All expressions were resolved from cache
            if (cachedTypes.size() == expressionsToResolve.size()) {
                return cachedTypes;
            }

            Map<NodeRef<Expression>, Type> resolvedTypes = analyzeExpressions(createExpressionAnalyzer(session, plannerContext, statementAnalyzerFactory, inputTypes), expressions)
                    .getExpressionTypes();

            typesCache.putAll(resolvedTypes);
            return resolvedTypes;
        }

        private static ExpressionAnalyzer createExpressionAnalyzer(Session session,
                                                                  PlannerContext plannerContext,
                                                                  StatementAnalyzerFactory statementAnalyzerFactory,
                                                                  TypeProvider types)
        {
            return new ExpressionAnalyzer(plannerContext, new AllowAllAccessControl(), statementAnalyzerFactory, new Analysis(null, ImmutableMap.of(), OTHERS), session, types, WarningCollector.NOOP);
        }

        private static ImmutableList<NodeRef<Expression>> collectExpressions(Iterable<? extends Node> expressions)
        {
            ImmutableList.Builder<NodeRef<Expression>> builder = ImmutableList.builder();

            for (Node expression : expressions) {
                if (expression instanceof Expression) {
                    builder.add(NodeRef.of((Expression) expression));
                }

                if (!expression.getChildren().isEmpty()) {
                    builder.addAll(collectExpressions(expression.getChildren()));
                }
            }

            return builder.build();
        }
    }
}
