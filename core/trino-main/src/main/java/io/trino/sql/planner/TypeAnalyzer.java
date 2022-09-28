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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AnalyzePropertyManager;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;

import javax.inject.Inject;

import java.util.Map;

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

    @Inject
    public TypeAnalyzer(PlannerContext plannerContext, StatementAnalyzerFactory statementAnalyzerFactory)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
    }

    public Map<NodeRef<Expression>, Type> getTypes(Session session, TypeProvider inputTypes, Iterable<Expression> expressions)
    {
        return analyzeExpressions(
                session,
                plannerContext,
                statementAnalyzerFactory,
                new AllowAllAccessControl(),
                inputTypes,
                expressions,
                ImmutableMap.of(),
                WarningCollector.NOOP,
                OTHERS)
                .getExpressionTypes();
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
                        new TablePropertyManager(CatalogServiceProvider.fail("table properties not supported in testing type analyzer")),
                        new AnalyzePropertyManager(CatalogServiceProvider.fail("analyze properties not supported in testing type analyzer"))));
    }
}
