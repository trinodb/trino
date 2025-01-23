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
package io.trino.sql;

import com.google.inject.Inject;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.connector.CatalogFactory;
import io.trino.execution.CreateCatalogTask;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.metadata.CatalogManager;
import io.trino.security.AccessControl;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.ConnectorName;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.weakref.jmx.$internal.guava.collect.ImmutableSet.toImmutableSet;

public class SensitiveStatementRedactor
{
    private static final String REDACTED_VALUE = "***";

    private final boolean enabled;
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final CatalogFactory catalogFactory;
    private final CatalogManager catalogManager;

    @Inject
    public SensitiveStatementRedactor(
            FeaturesConfig config,
            PlannerContext plannerContext,
            AccessControl accessControl,
            CatalogFactory catalogFactory,
            CatalogManager catalogManager)
    {
        this.enabled = requireNonNull(config, "config is null").isStatementRedactingEnabled();
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    public RedactedQuery redact(String rawQuery, PreparedQuery preparedQuery, Session session)
    {
        if (enabled) {
            Statement statement = preparedQuery.getStatement();
            List<Expression> parameters = preparedQuery.getParameters();
            // Redaction is performed at an early stage of query processing, before the session
            // is registered in the language function manager. Since property evaluation may require
            // accessing functions for the session, we temporarily register the session here.
            plannerContext.getLanguageFunctionManager().registerQuery(session);
            try {
                RedactingVisitor visitor = new RedactingVisitor(session, parameters);
                Node redactedStatement = visitor.process(statement);
                if (visitor.isRedacted()) {
                    String redactedQuery = formatRedactedSql(preparedQuery.getWrappedStatement(), redactedStatement);
                    if (preparedQuery.getPrepareSql().isPresent()) {
                        return new RedactedQuery(rawQuery, Optional.of(redactedQuery));
                    }
                    return new RedactedQuery(redactedQuery, Optional.empty());
                }
            }
            finally {
                plannerContext.getLanguageFunctionManager().unregisterQuery(session);
            }
        }
        return new RedactedQuery(rawQuery, preparedQuery.getPrepareSql());
    }

    private static String formatRedactedSql(Statement wrappedStatement, Node redactedStatement)
    {
        String redactedSql = SqlFormatter.formatSql(redactedStatement);
        if (wrappedStatement instanceof ExecuteImmediate executeImmediate) {
            ExecuteImmediate redactedExecuteImmediate = new ExecuteImmediate(
                    executeImmediate.getLocation().orElseThrow(),
                    new StringLiteral(executeImmediate.getStatement().getLocation().orElseThrow(), redactedSql),
                    executeImmediate.getParameters());
            return SqlFormatter.formatSql(redactedExecuteImmediate);
        }
        return redactedSql;
    }

    private static List<Property> redactProperties(List<Property> properties, Set<String> sensitiveProperties)
    {
        return properties.stream()
                .map(property -> {
                    if (sensitiveProperties.contains(property.getName().getValue())) {
                        return new Property(property.getName(), new StringLiteral(REDACTED_VALUE));
                    }
                    return property;
                })
                .collect(toImmutableList());
    }

    private class RedactingVisitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final List<Expression> parameters;

        private boolean redacted;

        public RedactingVisitor(Session session, List<Expression> parameters)
        {
            this.session = requireNonNull(session, "session is null");
            this.parameters = requireNonNull(parameters, "parameters is null");
        }

        public boolean isRedacted()
        {
            return redacted;
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        @Override
        protected Node visitPrepare(Prepare node, Void context)
        {
            Statement statement = (Statement) process(node.getStatement());
            return new Prepare(node.getLocation().orElseThrow(), node.getName(), statement);
        }

        @Override
        protected Node visitExplain(Explain node, Void context)
        {
            Statement statement = (Statement) process(node.getStatement());
            return new Explain(node.getLocation().orElseThrow(), statement, node.getOptions());
        }

        @Override
        protected Node visitExplainAnalyze(ExplainAnalyze node, Void context)
        {
            Statement statement = (Statement) process(node.getStatement());
            return new ExplainAnalyze(node.getLocation().orElseThrow(), statement, node.isVerbose());
        }

        @Override
        protected Node visitCreateCatalog(CreateCatalog node, Void context)
        {
            List<Property> properties = node.getProperties();
            ConnectorName connectorName = new ConnectorName(node.getConnectorName().getValue());
            CatalogName catalogName = new CatalogName(node.getCatalogName().getValue());

            Set<String> sensitiveProperties;
            try {
                Map<String, String> evaluatedProperties = CreateCatalogTask.evaluateProperties(node, session, plannerContext, accessControl, parameters);
                CatalogProperties catalogProperties = catalogManager.createCatalogProperties(catalogName, connectorName, evaluatedProperties);
                sensitiveProperties = catalogFactory.getSecuritySensitivePropertyNames(catalogProperties);
            }
            catch (RuntimeException e) {
                // To obtain security-sensitive properties, we need to perform a few steps that usually occur during
                // the execution phase, such as evaluating properties, resolving secrets, validating the configuration, etc.
                // If any exception occurs while performing these steps preemptively, we don't want to fail the entire query
                // because it's not the redactor's responsibility. Instead, we take a defensive approach and mask all the properties.
                sensitiveProperties = getPropertyNames(properties);
            }

            List<Property> redactedProperties = redact(properties, sensitiveProperties);

            return node.withProperties(redactedProperties);
        }

        private List<Property> redact(List<Property> properties, Set<String> sensitiveProperties)
        {
            redacted = true;
            return redactProperties(properties, sensitiveProperties);
        }

        private static Set<String> getPropertyNames(List<Property> properties)
        {
            return properties.stream()
                    .map(Property::getName)
                    .map(Identifier::getValue)
                    .collect(toImmutableSet());
        }
    }
}
