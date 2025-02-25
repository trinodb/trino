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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.QueryPreparer.PreparedQuery;
import io.trino.execution.SessionPropertyEvaluator;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.SessionProperty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class SessionPropertyResolver
{
    private final SessionPropertyEvaluator sessionPropertyEvaluator;
    private final AccessControl accessControl;

    @Inject
    public SessionPropertyResolver(SessionPropertyEvaluator sessionPropertyEvaluator, AccessControl accessControl)
    {
        this.sessionPropertyEvaluator = requireNonNull(sessionPropertyEvaluator, "sessionEvaluator is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    public SessionPropertiesApplier getSessionPropertiesApplier(PreparedQuery preparedQuery)
    {
        if (!(preparedQuery.getStatement() instanceof Query queryStatement)) {
            return session -> session;
        }
        return session -> prepareSession(session, queryStatement.getSessionProperties(), bindParameters(preparedQuery.getStatement(), preparedQuery.getParameters()));
    }

    private Session prepareSession(Session session, List<SessionProperty> sessionProperties, Map<NodeRef<Parameter>, Expression> parameters)
    {
        ResolvedSessionProperties resolvedSessionProperties = resolve(session, parameters, sessionProperties);
        return overrideProperties(session, resolvedSessionProperties);
    }

    private ResolvedSessionProperties resolve(Session session, Map<NodeRef<Parameter>, Expression> parameters, List<SessionProperty> sessionProperties)
    {
        ImmutableMap.Builder<String, String> systemProperties = ImmutableMap.builder();
        Table<String, String, String> catalogProperties = HashBasedTable.create();
        Set<QualifiedName> seenPropertyNames = new HashSet<>();

        for (SessionProperty sessionProperty : sessionProperties) {
            List<String> nameParts = sessionProperty.getName().getParts();

            if (!seenPropertyNames.add(sessionProperty.getName())) {
                throw semanticException(INVALID_SESSION_PROPERTY, sessionProperty, "Session property '%s' already set", sessionProperty.getName());
            }

            if (nameParts.size() == 1) {
                systemProperties.put(nameParts.getFirst(), sessionPropertyEvaluator.evaluate(session, sessionProperty.getName(), sessionProperty.getValue(), parameters));
            }
            else if (nameParts.size() == 2) {
                String catalogName = nameParts.getFirst();
                String propertyName = nameParts.getLast();
                catalogProperties.put(catalogName, propertyName, sessionPropertyEvaluator.evaluate(session, sessionProperty.getName(), sessionProperty.getValue(), parameters));
            }
            else {
                throw semanticException(INVALID_SESSION_PROPERTY, sessionProperty, "Invalid session property '%s'", sessionProperty.getName());
            }
        }

        return new ResolvedSessionProperties(systemProperties.buildOrThrow(), catalogProperties.rowMap());
    }

    private Session overrideProperties(Session session, ResolvedSessionProperties resolvedSessionProperties)
    {
        requireNonNull(resolvedSessionProperties, "resolvedSessionProperties is null");

        validateSystemProperties(session, resolvedSessionProperties.systemProperties());

        // Catalog session properties were already evaluated so we need to evaluate overrides
        if (session.getTransactionId().isPresent()) {
            validateCatalogProperties(session, resolvedSessionProperties.catalogProperties());
        }

        // NOTE: properties are validated before calling overrideProperties
        Map<String, String> systemProperties = new HashMap<>();
        systemProperties.putAll(session.getSystemProperties());
        systemProperties.putAll(resolvedSessionProperties.systemProperties());

        Map<String, Map<String, String>> catalogProperties = new HashMap<>(session.getCatalogProperties());
        for (Map.Entry<String, Map<String, String>> catalogEntry : resolvedSessionProperties.catalogProperties().entrySet()) {
            catalogProperties.computeIfAbsent(catalogEntry.getKey(), _ -> new HashMap<>())
                    .putAll(catalogEntry.getValue());
        }

        return session.withProperties(systemProperties, catalogProperties);
    }

    private void validateSystemProperties(Session session, Map<String, String> systemProperties)
    {
        for (Map.Entry<String, String> property : systemProperties.entrySet()) {
            // verify permissions
            accessControl.checkCanSetSystemSessionProperty(session.getIdentity(), session.getQueryId(), property.getKey());
        }
    }

    private void validateCatalogProperties(Session session, Map<String, Map<String, String>> catalogsProperties)
    {
        checkState(session.getTransactionId().isPresent(), "Not in transaction");
        for (Map.Entry<String, Map<String, String>> catalogProperties : catalogsProperties.entrySet()) {
            for (Map.Entry<String, String> catalogProperty : catalogProperties.getValue().entrySet()) {
                // verify permissions
                accessControl.checkCanSetCatalogSessionProperty(new SecurityContext(session.getRequiredTransactionId(), session.getIdentity(), session.getQueryId(), session.getStart()), catalogProperties.getKey(), catalogProperty.getKey());
            }
        }
    }

    public record ResolvedSessionProperties(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties)
    {
        public ResolvedSessionProperties
        {
            systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
            catalogProperties = ImmutableMap.copyOf(requireNonNull(catalogProperties, "catalogProperties is null"));
        }
    }

    @FunctionalInterface
    public interface SessionPropertiesApplier
            extends Function<Session, Session>
    {
    }
}
