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
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.SessionSpecification;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.metadata.SessionPropertyManager.evaluatePropertyValue;
import static io.trino.metadata.SessionPropertyManager.serializeSessionProperty;
import static io.trino.spi.StandardErrorCode.CATALOG_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SessionSpecificationEvaluator
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;

    @Inject
    public SessionSpecificationEvaluator(PlannerContext plannerContext, AccessControl accessControl, SessionPropertyManager sessionPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    public SessionSpecificationsApplier getSessionSpecificationApplier(PreparedQuery preparedQuery)
    {
        if (!(preparedQuery.getStatement() instanceof Query queryStatement)) {
            return session -> session;
        }
        return session -> prepareSession(session, queryStatement.getSessionProperties(), bindParameters(preparedQuery.getStatement(), preparedQuery.getParameters()));
    }

    private Session prepareSession(Session session, List<SessionSpecification> specifications, Map<NodeRef<Parameter>, Expression> parameters)
    {
        ResolvedSessionSpecifications resolvedSessionSpecifications = resolve(session, parameters, specifications);
        return overrideProperties(session, resolvedSessionSpecifications);
    }

    private ResolvedSessionSpecifications resolve(Session session, Map<NodeRef<Parameter>, Expression> parameters, List<SessionSpecification> specifications)
    {
        ImmutableMap.Builder<String, String> sessionProperties = ImmutableMap.builder();
        Table<String, String, String> catalogProperties = HashBasedTable.create();
        Set<QualifiedName> seenPropertyNames = new HashSet<>();

        for (SessionSpecification specification : specifications) {
            List<String> nameParts = specification.getName().getParts();

            if (!seenPropertyNames.add(specification.getName())) {
                throw semanticException(INVALID_SESSION_PROPERTY, specification, "Session property %s already set", specification.getName());
            }

            if (nameParts.size() == 1) {
                Optional<PropertyMetadata<?>> systemSessionPropertyMetadata = sessionPropertyManager.getSystemSessionPropertyMetadata(nameParts.getFirst());
                if (systemSessionPropertyMetadata.isEmpty()) {
                    throw semanticException(INVALID_SESSION_PROPERTY, specification, "Session property %s does not exist", specification.getName());
                }
                sessionProperties.put(nameParts.getFirst(), toSessionValue(session, parameters, specification, systemSessionPropertyMetadata.get()));
            }
            else if (nameParts.size() == 2) {
                String catalogName = nameParts.getFirst();
                String propertyName = nameParts.getLast();

                CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), session, specification, catalogName);
                Optional<PropertyMetadata<?>> connectorSessionPropertyMetadata = sessionPropertyManager.getConnectorSessionPropertyMetadata(catalogHandle, propertyName);
                if (connectorSessionPropertyMetadata.isEmpty()) {
                    throw semanticException(INVALID_SESSION_PROPERTY, specification, "Session property %s does not exist", specification.getName());
                }
                catalogProperties.put(catalogName, propertyName, toSessionValue(session, parameters, specification, connectorSessionPropertyMetadata.get()));
            }
            else {
                throw semanticException(INVALID_SESSION_PROPERTY, specification, "Invalid session property '%s'", specification.getName());
            }
        }

        return new ResolvedSessionSpecifications(sessionProperties.buildOrThrow(), catalogProperties.rowMap());
    }

    private Session overrideProperties(Session session, ResolvedSessionSpecifications resolvedSessionSpecifications)
    {
        requireNonNull(resolvedSessionSpecifications, "resolvedSessionSpecifications is null");

        // TODO Consider moving validation to Session.withProperties method
        validateSystemProperties(session, resolvedSessionSpecifications.systemProperties());

        // Catalog session properties were already evaluated so we need to evaluate overrides
        if (session.getTransactionId().isPresent()) {
            validateCatalogProperties(session, resolvedSessionSpecifications.catalogProperties());
        }

        // NOTE: properties are validated before calling overrideProperties
        Map<String, String> systemProperties = new HashMap<>();
        systemProperties.putAll(session.getSystemProperties());
        systemProperties.putAll(resolvedSessionSpecifications.systemProperties());

        Map<String, Map<String, String>> catalogProperties = new HashMap<>(session.getCatalogProperties());
        for (Map.Entry<String, Map<String, String>> catalogEntry : resolvedSessionSpecifications.catalogProperties().entrySet()) {
            catalogProperties.computeIfAbsent(catalogEntry.getKey(), id -> new HashMap<>())
                    .putAll(catalogEntry.getValue());
        }

        return session.withProperties(systemProperties, catalogProperties);
    }

    // TODO Consider extracting a method from SetSessionTask and reusing it here
    private String toSessionValue(Session session, Map<NodeRef<Parameter>, Expression> parameters, SessionSpecification specification, PropertyMetadata<?> propertyMetadata)
    {
        Type type = propertyMetadata.getSqlType();
        Object objectValue;

        try {
            objectValue = evaluatePropertyValue(specification.getValue(), type, session, plannerContext, accessControl, parameters);
        }
        catch (TrinoException e) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    format("Unable to set session property '%s' to '%s': %s", specification.getName(), specification.getValue(), e.getRawMessage()));
        }

        String value = serializeSessionProperty(type, objectValue);
        // verify the SQL value can be decoded by the property
        try {
            propertyMetadata.decode(objectValue);
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_SESSION_PROPERTY, specification, "%s", e.getMessage());
        }

        return value;
    }

    private void validateSystemProperties(Session session, Map<String, String> systemProperties)
    {
        for (Map.Entry<String, String> property : systemProperties.entrySet()) {
            // verify permissions
            accessControl.checkCanSetSystemSessionProperty(session.getIdentity(), session.getQueryId(), property.getKey());
            // validate session property value
            sessionPropertyManager.validateSystemSessionProperty(property.getKey(), property.getValue());
        }
    }

    private void validateCatalogProperties(Session session, Map<String, Map<String, String>> catalogsProperties)
    {
        checkState(session.getTransactionId().isPresent(), "Not in transaction");
        for (Map.Entry<String, Map<String, String>> catalogProperties : catalogsProperties.entrySet()) {
            CatalogHandle catalogHandle = plannerContext.getMetadata().getCatalogHandle(session, catalogProperties.getKey())
                    .orElseThrow(() -> new TrinoException(CATALOG_NOT_FOUND, "Catalog '%s' not found".formatted(catalogProperties.getKey())));

            for (Map.Entry<String, String> catalogProperty : catalogProperties.getValue().entrySet()) {
                // verify permissions
                accessControl.checkCanSetCatalogSessionProperty(new SecurityContext(session.getRequiredTransactionId(), session.getIdentity(), session.getQueryId(), session.getStart()), catalogProperties.getKey(), catalogProperty.getKey());
                // validate catalog session property value
                sessionPropertyManager.validateCatalogSessionProperty(catalogProperties.getKey(), catalogHandle, catalogProperty.getKey(), catalogProperty.getValue());
            }
        }
    }

    public record ResolvedSessionSpecifications(Map<String, String> systemProperties, Map<String, Map<String, String>> catalogProperties)
    {
        public ResolvedSessionSpecifications
        {
            systemProperties = ImmutableMap.copyOf(requireNonNull(systemProperties, "systemProperties is null"));
            catalogProperties = ImmutableMap.copyOf(requireNonNull(catalogProperties, "catalogProperties is null"));
        }
    }

    @FunctionalInterface
    public interface SessionSpecificationsApplier
            extends Function<Session, Session>
    {
    }
}
