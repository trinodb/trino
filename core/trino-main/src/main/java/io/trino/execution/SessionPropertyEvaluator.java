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

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.TIME_ZONE_ID;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.metadata.SessionPropertyManager.evaluatePropertyValue;
import static io.trino.metadata.SessionPropertyManager.serializeSessionProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SessionPropertyEvaluator
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;
    private final Optional<TimeZoneKey> forcedSessionTimeZone;

    @Inject
    public SessionPropertyEvaluator(PlannerContext plannerContext, AccessControl accessControl, SessionPropertyManager sessionPropertyManager, SqlEnvironmentConfig environmentConfig)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.forcedSessionTimeZone = requireNonNull(environmentConfig, "environmentConfig is null").getForcedSessionTimeZone();
    }

    public String evaluate(Session session, QualifiedName name, Expression expression, Map<NodeRef<Parameter>, Expression> parameters)
    {
        List<String> nameParts = name.getParts();
        if (nameParts.size() == 1) {
            PropertyMetadata<?> systemPropertyMetadata = sessionPropertyManager.getSystemSessionPropertyMetadata(nameParts.getFirst())
                    .orElseThrow(() -> semanticException(INVALID_SESSION_PROPERTY, expression, "Session property %s does not exist", name));

            return evaluate(session, name, expression, parameters, systemPropertyMetadata);
        }
        else if (nameParts.size() == 2) {
            String catalogName = nameParts.getFirst();
            String propertyName = nameParts.getLast();

            CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), session, expression, catalogName);
            PropertyMetadata<?> connectorPropertyMetadata = sessionPropertyManager.getConnectorSessionPropertyMetadata(catalogHandle, propertyName)
                    .orElseThrow(() -> semanticException(INVALID_SESSION_PROPERTY, expression, "Session property %s does not exist", name));

            return evaluate(session, name, expression, parameters, connectorPropertyMetadata);
        }
        throw semanticException(INVALID_SESSION_PROPERTY, expression, "Invalid session property '%s'", name);
    }

    private String evaluate(Session session, QualifiedName name, Expression expression, Map<NodeRef<Parameter>, Expression> parameters, PropertyMetadata<?> propertyMetadata)
    {
        if (propertyMetadata.getName().equals(TIME_ZONE_ID) && forcedSessionTimeZone.isPresent()) {
            return serializeSessionProperty(propertyMetadata.getSqlType(), forcedSessionTimeZone.get().toString());
        }

        Type type = propertyMetadata.getSqlType();
        Object objectValue;

        try {
            objectValue = evaluatePropertyValue(expression, type, session, plannerContext, accessControl, parameters);
        }
        catch (TrinoException e) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    format("Unable to set session property '%s' to '%s': %s", name, expression, e.getRawMessage()));
        }

        String value = serializeSessionProperty(type, objectValue);

        try {
            // verify the SQL value can be decoded by the property
            propertyMetadata.decode(objectValue);
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_SESSION_PROPERTY, expression, "%s", e.getMessage());
        }

        return value;
    }
}
