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
import info.debatty.java.stringsimilarity.JaroWinkler;
import info.debatty.java.stringsimilarity.interfaces.StringSimilarity;
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
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.TIME_ZONE_ID;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.metadata.SessionPropertyManager.evaluatePropertyValue;
import static io.trino.metadata.SessionPropertyManager.serializeSessionProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.Comparator.comparingDouble;
import static java.util.Objects.requireNonNull;

public class SessionPropertyEvaluator
{
    private static final StringSimilarity SIMILARITY = new JaroWinkler();

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
                    .orElseThrow(() -> semanticException(INVALID_SESSION_PROPERTY, expression, "Session property '%s' does not exist%s", name, suggest(name, sessionPropertyManager.getSystemSessionPropertiesMetadata())));

            return evaluate(session, name, expression, parameters, systemPropertyMetadata);
        }
        else if (nameParts.size() == 2) {
            String catalogName = nameParts.getFirst();
            String propertyName = nameParts.getLast();

            CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), session, expression, catalogName);
            PropertyMetadata<?> connectorPropertyMetadata = sessionPropertyManager.getConnectorSessionPropertyMetadata(catalogHandle, propertyName)
                    .orElseThrow(() -> semanticException(INVALID_SESSION_PROPERTY, expression, "Session property '%s' does not exist%s", name, suggest(name, sessionPropertyManager.getConnectionSessionPropertiesMetadata(catalogHandle))));

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

    public static List<PropertyMetadata<?>> findSimilar(String propertyName, Set<PropertyMetadata<?>> candidates, int count)
    {
        return candidates.stream()
                .filter(property -> !property.isHidden())
                .map(candidate -> new Match(candidate, SIMILARITY.similarity(candidate.getName(), propertyName)))
                .filter(match -> match.ratio() > 0.85)
                .sorted(comparingDouble(Match::ratio).reversed())
                .limit(count)
                .map(Match::metadata)
                .collect(toImmutableList());
    }

    private record Match(PropertyMetadata<?> metadata, double ratio)
    {
        public Match
        {
            requireNonNull(metadata, "metadata is null");
            verify(ratio >= 0.0 && ratio <= 1.0, "ratio must be in the [0, 1.0] range");
        }
    }

    private static String suggest(QualifiedName propertyName, Set<PropertyMetadata<?>> knownProperties)
    {
        List<PropertyMetadata<?>> suggestions = findSimilar(propertyName.getSuffix(), knownProperties, 3);
        if (suggestions.isEmpty()) {
            return "";
        }

        return ". Did you mean to use " + switch (suggestions.size()) {
            case 3 -> "'" + formatSuggestion(propertyName, suggestions.get(0)) + "', '" + formatSuggestion(propertyName, suggestions.get(1)) + "' or '" + formatSuggestion(propertyName, suggestions.get(2)) + "'?";
            case 2 -> "'" + formatSuggestion(propertyName, suggestions.get(0)) + "' or '" + formatSuggestion(propertyName, suggestions.get(1)) + "'?";
            default -> "'" + formatSuggestion(propertyName, suggestions.get(0)) + "'?";
        };
    }

    private static String formatSuggestion(QualifiedName name, PropertyMetadata<?> suggestion)
    {
        if (name.getParts().size() == 2) {
            return name.getParts().getFirst() + "." + suggestion.getName();
        }

        return suggestion.getName();
    }
}
