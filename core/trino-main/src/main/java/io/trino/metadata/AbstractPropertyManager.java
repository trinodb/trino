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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ParameterRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.DUPLICATE_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

abstract class AbstractPropertyManager<K>
{
    protected final ConcurrentMap<K, Map<String, PropertyMetadata<?>>> connectorProperties = new ConcurrentHashMap<>();
    private final String propertyType;
    private final ErrorCodeSupplier propertyError;

    protected AbstractPropertyManager(String propertyType, ErrorCodeSupplier propertyError)
    {
        requireNonNull(propertyType, "propertyType is null");
        this.propertyType = propertyType;
        this.propertyError = requireNonNull(propertyError, "propertyError is null");
    }

    protected final void doAddProperties(K propertiesKey, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(propertiesKey, "propertiesKey is null");
        requireNonNull(properties, "properties is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);

        checkState(connectorProperties.putIfAbsent(propertiesKey, propertiesByName) == null, "Properties for key %s are already registered", propertiesKey);
    }

    protected final void doRemoveProperties(K propertiesKey)
    {
        connectorProperties.remove(propertiesKey);
    }

    protected final Properties doGetOnlySpecifiedProperties(
            K propertiesKey,
            String catalogNameForDiagnostics,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedPropertiesMetadata = getSupportedPropertiesMetadata(propertiesKey, catalogNameForDiagnostics);

        SeparatedProperties separatedProperties = separateProperties(propertiesKey, catalogNameForDiagnostics, properties);

        // builder for properties of which the values (after being evaluated or looked up) are non-null
        ImmutableMap.Builder<String, Object> nonNullPropertiesBuilder;

        // evaluate the value expressions of the properties that are not set to DEFAULT (the results are necessarily non-null)
        nonNullPropertiesBuilder =
                evaluateSqlProperties(
                        propertiesKey,
                        supportedPropertiesMetadata,
                        catalogNameForDiagnostics,
                        separatedProperties.nonDefaultProperties,
                        session,
                        plannerContext,
                        accessControl,
                        parameters);

        // builder for names of properties of which the values are null
        ImmutableSet.Builder<String> nullPropertyNamesBuilder = ImmutableSet.builder();

        // for each property set to DEFAULT, look up its default value; if the default value is non-null, then put the property into
        // nonNullPropertiesBuilder, otherwise put its name into nullPropertyNamesBuilder
        for (String name : separatedProperties.defaultPropertyNames) {
            PropertyMetadata<?> propertyMetadata = supportedPropertiesMetadata.get(name);
            if (propertyMetadata == null) {
                throw new TrinoException(
                        propertyError,
                        format("%s does not support %s property '%s'",
                                formatPropertiesKeyForMessage(catalogNameForDiagnostics, propertiesKey),
                                propertyType,
                                name));
            }
            Object value = propertyMetadata.getDefaultValue();
            if (value != null) {
                nonNullPropertiesBuilder.put(name, value);
            }
            else {
                nullPropertyNamesBuilder.add(name);
            }
        }

        return new Properties(nonNullPropertiesBuilder.build(), nullPropertyNamesBuilder.build());
    }

    protected final Map<String, Object> doGetProperties(
            K propertiesKey,
            String catalogNameForDiagnostics,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedPropertiesMetadata = getSupportedPropertiesMetadata(propertiesKey, catalogNameForDiagnostics);
        SeparatedProperties separatedProperties = separateProperties(propertiesKey, catalogNameForDiagnostics, properties);

        // check that all the properties that are set to DEFAULT are supported
        for (String name : separatedProperties.defaultPropertyNames) {
            if (!supportedPropertiesMetadata.containsKey(name)) {
                throw new TrinoException(
                        propertyError,
                        format("%s does not support %s property '%s'",
                                formatPropertiesKeyForMessage(catalogNameForDiagnostics, propertiesKey),
                                propertyType,
                                name));
            }
        }

        // call the version of doGetProperties that takes properties as a Map<String, Expression> to evaluate the properties that are not
        // set to DEFAULT (the properties that are set to DEFAULT (and any properties that are not specified) will be filled in if their
        // default values are not null)
        return doGetProperties(
                propertiesKey,
                catalogNameForDiagnostics,
                separatedProperties.nonDefaultProperties,
                session,
                plannerContext,
                accessControl,
                parameters);
    }

    protected final Map<String, Object> doGetProperties(
            K propertiesKey,
            String catalogNameForDiagnostics,
            Map<String, Expression> sqlProperties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedPropertiesMetadata = getSupportedPropertiesMetadata(propertiesKey, catalogNameForDiagnostics);
        ImmutableMap.Builder<String, Object> propertiesBuilder =
                evaluateSqlProperties(
                        propertiesKey,
                        supportedPropertiesMetadata,
                        catalogNameForDiagnostics,
                        sqlProperties,
                        session,
                        plannerContext,
                        accessControl,
                        parameters);

        Map<String, Object> specifiedProperties = propertiesBuilder.build();

        // Fill in the remaining properties with non-null defaults
        for (PropertyMetadata<?> propertyMetadata : supportedPropertiesMetadata.values()) {
            if (!specifiedProperties.containsKey(propertyMetadata.getName())) {
                Object value = propertyMetadata.getDefaultValue();
                if (value != null) {
                    propertiesBuilder.put(propertyMetadata.getName(), value);
                }
            }
        }

        return propertiesBuilder.build();
    }

    protected final Collection<PropertyMetadata<?>> doGetAllProperties(K key)
    {
        return Optional.ofNullable(connectorProperties.get(key))
                .map(Map::values)
                .orElse(ImmutableList.of());
    }

    /**
     * Separates the {@link Property}'s in {@code properties} based on whether they are set to DEFAULT or not
     */
    private SeparatedProperties separateProperties(
            K propertiesKey,
            String catalogNameForDiagnostics,
            Iterable<Property> properties)
    {
        Map<String, Expression> nonDefaultProperties = new HashMap<>();
        Set<String> defaultPropertyNames = new HashSet<>();
        for (Property property : properties) {
            String name = property.getName().getValue().toLowerCase(ENGLISH);   // property names are case-insensitive and normalized to lower case
            if (nonDefaultProperties.containsKey(name) || defaultPropertyNames.contains(name)) {
                throw new TrinoException(
                        DUPLICATE_PROPERTY,
                        format("%s %s property '%s' is specified more than once",
                                formatPropertiesKeyForMessage(catalogNameForDiagnostics, propertiesKey),
                                propertyType,
                                name));
            }
            if (!property.isSetToDefault()) {
                nonDefaultProperties.put(name, property.getNonDefaultValue());
            }
            else {
                defaultPropertyNames.add(name);
            }
        }
        return new SeparatedProperties(nonDefaultProperties, defaultPropertyNames);
    }

    /**
     * Represents the result of separating a collection of {@link Property}'s based on whether they are set to DEFAULT or not. This is the
     * return type of {@link #separateProperties}.
     */
    private static class SeparatedProperties
    {
        final Map<String, Expression> nonDefaultProperties;
        final Set<String> defaultPropertyNames;

        SeparatedProperties(Map<String, Expression> nonDefaultProperties, Set<String> defaultPropertyNames)
        {
            this.nonDefaultProperties = ImmutableMap.copyOf(requireNonNull(nonDefaultProperties, "nonDefaultProperties is null"));
            this.defaultPropertyNames = ImmutableSet.copyOf(requireNonNull(defaultPropertyNames, "defaultPropertyNames is null"));
        }
    }

    private Map<String, PropertyMetadata<?>> getSupportedPropertiesMetadata(K propertiesKey, String catalogNameForDiagnostics)
    {
        Map<String, PropertyMetadata<?>> supportedPropertiesMetadata = connectorProperties.get(propertiesKey);
        if (supportedPropertiesMetadata == null) {
            throw new TrinoException(NOT_FOUND, formatPropertiesKeyForMessage(catalogNameForDiagnostics, propertiesKey) + " not found");
        }
        return supportedPropertiesMetadata;
    }

    /**
     * Evaluates {@code sqlProperties}. The results are decoded Java objects and are put into an {@code ImmutableMap.Builder<String, Object>}
     * (as opposed to an {@code ImmutableMap<String, Object>} so that a caller has the option of adding more entries to the {@code Builder}
     * before building an {@code ImmutableMap}).
     */
    private ImmutableMap.Builder<String, Object> evaluateSqlProperties(
            K propertiesKey,
            Map<String, PropertyMetadata<?>> supportedPropertiesMetadata,
            String catalogNameForDiagnostics,
            Map<String, Expression> sqlProperties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        ImmutableMap.Builder<String, Object> propertiesBuilder = ImmutableMap.builder();
        for (Map.Entry<String, Expression> sqlProperty : sqlProperties.entrySet()) {
            String propertyName = sqlProperty.getKey().toLowerCase(ENGLISH);   // property names are case-insensitive and normalized to lower case
            PropertyMetadata<?> propertyMetadata = supportedPropertiesMetadata.get(propertyName);
            if (propertyMetadata == null) {
                throw new TrinoException(
                        propertyError,
                        format("%s does not support %s property '%s'",
                                formatPropertiesKeyForMessage(catalogNameForDiagnostics, propertiesKey),
                                propertyType,
                                propertyName));
            }

            Object sqlObjectValue;
            try {
                sqlObjectValue = evaluatePropertyValue(sqlProperty.getValue(), propertyMetadata.getSqlType(), session, plannerContext, accessControl, parameters);
            }
            catch (TrinoException e) {
                throw new TrinoException(
                        propertyError,
                        format("Invalid value for %s property '%s': Cannot convert [%s] to %s",
                                propertyType,
                                propertyMetadata.getName(),
                                sqlProperty.getValue(),
                                propertyMetadata.getSqlType()),
                        e);
            }

            Object value;
            try {
                value = propertyMetadata.decode(sqlObjectValue);
            }
            catch (Exception e) {
                throw new TrinoException(
                        propertyError,
                        format(
                                "Unable to set %s property '%s' to [%s]: %s",
                                propertyType,
                                propertyMetadata.getName(),
                                sqlProperty.getValue(),
                                e.getMessage()),
                        e);
            }

            propertiesBuilder.put(propertyMetadata.getName(), value);
        }

        return propertiesBuilder;
    }

    private Object evaluatePropertyValue(
            Expression expression,
            Type expectedType,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
        Object value = evaluateConstantExpression(rewritten, expectedType, plannerContext, session, accessControl, parameters);

        // convert to object value type of SQL type
        BlockBuilder blockBuilder = expectedType.createBlockBuilder(null, 1);
        writeNativeValue(expectedType, blockBuilder, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);

        if (objectValue == null) {
            throw new TrinoException(propertyError, format("Invalid null value for %s property", propertyType));
        }
        return objectValue;
    }

    protected abstract String formatPropertiesKeyForMessage(String catalogName, K propertiesKey);
}
