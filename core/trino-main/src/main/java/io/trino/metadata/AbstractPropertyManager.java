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

import com.google.common.collect.ImmutableMap;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
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

    protected final Map<String, Object> doGetProperties(
            K propertiesKey,
            String catalogNameForDiagnostics,
            Map<String, Expression> sqlPropertyValues,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean setDefaultProperties)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = connectorProperties.get(propertiesKey);
        if (supportedProperties == null) {
            throw new TrinoException(NOT_FOUND, formatPropertiesKeyForMessage(catalogNameForDiagnostics, propertiesKey) + " not found");
        }

        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();

        // Fill in user-specified properties
        for (Map.Entry<String, Expression> sqlProperty : sqlPropertyValues.entrySet()) {
            String propertyName = sqlProperty.getKey().toLowerCase(ENGLISH);
            PropertyMetadata<?> property = supportedProperties.get(propertyName);
            if (property == null) {
                throw new TrinoException(
                        propertyError,
                        format("%s does not support %s property '%s'",
                                formatPropertiesKeyForMessage(catalogNameForDiagnostics, propertiesKey),
                                propertyType,
                                propertyName));
            }

            Object sqlObjectValue;
            try {
                sqlObjectValue = evaluatePropertyValue(sqlProperty.getValue(), property.getSqlType(), session, plannerContext, accessControl, parameters);
            }
            catch (TrinoException e) {
                throw new TrinoException(
                        propertyError,
                        format("Invalid value for %s property '%s': Cannot convert [%s] to %s",
                                propertyType,
                                property.getName(),
                                sqlProperty.getValue(),
                                property.getSqlType()),
                        e);
            }

            Object value;
            try {
                value = property.decode(sqlObjectValue);
            }
            catch (Exception e) {
                throw new TrinoException(
                        propertyError,
                        format(
                                "Unable to set %s property '%s' to [%s]: %s",
                                propertyType,
                                property.getName(),
                                sqlProperty.getValue(),
                                e.getMessage()),
                        e);
            }

            properties.put(property.getName(), value);
        }
        Map<String, Object> userSpecifiedProperties = properties.buildOrThrow();

        if (!setDefaultProperties) {
            return properties.buildOrThrow();
        }
        // Fill in the remaining properties with non-null defaults
        for (PropertyMetadata<?> propertyMetadata : supportedProperties.values()) {
            if (!userSpecifiedProperties.containsKey(propertyMetadata.getName())) {
                Object value = propertyMetadata.getDefaultValue();
                if (value != null) {
                    properties.put(propertyMetadata.getName(), value);
                }
            }
        }
        return properties.buildOrThrow();
    }

    protected final Map<K, Map<String, PropertyMetadata<?>>> doGetAllProperties()
    {
        return ImmutableMap.copyOf(connectorProperties);
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
