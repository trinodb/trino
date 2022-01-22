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
import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.metadata.PropertyUtil.evaluateProperties;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

abstract class AbstractPropertyManager<K>
{
    protected final ConcurrentMap<K, Map<String, PropertyMetadata<?>>> connectorProperties = new ConcurrentHashMap<>();
    private final ErrorCodeSupplier propertyError;

    protected AbstractPropertyManager(ErrorCodeSupplier propertyError)
    {
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

    protected final Collection<PropertyMetadata<?>> doGetAllProperties(K key)
    {
        return Optional.ofNullable(connectorProperties.get(key))
                .map(Map::values)
                .orElse(ImmutableList.of());
    }

    protected final Map<String, Optional<Object>> doGetNullableProperties(
            K propertiesKey,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean includeAllProperties,
            String propertyTypeDescription)
    {
        Map<String, PropertyMetadata<?>> propertyMetadata = connectorProperties.get(propertiesKey);
        if (propertyMetadata == null) {
            throw new TrinoException(NOT_FOUND, capitalize(propertyTypeDescription) + " not found");
        }

        return evaluateProperties(
                properties,
                session,
                plannerContext,
                accessControl,
                parameters,
                includeAllProperties,
                propertyMetadata,
                propertyError,
                propertyTypeDescription);
    }

    private static String capitalize(String value)
    {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }
}
