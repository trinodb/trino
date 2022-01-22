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
import io.trino.connector.CatalogName;
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
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.PropertyUtil.evaluateProperties;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

abstract class AbstractCatalogPropertyManager
{
    private final ConcurrentMap<CatalogName, Map<String, PropertyMetadata<?>>> connectorProperties = new ConcurrentHashMap<>();
    private final String propertyType;
    private final ErrorCodeSupplier propertyError;

    protected AbstractCatalogPropertyManager(String propertyType, ErrorCodeSupplier propertyError)
    {
        this.propertyType = propertyType;
        this.propertyError = requireNonNull(propertyError, "propertyError is null");
    }

    public void addProperties(CatalogName catalogName, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(properties, "properties is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);

        checkState(connectorProperties.putIfAbsent(catalogName, propertiesByName) == null, "Properties for key %s are already registered", catalogName);
    }

    public void removeProperties(CatalogName catalogName)
    {
        connectorProperties.remove(catalogName);
    }

    public Map<String, Object> getProperties(
            CatalogName catalog,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean includeAllProperties)
    {
        Map<String, Optional<Object>> nullableValues = getNullableProperties(
                catalog,
                properties,
                session,
                plannerContext,
                accessControl,
                parameters,
                includeAllProperties);
        return nullableValues.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().orElseThrow()));
    }

    public Map<String, Optional<Object>> getNullableProperties(
            CatalogName catalog,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean includeAllProperties)
    {
        Map<String, PropertyMetadata<?>> propertyMetadata = connectorProperties.get(catalog);
        if (propertyMetadata == null) {
            throw new TrinoException(NOT_FOUND, format("Catalog '%s' %s property not found", catalog, propertyType));
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
                format("catalog '%s' %s property", catalog, propertyType));
    }

    public Collection<PropertyMetadata<?>> getAllProperties(CatalogName catalogName)
    {
        return Optional.ofNullable(connectorProperties.get(catalogName))
                .map(Map::values)
                .orElse(ImmutableList.of());
    }
}
