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

import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.PropertyUtil.evaluateProperties;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

abstract class AbstractCatalogPropertyManager
{
    private final String propertyType;
    private final ErrorCodeSupplier propertyError;
    private final CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorProperties;

    protected AbstractCatalogPropertyManager(
            String propertyType,
            ErrorCodeSupplier propertyError,
            CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorProperties)
    {
        this.propertyType = requireNonNull(propertyType, "propertyType is null");
        this.propertyError = requireNonNull(propertyError, "propertyError is null");
        this.connectorProperties = requireNonNull(connectorProperties, "connectorProperties is null");
    }

    public Map<String, Object> getProperties(
            String catalogName,
            CatalogHandle catalogHandle,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean includeAllProperties)
    {
        Map<String, Optional<Object>> nullableValues = getNullableProperties(
                catalogName,
                catalogHandle,
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
            String catalogName,
            CatalogHandle catalogHandle,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean includeAllProperties)
    {
        Map<String, PropertyMetadata<?>> propertyMetadata = connectorProperties.getService(catalogHandle);
        return evaluateProperties(
                properties,
                session,
                plannerContext,
                accessControl,
                parameters,
                includeAllProperties,
                propertyMetadata,
                propertyError,
                format("catalog '%s' %s property", catalogName, propertyType));
    }

    public Collection<PropertyMetadata<?>> getAllProperties(CatalogHandle catalogHandle)
    {
        return connectorProperties.getService(catalogHandle).values();
    }
}
