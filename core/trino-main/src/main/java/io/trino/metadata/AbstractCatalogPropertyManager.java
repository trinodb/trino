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
import io.trino.connector.CatalogName;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;

abstract class AbstractCatalogPropertyManager
        extends AbstractPropertyManager<CatalogName>
{
    private final String propertyType;

    protected AbstractCatalogPropertyManager(String propertyType, ErrorCodeSupplier propertyError)
    {
        super(propertyError);
        this.propertyType = propertyType;
    }

    public void addProperties(CatalogName catalogName, List<PropertyMetadata<?>> properties)
    {
        doAddProperties(catalogName, properties);
    }

    public void removeProperties(CatalogName catalogName)
    {
        doRemoveProperties(catalogName);
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
        return doGetNullableProperties(
                catalog,
                properties,
                session,
                plannerContext,
                accessControl,
                parameters,
                includeAllProperties,
                format("catalog '%s' %s property", catalog, propertyType));
    }

    public Collection<PropertyMetadata<?>> getAllProperties(CatalogName catalogName)
    {
        return doGetAllProperties(catalogName);
    }
}
