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

import com.google.common.collect.Maps;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metadata.PropertyUtil.evaluateProperties;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableProceduresPropertyManager
{
    private final ConcurrentMap<Key, Map<String, PropertyMetadata<?>>> connectorProperties = new ConcurrentHashMap<>();

    public void addProperties(CatalogName catalogName, String procedureName, List<PropertyMetadata<?>> properties)
    {
        requireNonNull(catalogName, "catalogName is null");
        requireNonNull(procedureName, "procedureName is null");
        requireNonNull(catalogName, "catalogName is null");

        Map<String, PropertyMetadata<?>> propertiesByName = Maps.uniqueIndex(properties, PropertyMetadata::getName);

        Key propertiesKey = new Key(catalogName, procedureName);
        checkState(connectorProperties.putIfAbsent(propertiesKey, propertiesByName) == null, "Properties for key %s are already registered", propertiesKey);
    }

    public void removeProperties(CatalogName catalogName)
    {
        Set<Key> keysToRemove = connectorProperties.keySet().stream()
                .filter(key -> catalogName.equals(key.getCatalogName()))
                .collect(toImmutableSet());
        for (Key key : keysToRemove) {
            connectorProperties.remove(key);
        }
    }

    public Map<String, Object> getProperties(
            CatalogName catalog,
            String procedureName,
            Map<Identifier, Expression> sqlPropertyValues,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        Map<String, PropertyMetadata<?>> supportedProperties = connectorProperties.get(new Key(catalog, procedureName));
        if (supportedProperties == null) {
            throw new TrinoException(NOT_FOUND, format("Catalog '%s' table procedure '%s' property not found", catalog, procedureName));
        }

        Map<String, Optional<Object>> propertyValues = evaluateProperties(
                sqlPropertyValues.entrySet().stream()
                        .map(entry -> new Property(entry.getKey(), entry.getValue()))
                        .collect(toImmutableList()),
                session,
                plannerContext,
                accessControl,
                parameters,
                true,
                supportedProperties,
                INVALID_PROCEDURE_ARGUMENT,
                format("catalog '%s' table procedure '%s' property", catalog, procedureName));
        return propertyValues.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().orElseThrow()));
    }

    static final class Key
    {
        private final CatalogName catalogName;
        private final String procedureName;

        private Key(CatalogName catalogName, String procedureName)
        {
            this.catalogName = requireNonNull(catalogName, "catalogName is null");
            this.procedureName = requireNonNull(procedureName, "procedureName is null");
        }

        public CatalogName getCatalogName()
        {
            return catalogName;
        }

        public String getProcedureName()
        {
            return procedureName;
        }

        @Override
        public String toString()
        {
            return catalogName + ":" + procedureName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key key = (Key) o;
            return Objects.equals(catalogName, key.catalogName)
                    && Objects.equals(procedureName, key.procedureName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalogName, procedureName);
        }
    }
}
