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
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableProceduresPropertyManager
        extends AbstractPropertyManager<TableProceduresPropertyManager.Key>
{
    public TableProceduresPropertyManager()
    {
        super("procedure", INVALID_PROCEDURE_ARGUMENT);
    }

    public void addProperties(CatalogName catalogName, String procedureName, List<PropertyMetadata<?>> properties)
    {
        doAddProperties(new Key(catalogName, procedureName), properties);
    }

    public void removeProperties(CatalogName catalogName)
    {
        Set<Key> keysToRemove = connectorProperties.keySet().stream()
                .filter(key -> catalogName.equals(key.getCatalogName()))
                .collect(toImmutableSet());
        for (Key key : keysToRemove) {
            doRemoveProperties(key);
        }
    }

    public Map<String, Object> getProperties(
            CatalogName catalog,
            String procedureName,
            String catalogNameForDiagnostics,
            Map<String, Expression> sqlPropertyValues,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        return doGetProperties(
                new Key(catalog, procedureName),
                catalogNameForDiagnostics,
                sqlPropertyValues,
                session,
                plannerContext,
                accessControl,
                parameters);
    }

    public Map<Key, Map<String, PropertyMetadata<?>>> getAllProperties()
    {
        return doGetAllProperties();
    }

    @Override
    protected String formatPropertiesKeyForMessage(String catalogName, Key propertiesKey)
    {
        return format("Catalog %s table procedure %s", catalogName, propertiesKey.procedureName);
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
