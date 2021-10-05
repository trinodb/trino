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
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.List;
import java.util.Map;

abstract class AbstractCatalogPropertyManager
        extends AbstractPropertyManager<CatalogName>
{
    protected AbstractCatalogPropertyManager(String propertyType, ErrorCodeSupplier propertyError)
    {
        super(propertyType, propertyError);
    }

    public final void addProperties(CatalogName catalogName, List<PropertyMetadata<?>> properties)
    {
        innerAddProperties(catalogName, properties);
    }

    public final void removeProperties(CatalogName catalogName)
    {
        innerRemoveProperties(catalogName);
    }

    public final Map<String, Object> getProperties(
            CatalogName catalog,
            String catalogName, // only use this for error messages
            Map<String, Expression> sqlPropertyValues,
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean setDefaultProperties)
    {
        return innerGetProperties(
                catalog,
                catalogName,
                sqlPropertyValues,
                session,
                metadata,
                accessControl,
                parameters,
                setDefaultProperties);
    }

    public Map<CatalogName, Map<String, PropertyMetadata<?>>> getAllProperties()
    {
        return innerGetAllProperties();
    }

    @Override
    protected String formatPropertiesKeyForMessage(String catalogName, CatalogName ignored)
    {
        return "Catalog '" + catalogName + "'";
    }
}
