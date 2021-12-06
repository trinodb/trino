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

import java.util.List;
import java.util.Map;

abstract class AbstractCatalogPropertyManager
        extends AbstractPropertyManager<CatalogName>
{
    protected AbstractCatalogPropertyManager(String propertyType, ErrorCodeSupplier propertyError)
    {
        super(propertyType, propertyError);
    }

    public void addProperties(CatalogName catalogName, List<PropertyMetadata<?>> properties)
    {
        doAddProperties(catalogName, properties);
    }

    public void removeProperties(CatalogName catalogName)
    {
        doRemoveProperties(catalogName);
    }

    /**
     * Evaluate {@code properties}. The returned {@code Map&lt;String, Object&gt;} contains a supported property iff its value is
     * (implicitly or explictly) set to a non-{@code null} value. Thus,
     * <ul>
     *     <li>If a property does not appear in the result, then its value is {@code null}</li>
     *     <li>If a {@code Property} in {@code properties} is set to DEFAULT, then it will appear in the result iff the default value is not
     *     {@code null}.</li>
     *     <li>If a property does not appear in {@code properties} at all, then it will appear in the result iff its default value is not
     *     {@code null}. (Thus, specifying a property to have the DEFAULT value is the same as not specifying it at all.)</li>
     * </ul>
     */
    public Map<String, Object> getProperties(
            CatalogName catalog,
            String catalogNameForDiagnostics,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        return doGetProperties(
                catalog,
                catalogNameForDiagnostics,
                properties,
                session,
                plannerContext,
                accessControl,
                parameters);
    }

    /**
     * Evaluate {@code properties}. Unlike {@link #getProperties}, a property appears in the returned result iff it is specified in
     * {@code properties}.
     */
    public Properties getOnlySpecifiedProperties(
            CatalogName catalog,
            String catalogNameForDiagnostics,
            Iterable<Property> properties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        return doGetOnlySpecifiedProperties(
                catalog,
                catalogNameForDiagnostics,
                properties,
                session,
                plannerContext,
                accessControl,
                parameters);
    }

    public Map<CatalogName, Map<String, PropertyMetadata<?>>> getAllProperties()
    {
        return doGetAllProperties();
    }

    @Override
    protected String formatPropertiesKeyForMessage(String catalogName, CatalogName ignored)
    {
        return "Catalog '" + catalogName + "'";
    }
}
