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
import io.trino.connector.CatalogServiceProvider;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.TableProcedureMetadata;
import io.trino.spi.session.PropertyMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.metadata.PropertyUtil.evaluateProperties;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableProceduresPropertyManager
{
    private final CatalogServiceProvider<CatalogTableProcedures> tableProceduresProvider;

    public TableProceduresPropertyManager(CatalogServiceProvider<CatalogTableProcedures> tableProceduresProvider)
    {
        this.tableProceduresProvider = requireNonNull(tableProceduresProvider, "tableProceduresProvider is null");
    }

    public Map<String, Object> getProperties(
            String catalogName,
            CatalogHandle catalogHandle,
            String procedureName,
            Map<String, Expression> sqlPropertyValues,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters)
    {
        TableProcedureMetadata tableProcedure = tableProceduresProvider.getService(catalogHandle).getTableProcedure(procedureName);
        Map<String, PropertyMetadata<?>> supportedProperties = Maps.uniqueIndex(tableProcedure.getProperties(), PropertyMetadata::getName);

        Map<String, Optional<Object>> propertyValues = evaluateProperties(
                sqlPropertyValues.entrySet().stream()
                        .map(entry -> new Property(new Identifier(entry.getKey()), entry.getValue()))
                        .collect(toImmutableList()),
                session,
                plannerContext,
                accessControl,
                parameters,
                true,
                supportedProperties,
                INVALID_PROCEDURE_ARGUMENT,
                format("catalog '%s' table procedure '%s' property", catalogName, procedureName));
        return propertyValues.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().orElseThrow()));
    }
}
