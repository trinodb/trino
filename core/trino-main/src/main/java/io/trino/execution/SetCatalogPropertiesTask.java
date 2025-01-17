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
package io.trino.execution;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.security.AccessControl;
import io.trino.spi.catalog.CatalogName;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.SetCatalogProperties;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.PropertyUtil.evaluateProperty;
import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SetCatalogPropertiesTask
        implements DataDefinitionTask<SetCatalogProperties>
{
    private final CatalogManager catalogManager;
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;

    @Inject
    public SetCatalogPropertiesTask(CatalogManager catalogManager, PlannerContext plannerContext, AccessControl accessControl)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET CATALOG PROPERTIES";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetCatalogProperties statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        String catalogName = statement.getName().getValue().toLowerCase(ENGLISH);

        Map<String, Optional<String>> properties = getProperties(statement, parameters, session);

        catalogManager.alterCatalog(new CatalogName(catalogName), properties);

        return immediateVoidFuture();
    }

    private Map<String, Optional<String>> getProperties(SetCatalogProperties statement, List<Expression> parameters, Session session)
    {
        Map<NodeRef<Parameter>, Expression> boundParameters = bindParameters(statement, parameters);
        ImmutableMap.Builder<String, Optional<String>> propertiesBuilder = ImmutableMap.builder();
        for (Property property : statement.getProperties()) {
            String name = property.getName().getValue();
            if (property.isSetToDefault()) {
                propertiesBuilder.put(name, Optional.empty());
            }
            else {
                propertiesBuilder.put(
                        name,
                        Optional.of((String) evaluateProperty(
                                extractLocation(property),
                                property.getName().getValue(),
                                VARCHAR,
                                property.getNonDefaultValue(),
                                session,
                                plannerContext,
                                accessControl,
                                boundParameters,
                                INVALID_CATALOG_PROPERTY,
                                "catalog property")));
            }
        }
        return propertiesBuilder.buildKeepingLast();
    }
}
