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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.connector.ConnectorName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.metadata.PropertyUtil;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Property;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.spi.StandardErrorCode.INVALID_CATALOG_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class CreateCatalogTask
        implements DataDefinitionTask<CreateCatalog>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final CatalogManager catalogManager;

    @Inject
    public CreateCatalogTask(PlannerContext plannerContext, AccessControl accessControl, CatalogManager catalogManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
    }

    @Override
    public String getName()
    {
        return "CREATE CATALOG";
    }

    @Override
    public ListenableFuture<Void> execute(
            CreateCatalog statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        if (statement.getPrincipal().isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "CREATE CATALOG with AUTHORIZATION is not yet supported");
        }

        Session session = stateMachine.getSession();
        String catalog = statement.getCatalogName().toString();
        accessControl.checkCanCreateCatalog(session.toSecurityContext(), catalog);

        Map<String, String> properties = new HashMap<>();
        for (Property property : statement.getProperties()) {
            if (property.isSetToDefault()) {
                throw semanticException(INVALID_CATALOG_PROPERTY, property, "Catalog properties do not support DEFAULT value");
            }
            properties.put(
                    property.getName().getValue(),
                    (String) PropertyUtil.evaluateProperty(
                            property.getName().getValue(),
                            VARCHAR,
                            property.getNonDefaultValue(),
                            session,
                            plannerContext,
                            accessControl,
                            bindParameters(statement, parameters),
                            INVALID_CATALOG_PROPERTY,
                            "catalog property"));
        }

        ConnectorName connectorName = new ConnectorName(statement.getConnectorName().toString());
        catalogManager.createCatalog(catalog, connectorName, properties, statement.isNotExists());
        return immediateVoidFuture();
    }
}
