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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetProperties;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.tree.SetProperties.Type.MATERIALIZED_VIEW;
import static io.trino.sql.tree.SetProperties.Type.TABLE;
import static java.util.Objects.requireNonNull;

public class SetPropertiesTask
        implements DataDefinitionTask<SetProperties>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final TablePropertyManager tablePropertyManager;
    private final MaterializedViewPropertyManager materializedViewPropertyManager;

    @Inject
    public SetPropertiesTask(PlannerContext plannerContext, AccessControl accessControl, TablePropertyManager tablePropertyManager, MaterializedViewPropertyManager materializedViewPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
        this.materializedViewPropertyManager = requireNonNull(materializedViewPropertyManager, "materializedViewPropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "SET PROPERTIES";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetProperties statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName objectName = createQualifiedObjectName(session, statement, statement.getName());

        String catalogName = objectName.getCatalogName();
        if (statement.getType() == TABLE) {
            Map<String, Optional<Object>> properties = tablePropertyManager.getNullableProperties(
                    catalogName,
                    getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, catalogName),
                    statement.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    bindParameters(statement, parameters),
                    false);
            setTableProperties(statement, objectName, session, properties);
        }
        else if (statement.getType() == MATERIALIZED_VIEW) {
            Map<String, Optional<Object>> properties = materializedViewPropertyManager.getNullableProperties(
                    catalogName,
                    getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, catalogName),
                    statement.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    bindParameters(statement, parameters),
                    false);
            setMaterializedViewProperties(statement, objectName, session, properties);
        }
        else {
            throw semanticException(NOT_SUPPORTED, statement, "Unsupported target type: %s", statement.getType());
        }

        return immediateVoidFuture();
    }

    private void setTableProperties(SetProperties statement, QualifiedObjectName tableName, Session session, Map<String, Optional<Object>> properties)
    {
        if (plannerContext.getMetadata().isMaterializedView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot set properties to a materialized view in ALTER TABLE");
        }

        if (plannerContext.getMetadata().isView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot set properties to a view in ALTER TABLE");
        }

        TableHandle tableHandle = plannerContext.getMetadata().getTableHandle(session, tableName)
                .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: %s", tableName));

        accessControl.checkCanSetTableProperties(session.toSecurityContext(), tableName, properties);
        plannerContext.getMetadata().setTableProperties(session, tableHandle, properties);
    }

    private void setMaterializedViewProperties(
            SetProperties statement,
            QualifiedObjectName materializedViewName,
            Session session,
            Map<String, Optional<Object>> properties)
    {
        if (plannerContext.getMetadata().getMaterializedView(session, materializedViewName).isEmpty()) {
            String additionalInformation;
            if (plannerContext.getMetadata().getView(session, materializedViewName).isPresent()) {
                additionalInformation = ", but a view with that name exists.";
            }
            else if (plannerContext.getMetadata().getTableHandle(session, materializedViewName).isPresent()) {
                additionalInformation = ", but a table with that name exists. Did you mean ALTER TABLE " + materializedViewName + " SET PROPERTIES ...?";
            }
            else {
                additionalInformation = "";
            }
            throw semanticException(TABLE_NOT_FOUND, statement, "Materialized View '%s' does not exist%s", materializedViewName, additionalInformation);
        }
        accessControl.checkCanSetMaterializedViewProperties(session.toSecurityContext(), materializedViewName, properties);
        plannerContext.getMetadata().setMaterializedViewProperties(session, materializedViewName, properties);
    }
}
