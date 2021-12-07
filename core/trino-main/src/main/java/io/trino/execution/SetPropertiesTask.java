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
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.MaterializedViewPropertyManager;
import io.trino.metadata.Properties;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetProperties;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.tree.SetProperties.Type.MATERIALIZED_VIEW;
import static io.trino.sql.tree.SetProperties.Type.TABLE;
import static java.lang.String.format;
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

        if (statement.getType() == TABLE) {
            Properties properties = tablePropertyManager.getOnlySpecifiedProperties(
                    getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, objectName.getCatalogName()),
                    objectName.getCatalogName(),
                    statement.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    parameterExtractor(statement, parameters));
            setTableProperties(statement, objectName, session, properties);
        }
        else if (statement.getType() == MATERIALIZED_VIEW) {
            Properties properties = materializedViewPropertyManager.getOnlySpecifiedProperties(
                    getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, objectName.getCatalogName()),
                    objectName.getCatalogName(),
                    statement.getProperties(),
                    session,
                    plannerContext,
                    accessControl,
                    parameterExtractor(statement, parameters));
            setMaterializedViewProperties(statement, objectName, session, properties);
        }
        else {
            throw semanticException(NOT_SUPPORTED, statement, "Unsupported target type: %s", statement.getType());
        }

        return immediateVoidFuture();
    }

    private void setTableProperties(SetProperties statement, QualifiedObjectName tableName, Session session, Properties properties)
    {
        if (plannerContext.getMetadata().isMaterializedView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot set properties to a materialized view in ALTER TABLE");
        }

        if (plannerContext.getMetadata().isView(session, tableName)) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot set properties to a view in ALTER TABLE");
        }

        Optional<TableHandle> tableHandle = plannerContext.getMetadata().getTableHandle(session, tableName);
        if (tableHandle.isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table does not exist: %s", tableName);
        }

        accessControl.checkCanSetTableProperties(session.toSecurityContext(), tableName, properties.getNonNullProperties(), properties.getNullPropertyNames());

        plannerContext.getMetadata().setTableProperties(session, tableHandle.get(), properties.getNonNullProperties(), properties.getNullPropertyNames());
    }

    private void setMaterializedViewProperties(
            SetProperties statement,
            QualifiedObjectName materializedViewName,
            Session session,
            Properties properties)
    {
        if (plannerContext.getMetadata().getMaterializedView(session, materializedViewName).isEmpty()) {
            String exceptionMessage = format("Materialized View '%s' does not exist", materializedViewName);
            if (plannerContext.getMetadata().getView(session, materializedViewName).isPresent()) {
                exceptionMessage += ", but a view with that name exists.";
            }
            else if (plannerContext.getMetadata().getTableHandle(session, materializedViewName).isPresent()) {
                exceptionMessage += ", but a table with that name exists. Did you mean ALTER TABLE " + materializedViewName + " SET PROPERTIES ...?";
            }
            throw semanticException(TABLE_NOT_FOUND, statement, exceptionMessage);
        }
        accessControl.checkCanSetMaterializedViewProperties(session.toSecurityContext(), materializedViewName, properties.getNonNullProperties(), properties.getNullPropertyNames());
        plannerContext.getMetadata().setMaterializedViewProperties(session, materializedViewName, properties.getNonNullProperties(), properties.getNullPropertyNames());
    }
}
