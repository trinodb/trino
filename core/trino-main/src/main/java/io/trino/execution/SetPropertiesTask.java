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
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AccessControl;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetProperties;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.NodeUtils.mapFromProperties;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class SetPropertiesTask
        implements DataDefinitionTask<SetProperties>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final TablePropertyManager tablePropertyManager;

    @Inject
    public SetPropertiesTask(PlannerContext plannerContext, AccessControl accessControl, TablePropertyManager tablePropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.tablePropertyManager = requireNonNull(tablePropertyManager, "tablePropertyManager is null");
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
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());

        Map<String, Expression> sqlProperties = mapFromProperties(statement.getProperties());

        if (statement.getType() == SetProperties.Type.TABLE) {
            Map<String, Object> properties = tablePropertyManager.getProperties(
                    getRequiredCatalogHandle(plannerContext.getMetadata(), session, statement, tableName.getCatalogName()),
                    tableName.getCatalogName(),
                    sqlProperties,
                    session,
                    plannerContext,
                    accessControl,
                    parameterExtractor(statement, parameters),
                    false); // skip setting of default properties since they should not be stored explicitly
            setTableProperties(statement, tableName, session, properties);
        }
        else {
            throw semanticException(NOT_SUPPORTED, statement, "Unsupported target type: %s", statement.getType());
        }

        return immediateVoidFuture();
    }

    private void setTableProperties(SetProperties statement, QualifiedObjectName tableName, Session session, Map<String, Object> properties)
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

        accessControl.checkCanSetTableProperties(session.toSecurityContext(), tableName, properties);

        plannerContext.getMetadata().setTableProperties(session, tableHandle.get(), properties);
    }
}
