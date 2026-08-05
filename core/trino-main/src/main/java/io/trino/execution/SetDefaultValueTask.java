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
import io.trino.connector.CatalogHandle;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetDefaultValue;

import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.connector.ConnectorCapabilities.DEFAULT_COLUMN_VALUE;
import static io.trino.sql.analyzer.ExpressionAnalyzer.analyzeDefaultColumnValue;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class SetDefaultValueTask
        implements DataDefinitionTask<SetDefaultValue>
{
    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public SetDefaultValueTask(PlannerContext plannerContext, AccessControl accessControl)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = plannerContext.getMetadata();
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET DEFAULT";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetDefaultValue statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        Map<NodeRef<Parameter>, Expression> parameterLookup = bindParameters(statement, parameters);
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTableName());
        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, tableName);

        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            String exceptionMessage = "Table '%s' does not exist".formatted(tableName);
            if (metadata.getMaterializedView(session, tableName).isPresent()) {
                exceptionMessage += ", but a materialized view with that name exists.";
            }
            else if (metadata.isView(session, tableName)) {
                exceptionMessage += ", but a view with that name exists.";
            }
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "%s", exceptionMessage);
            }
            return immediateVoidFuture();
        }
        accessControl.checkCanAlterColumn(session.toSecurityContext(), tableName);

        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();
        CatalogHandle catalogHandle = tableHandle.catalogHandle();
        QualifiedName field = statement.getColumnName();
        if (field.getOriginalParts().size() != 1) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot modify nested fields");
        }
        String columnName = field.getOriginalParts().getFirst().getValue();
        ColumnHandle columnHandle = metadata.getColumnHandles(session, tableHandle).get(columnName);

        if (columnHandle == null) {
            throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", columnName);
        }

        ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
        if (columnMetadata.isHidden()) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot modify hidden column");
        }

        if (!metadata.getConnectorCapabilities(session, catalogHandle).contains(DEFAULT_COLUMN_VALUE)) {
            throw semanticException(NOT_SUPPORTED, statement, "Catalog '%s' does not support default value for column name '%s'", catalogHandle, columnName);
        }
        Expression defaultValue = statement.getDefaultValue();
        analyzeDefaultColumnValue(session, plannerContext, accessControl, parameterLookup, warningCollector, columnMetadata.getType(), defaultValue);

        metadata.setDefaultValue(session, tableHandle, columnHandle, defaultValue.toString());
        return immediateVoidFuture();
    }
}
