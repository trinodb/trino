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
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetColumnType;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SetColumnTypeTask
        implements DataDefinitionTask<SetColumnType>
{
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final AccessControl accessControl;

    @Inject
    public SetColumnTypeTask(Metadata metadata, TypeManager typeManager, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET COLUMN TYPE";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetColumnType statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName qualifiedObjectName = createQualifiedObjectName(session, statement, statement.getTableName());
        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, qualifiedObjectName);
        if (redirectionAwareTableHandle.getTableHandle().isEmpty()) {
            String exceptionMessage = format("Table '%s' does not exist", qualifiedObjectName);
            if (metadata.getMaterializedView(session, qualifiedObjectName).isPresent()) {
                exceptionMessage += ", but a materialized view with that name exists.";
            }
            else if (metadata.getView(session, qualifiedObjectName).isPresent()) {
                exceptionMessage += ", but a view with that name exists.";
            }
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, exceptionMessage);
            }
            return immediateVoidFuture();
        }

        accessControl.checkCanAlterColumn(session.toSecurityContext(), redirectionAwareTableHandle.getRedirectedTableName().orElse(qualifiedObjectName));

        TableHandle tableHandle = redirectionAwareTableHandle.getTableHandle().get();
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle column = columnHandles.get(statement.getColumnName().getValue().toLowerCase(ENGLISH));
        if (column == null) {
            throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", statement.getColumnName());
        }

        metadata.setColumnType(session, tableHandle, column, getColumnType(statement));

        return immediateVoidFuture();
    }

    private Type getColumnType(SetColumnType statement)
    {
        Type type;
        try {
            type = typeManager.getType(toTypeSignature(statement.getType()));
        }
        catch (TypeNotFoundException e) {
            throw semanticException(TYPE_NOT_FOUND, statement.getType(), "Unknown type '%s' for column '%s'", statement.getType(), statement.getColumnName());
        }
        if (type.equals(UNKNOWN)) {
            throw semanticException(COLUMN_TYPE_UNKNOWN, statement.getType(), "Unknown type '%s' for column '%s'", statement.getType(), statement.getColumnName());
        }
        return type;
    }
}
