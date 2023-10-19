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
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.RedirectionAwareTableHandle;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;

import java.util.List;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class DropColumnTask
        implements DataDefinitionTask<DropColumn>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DropColumnTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP COLUMN";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropColumn statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getTable());
        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, tableName);
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
            }
            return immediateVoidFuture();
        }
        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();

        // Use getParts method because the column name should be lowercase
        String column = statement.getField().getParts().get(0);

        QualifiedObjectName qualifiedTableName = redirectionAwareTableHandle.redirectedTableName().orElse(tableName);
        accessControl.checkCanDropColumn(session.toSecurityContext(), qualifiedTableName);

        ColumnHandle columnHandle = metadata.getColumnHandles(session, tableHandle).get(column);
        if (columnHandle == null) {
            if (!statement.isColumnExists()) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", column);
            }
            return immediateVoidFuture();
        }

        // Use getOriginalParts method because field names in row types are case-sensitive
        List<String> fieldPath = statement.getField().getOriginalParts().subList(1, statement.getField().getOriginalParts().size()).stream()
                .map(Identifier::getValue)
                .collect(toImmutableList());
        ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
        if (columnMetadata.isHidden()) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot drop hidden column");
        }
        if (fieldPath.isEmpty()) {
            if (metadata.getTableMetadata(session, tableHandle).getColumns().stream()
                    .filter(info -> !info.isHidden()).count() <= 1) {
                throw semanticException(NOT_SUPPORTED, statement, "Cannot drop the only column in a table");
            }
            metadata.dropColumn(session, tableHandle, qualifiedTableName.asCatalogSchemaTableName(), columnHandle);
        }
        else {
            RowType containingType = null;
            Type currentType = columnMetadata.getType();
            for (String fieldName : fieldPath) {
                if (currentType instanceof RowType rowType) {
                    List<RowType.Field> candidates = rowType.getFields().stream()
                            // case-sensitive match
                            .filter(rowField -> rowField.getName().isPresent() && rowField.getName().get().equals(fieldName))
                            .collect(toImmutableList());
                    if (candidates.size() > 1) {
                        throw semanticException(COLUMN_NOT_FOUND, statement, "Field path %s within %s is ambiguous", fieldPath, columnMetadata.getType());
                    }
                    if (candidates.size() == 1) {
                        RowType.Field rowField = getOnlyElement(candidates);
                        containingType = rowType;
                        currentType = rowField.getType();
                        continue;
                    }
                    if (statement.isColumnExists() && candidates.size() == 0) {
                        // TODO should we allow only the leaf not to exist, or any path component?
                        return immediateVoidFuture();
                    }
                }
                // TODO: Support array and map types
                throw semanticException(
                        NOT_SUPPORTED,
                        statement,
                        "Cannot resolve field '%s' within %s type when dropping %s in %s",
                        fieldName,
                        currentType,
                        fieldPath,
                        columnMetadata.getType());
            }

            verifyNotNull(containingType, "containingType is null");
            if (containingType.getFields().size() == 1) {
                throw semanticException(NOT_SUPPORTED, statement, "Cannot drop the only field in a row type");
            }
            metadata.dropField(session, tableHandle, columnHandle, fieldPath);
        }

        return immediateVoidFuture();
    }
}
