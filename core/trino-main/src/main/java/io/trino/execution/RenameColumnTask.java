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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.RenameColumn;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class RenameColumnTask
        implements DataDefinitionTask<RenameColumn>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public RenameColumnTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "RENAME COLUMN";
    }

    @Override
    public ListenableFuture<Void> execute(
            RenameColumn statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName originalTableName = createQualifiedObjectName(session, statement, statement.getTable());
        RedirectionAwareTableHandle redirectionAwareTableHandle = metadata.getRedirectionAwareTableHandle(session, originalTableName);
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", originalTableName);
            }
            return immediateVoidFuture();
        }
        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();

        String source = statement.getSource().getParts().get(0).toLowerCase(ENGLISH);
        String target = statement.getTarget().getValue().toLowerCase(ENGLISH);

        QualifiedObjectName qualifiedTableName = redirectionAwareTableHandle.redirectedTableName().orElse(originalTableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnHandle columnHandle = columnHandles.get(source);
        if (columnHandle == null) {
            if (!statement.isColumnExists()) {
                throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", source);
            }
            return immediateVoidFuture();
        }
        if (metadata.getColumnMetadata(session, tableHandle, columnHandle).isHidden()) {
            throw semanticException(NOT_SUPPORTED, statement, "Cannot rename hidden column");
        }

        if (statement.getSource().getParts().size() == 1) {
            accessControl.checkCanRenameColumn(session.toSecurityContext(), qualifiedTableName);

            if (columnHandles.containsKey(target)) {
                throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", target);
            }

            metadata.renameColumn(session, tableHandle, qualifiedTableName.asCatalogSchemaTableName(), columnHandle, target);
        }
        else {
            accessControl.checkCanAlterColumn(session.toSecurityContext(), qualifiedTableName);

            List<String> fieldPath = statement.getSource().getParts();

            ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandle);
            Type currentType = columnMetadata.getType();
            for (int i = 1; i < fieldPath.size() - 1; i++) {
                String fieldName = fieldPath.get(i);
                List<RowType.Field> candidates = getCandidates(currentType, fieldName);

                if (candidates.isEmpty()) {
                    throw semanticException(COLUMN_NOT_FOUND, statement, "Field '%s' does not exist within %s", fieldName, currentType);
                }
                if (candidates.size() > 1) {
                    throw semanticException(AMBIGUOUS_NAME, statement, "Field path %s within %s is ambiguous", fieldPath, columnMetadata.getType());
                }
                currentType = getOnlyElement(candidates).getType();
            }

            String sourceFieldName = getLast(statement.getSource().getParts());
            List<RowType.Field> sourceCandidates = getCandidates(currentType, sourceFieldName);
            if (sourceCandidates.isEmpty()) {
                if (!statement.isColumnExists()) {
                    throw semanticException(COLUMN_NOT_FOUND, statement, "Field '%s' does not exist", source);
                }
                return immediateVoidFuture();
            }
            if (sourceCandidates.size() > 1) {
                throw semanticException(AMBIGUOUS_NAME, statement, "Field path %s within %s is ambiguous", fieldPath, columnMetadata.getType());
            }

            List<RowType.Field> targetCandidates = getCandidates(currentType, target);
            if (!targetCandidates.isEmpty()) {
                throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Field '%s' already exists", target);
            }

            metadata.renameField(session, tableHandle, fieldPath, target);
        }

        return immediateVoidFuture();
    }

    private static List<RowType.Field> getCandidates(Type type, String fieldName)
    {
        if (!(type instanceof RowType rowType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
        List<RowType.Field> candidates = rowType.getFields().stream()
                // case-insensitive match
                .filter(rowField -> rowField.getName().isPresent() && rowField.getName().get().equalsIgnoreCase(fieldName))
                .collect(toImmutableList());

        return candidates;
    }
}
