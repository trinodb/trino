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

import com.google.common.collect.ImmutableList;
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
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeNotFoundException;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetColumnType;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.AMBIGUOUS_NAME;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
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
        if (redirectionAwareTableHandle.tableHandle().isEmpty()) {
            String exceptionMessage = format("Table '%s' does not exist", qualifiedObjectName);
            if (metadata.getMaterializedView(session, qualifiedObjectName).isPresent()) {
                exceptionMessage += ", but a materialized view with that name exists.";
            }
            else if (metadata.isView(session, qualifiedObjectName)) {
                exceptionMessage += ", but a view with that name exists.";
            }
            if (!statement.isTableExists()) {
                throw semanticException(TABLE_NOT_FOUND, statement, "%s", exceptionMessage);
            }
            return immediateVoidFuture();
        }

        accessControl.checkCanAlterColumn(session.toSecurityContext(), redirectionAwareTableHandle.redirectedTableName().orElse(qualifiedObjectName));

        TableHandle tableHandle = redirectionAwareTableHandle.tableHandle().get();
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        String columnName = statement.getColumnName().getParts().get(0).toLowerCase(ENGLISH);
        ColumnHandle column = columnHandles.get(columnName);
        if (column == null) {
            throw semanticException(COLUMN_NOT_FOUND, statement, "Column '%s' does not exist", statement.getColumnName());
        }

        Type type = getColumnType(statement);
        if (statement.getColumnName().getParts().size() == 1) {
            metadata.setColumnType(session, tableHandle, column, type);
        }
        else {
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle, column);
            List<String> fieldPath = statement.getColumnName().getParts();

            Type currentType = columnMetadata.getType();
            for (int i = 1; i < fieldPath.size(); i++) {
                String fieldName = fieldPath.get(i);
                List<RowType.Field> candidates = getCandidates(currentType, fieldName);

                if (candidates.isEmpty()) {
                    throw semanticException(COLUMN_NOT_FOUND, statement, "Field '%s' does not exist within %s", fieldName, currentType);
                }
                if (candidates.size() > 1) {
                    throw semanticException(AMBIGUOUS_NAME, statement, "Field path %s within %s is ambiguous", fieldPath, columnMetadata.getType());
                }
                currentType = getOnlyElement(candidates).getType();

                if (getOnlyElement(candidates).getName().isEmpty() && i == fieldPath.size() - 1) {
                    // field path ends up on 'element' unwrapping array
                    throw semanticException(COLUMN_NOT_FOUND, statement, "Field path %s does not point to row field", fieldPath);
                }
            }

            checkState(fieldPath.size() >= 2, "fieldPath size must be >= 2: %s", fieldPath);
            metadata.setFieldType(session, tableHandle, fieldPath, type);
        }

        return immediateVoidFuture();
    }

    private static List<RowType.Field> getCandidates(Type type, String fieldName)
    {
        if (type instanceof ArrayType arrayType) {
            if (!fieldName.equals("element")) {
                throw new TrinoException(NOT_SUPPORTED, "ARRAY type should be denoted by 'element' in the path; found '%s'".formatted(fieldName));
            }
            // return nameless Field to denote unwrapping of container
            return ImmutableList.of(RowType.field(arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            if (fieldName.equals("key")) {
                return ImmutableList.of(RowType.field(mapType.getKeyType()));
            }
            if (fieldName.equals("value")) {
                return ImmutableList.of(RowType.field(mapType.getValueType()));
            }
            throw new TrinoException(NOT_SUPPORTED, "MAP type should be denoted by 'key' or 'value' in the path; found '%s'".formatted(fieldName));
        }
        if (!(type instanceof RowType rowType)) {
            throw new TrinoException(NOT_SUPPORTED, "Unsupported type: " + type);
        }
        List<RowType.Field> candidates = rowType.getFields().stream()
                // case-insensitive match
                .filter(rowField -> rowField.getName().isPresent() && rowField.getName().get().equalsIgnoreCase(fieldName))
                .collect(toImmutableList());

        return candidates;
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
