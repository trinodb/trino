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
package io.prestosql.execution;

import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.Session;
import io.prestosql.connector.CatalogName;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.ColumnDefinition;
import io.prestosql.sql.tree.Expression;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.StandardErrorCode.COLUMN_ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.COLUMN_TYPE_UNKNOWN;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static io.prestosql.spi.connector.ConnectorCapabilities.NOT_NULL_COLUMN_CONSTRAINT;
import static io.prestosql.sql.NodeUtils.mapFromProperties;
import static io.prestosql.sql.ParameterUtils.parameterExtractor;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;
import static io.prestosql.sql.analyzer.TypeSignatureTranslator.toTypeSignature;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.util.Locale.ENGLISH;

public class AddColumnTask
        implements DataDefinitionTask<AddColumn>
{
    @Override
    public String getName()
    {
        return "ADD COLUMN";
    }

    @Override
    public ListenableFuture<?> execute(AddColumn statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName tableName = createQualifiedObjectName(session, statement, statement.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        if (!tableHandle.isPresent()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", tableName);
        }

        CatalogName catalogName = metadata.getCatalogHandle(session, tableName.getCatalogName())
                .orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog does not exist: " + tableName.getCatalogName()));

        accessControl.checkCanAddColumns(session.toSecurityContext(), tableName);

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.get());

        ColumnDefinition element = statement.getColumn();
        Type type;
        try {
            type = metadata.getType(toTypeSignature(element.getType()));
        }
        catch (TypeNotFoundException e) {
            throw semanticException(TYPE_NOT_FOUND, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (type.equals(UNKNOWN)) {
            throw semanticException(COLUMN_TYPE_UNKNOWN, element, "Unknown type '%s' for column '%s'", element.getType(), element.getName());
        }
        if (columnHandles.containsKey(element.getName().getValue().toLowerCase(ENGLISH))) {
            throw semanticException(COLUMN_ALREADY_EXISTS, statement, "Column '%s' already exists", element.getName());
        }
        if (!element.isNullable() && !metadata.getConnectorCapabilities(session, catalogName).contains(NOT_NULL_COLUMN_CONSTRAINT)) {
            throw semanticException(NOT_SUPPORTED, element, "Catalog '%s' does not support NOT NULL for column '%s'", catalogName.getCatalogName(), element.getName());
        }

        Map<String, Expression> sqlProperties = mapFromProperties(element.getProperties());
        Map<String, Object> columnProperties = metadata.getColumnPropertyManager().getProperties(
                catalogName,
                tableName.getCatalogName(),
                sqlProperties,
                session,
                metadata,
                parameterExtractor(statement, parameters));

        ColumnMetadata column = new ColumnMetadata(
                element.getName().getValue(),
                type,
                element.isNullable(), element.getComment().orElse(null),
                null,
                false,
                columnProperties);

        metadata.addColumn(session, tableHandle.get(), column);

        return immediateFuture(null);
    }
}
