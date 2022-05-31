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
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.Expression;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createCatalogSchemaName;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class DropSchemaTask
        implements DataDefinitionTask<DropSchema>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DropSchemaTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP SCHEMA";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropSchema statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        if (statement.isCascade()) {
            throw new TrinoException(NOT_SUPPORTED, "CASCADE is not yet supported for DROP SCHEMA");
        }

        Session session = stateMachine.getSession();
        CatalogSchemaName schema = createCatalogSchemaName(session, statement, Optional.of(statement.getSchemaName()));

        if (!metadata.schemaExists(session, schema)) {
            if (!statement.isExists()) {
                throw semanticException(SCHEMA_NOT_FOUND, statement, "Schema '%s' does not exist", schema);
            }
            return immediateVoidFuture();
        }

        if (!isSchemaEmpty(session, schema, metadata)) {
            throw semanticException(SCHEMA_NOT_EMPTY, statement, "Cannot drop non-empty schema '%s'", schema.getSchemaName());
        }

        accessControl.checkCanDropSchema(session.toSecurityContext(), schema);

        metadata.dropSchema(session, schema);

        return immediateVoidFuture();
    }

    private static boolean isSchemaEmpty(Session session, CatalogSchemaName schema, Metadata metadata)
    {
        QualifiedTablePrefix tablePrefix = new QualifiedTablePrefix(schema.getCatalogName(), schema.getSchemaName());

        // This is a best effort check that doesn't provide any guarantees against concurrent DDL operations
        return metadata.listTables(session, tablePrefix).isEmpty();
    }
}
