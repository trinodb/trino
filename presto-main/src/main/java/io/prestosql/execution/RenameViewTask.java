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
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.RenameView;
import io.prestosql.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.metadata.MetadataUtil.redirectToNewCatalogIfNecessary;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.TABLE_ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.prestosql.sql.analyzer.SemanticExceptions.semanticException;

public class RenameViewTask
        implements DataDefinitionTask<RenameView>
{
    @Override
    public String getName()
    {
        return "RENAME VIEW";
    }

    @Override
    public ListenableFuture<?> execute(RenameView statement, TransactionManager transactionManager, Metadata metadata, AccessControl accessControl, QueryStateMachine stateMachine, List<Expression> parameters)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName originalViewName = createQualifiedObjectName(session, statement, statement.getSource());
        QualifiedObjectName redirectedViewName = redirectToNewCatalogIfNecessary(session, originalViewName, metadata);
        Optional<ConnectorViewDefinition> viewDefinition = metadata.getView(session, originalViewName);
        if (viewDefinition.isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist", originalViewName);
        }

        QualifiedObjectName target = createQualifiedObjectName(session, statement, statement.getTarget());
        if (!redirectedViewName.getCatalogName().equals(target.getCatalogName())) {
            if (!originalViewName.getCatalogName().equals(target.getCatalogName())) {
                throw semanticException(NOT_SUPPORTED, statement, "View rename across catalogs is not supported");
            }
            target = new QualifiedObjectName(redirectedViewName.getCatalogName(), target.getSchemaName(), target.getObjectName());
        }
        if (metadata.getView(session, target).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target view '%s' already exists", target);
        }

        accessControl.checkCanRenameView(session.toSecurityContext(), redirectedViewName, target);

        metadata.renameView(session, redirectedViewName, target);

        return immediateFuture(null);
    }
}
