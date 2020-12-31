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
import static io.prestosql.spi.StandardErrorCode.CATALOG_NOT_FOUND;
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
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource());
        Optional<ConnectorViewDefinition> viewDefinition = metadata.getView(session, viewName);
        if (viewDefinition.isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist", viewName);
        }

        QualifiedObjectName target = createQualifiedObjectName(session, statement, statement.getTarget());
        if (metadata.getCatalogHandle(session, target.getCatalogName()).isEmpty()) {
            throw semanticException(CATALOG_NOT_FOUND, statement, "Target catalog '%s' does not exist", target.getCatalogName());
        }
        if (metadata.getView(session, target).isPresent()) {
            throw semanticException(TABLE_ALREADY_EXISTS, statement, "Target view '%s' already exists", target);
        }
        if (!viewName.getCatalogName().equals(target.getCatalogName())) {
            throw semanticException(NOT_SUPPORTED, statement, "View rename across catalogs is not supported");
        }

        accessControl.checkCanRenameView(session.toSecurityContext(), viewName, target);

        metadata.renameView(session, viewName, target);

        return immediateFuture(null);
    }
}
