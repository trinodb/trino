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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
import io.trino.spi.connector.CatalogHandle;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ResetSession;

import javax.inject.Inject;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class ResetSessionTask
        implements DataDefinitionTask<ResetSession>
{
    private final Metadata metadata;
    private final SessionPropertyManager sessionPropertyManager;

    @Inject
    public ResetSessionTask(Metadata metadata, SessionPropertyManager sessionPropertyManager)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "RESET SESSION";
    }

    @Override
    public ListenableFuture<Void> execute(
            ResetSession statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        List<String> parts = statement.getName().getParts();
        if (parts.size() > 2) {
            throw semanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", statement.getName());
        }

        // validate the property name
        if (parts.size() == 1) {
            if (sessionPropertyManager.getSystemSessionPropertyMetadata(parts.get(0)).isEmpty()) {
                throw semanticException(INVALID_SESSION_PROPERTY, statement, "Session property '%s' does not exist", statement.getName());
            }
        }
        else {
            CatalogHandle catalogHandle = getRequiredCatalogHandle(metadata, stateMachine.getSession(), statement, parts.get(0));
            if (sessionPropertyManager.getConnectorSessionPropertyMetadata(catalogHandle, parts.get(1)).isEmpty()) {
                throw semanticException(INVALID_SESSION_PROPERTY, statement, "Session property '%s' does not exist", statement.getName());
            }
        }

        stateMachine.addResetSessionProperties(statement.getName().toString());

        return immediateVoidFuture();
    }
}
