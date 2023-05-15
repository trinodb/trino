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
import io.trino.security.AccessControl;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.Expression;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class DropMaterializedViewTask
        implements DataDefinitionTask<DropMaterializedView>
{
    private final Metadata metadata;
    private final AccessControl accessControl;

    @Inject
    public DropMaterializedViewTask(Metadata metadata, AccessControl accessControl)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP MATERIALIZED VIEW";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropMaterializedView statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName name = createQualifiedObjectName(session, statement, statement.getName());

        if (!metadata.isMaterializedView(session, name)) {
            if (metadata.isView(session, name)) {
                throw semanticException(
                        GENERIC_USER_ERROR,
                        statement,
                        "Materialized view '%s' does not exist, but a view with that name exists. Did you mean DROP VIEW %s?", name, name);
            }
            if (metadata.getTableHandle(session, name).isPresent()) {
                throw semanticException(
                        GENERIC_USER_ERROR,
                        statement,
                        "Materialized view '%s' does not exist, but a table with that name exists. Did you mean DROP TABLE %s?", name, name);
            }
            if (!statement.isExists()) {
                throw semanticException(GENERIC_USER_ERROR, statement, "Materialized view '%s' does not exist", name);
            }
            return immediateVoidFuture();
        }

        accessControl.checkCanDropMaterializedView(session.toSecurityContext(), name);

        metadata.dropMaterializedView(session, name);

        return immediateVoidFuture();
    }
}
