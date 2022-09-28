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
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SetViewAuthorization;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.checkRoleExists;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SetViewAuthorizationTask
        implements DataDefinitionTask<SetViewAuthorization>
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final boolean isAllowSetViewAuthorization;

    @Inject
    public SetViewAuthorizationTask(Metadata metadata, AccessControl accessControl, FeaturesConfig featuresConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.isAllowSetViewAuthorization = featuresConfig.isAllowSetViewAuthorization();
    }

    @Override
    public String getName()
    {
        return "SET VIEW AUTHORIZATION";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetViewAuthorization statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        QualifiedObjectName viewName = createQualifiedObjectName(session, statement, statement.getSource());
        getRequiredCatalogHandle(metadata, session, statement, viewName.getCatalogName());
        ViewDefinition view = metadata.getView(session, viewName)
                .orElseThrow(() -> semanticException(TABLE_NOT_FOUND, statement, "View '%s' does not exist", viewName));

        TrinoPrincipal principal = createPrincipal(statement.getPrincipal());
        checkRoleExists(session, statement, metadata, principal, Optional.of(viewName.getCatalogName()).filter(catalog -> metadata.isCatalogManagedSecurity(session, catalog)));

        if (!view.isRunAsInvoker() && !isAllowSetViewAuthorization) {
            throw new TrinoException(
                    NOT_SUPPORTED,
                    format(
                            "Cannot set authorization for view %s to %s: this feature is disabled",
                            viewName.getCatalogName() + '.' + viewName.getSchemaName() + '.' + viewName.getObjectName(), principal));
        }
        accessControl.checkCanSetViewAuthorization(session.toSecurityContext(), viewName, principal);

        metadata.setViewAuthorization(session, viewName.asCatalogSchemaTableName(), principal);

        return immediateVoidFuture();
    }
}
