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
import io.trino.connector.CatalogName;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Catalog.SecurityManagement;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewDefinition;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.transaction.TransactionManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.metadata.MetadataUtil.checkRoleExists;
import static io.trino.metadata.MetadataUtil.createPrincipal;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.metadata.MetadataUtil.processRoleCommandCatalog;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.spi.security.AccessDeniedException.denySetViewAuthorization;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.sql.ParameterUtils.parameterExtractor;
import static io.trino.sql.ParsingUtil.createParsingOptions;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.tree.CreateView.Security.DEFINER;
import static java.util.Objects.requireNonNull;

public class SetViewAuthorizationTask
        implements DataDefinitionTask<SetViewAuthorization>
{
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final SqlParser sqlParser;
    private final TransactionManager transactionManager;
    private final AnalyzerFactory analyzerFactory;
    private final boolean isAllowSetViewAuthorization;

    @Inject
    public SetViewAuthorizationTask(
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            TransactionManager transactionManager,
            AnalyzerFactory analyzerFactory,
            FeaturesConfig featuresConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.analyzerFactory = requireNonNull(analyzerFactory, "analyzerFactory is null");
        this.isAllowSetViewAuthorization = requireNonNull(featuresConfig, "featuresConfig is null").isAllowSetViewAuthorization();
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
            // With SECURITY DEFINER the privileges to access data depend on the owner,
            // so we can't allow changing the owner if it allows the user to gain more privileges.
            switch (principal.getType()) {
                case USER:
                    checkCanSetAuthorizationToUser(viewName, principal);
                    break;

                case ROLE:
                    checkCanSetAuthorizationToRole(session, metadata, accessControl, statement, viewName, principal);
                    break;
            }

            checkCanAccessViewEntities(session, stateMachine, parameters, viewName, view, principal);
            checkTargetPrincipalCanCreateView(session, transactionManager, accessControl, viewName, principal);
        }

        accessControl.checkCanSetViewAuthorization(session.toSecurityContext(), viewName, principal);

        metadata.setViewAuthorization(session, viewName.asCatalogSchemaTableName(), principal);

        return immediateVoidFuture();
    }

    private void checkCanSetAuthorizationToUser(QualifiedObjectName viewName, TrinoPrincipal principal)
    {
        // TODO: implement a separate permission to transfer ownership to another user
        denySetViewAuthorization(viewName.toString(), principal);
    }

    private void checkCanSetAuthorizationToRole(Session session, Metadata metadata, AccessControl accessControl, SetViewAuthorization statement, QualifiedObjectName viewName, TrinoPrincipal principal)
    {
        // The logic for ROLEs is that the user should be able to transfer ownership to any role they can assume,
        // as this does not give them any additional privileges they can't already obtain:
        Optional<String> catalog = processRoleCommandCatalog(metadata, session, statement, Optional.of(viewName.getCatalogName()));
        if (catalog.isPresent()) {
            try {
                accessControl.checkCanSetCatalogRole(SecurityContext.of(session), principal.getName(), catalog.get());
            }
            catch (AccessDeniedException e) {
                wrapAccessDeniedException(viewName, principal, e);
            }
        }
        else {
            Set<RoleGrant> roleGrants = metadata.listApplicableRoles(session, new TrinoPrincipal(USER, session.getUser()), Optional.empty());
            if (roleGrants.stream().map(RoleGrant::getRoleName).noneMatch(principal.getName()::equals)) {
                denySetViewAuthorization(viewName.toString(), principal, "Not a member of role " + principal.getName());
            }
        }
    }

    private void checkCanAccessViewEntities(Session session, QueryStateMachine stateMachine, List<Expression> parameters, QualifiedObjectName viewName, ViewDefinition view, TrinoPrincipal targetPrincipal)
    {
        CreateView statement = new CreateView(
                QualifiedName.of(viewName.getCatalogName(), viewName.getSchemaName(), viewName.getObjectName()),
                (Query) sqlParser.createStatement(view.getOriginalSql(), createParsingOptions(session)),
                false,
                view.getComment(),
                Optional.of(DEFINER));
        try {
            analyzerFactory.createAnalyzer(session, parameters, parameterExtractor(statement, parameters), stateMachine.getWarningCollector())
                    .analyze(statement);
        }
        catch (AccessDeniedException e) {
            wrapAccessDeniedException(viewName, targetPrincipal, e);
        }
    }

    private void checkTargetPrincipalCanCreateView(Session session, TransactionManager transactionManager, AccessControl accessControl, QualifiedObjectName viewName, TrinoPrincipal targetPrincipal)
    {
        SecurityContext targetPrincipalContext = new SecurityContext(session.getRequiredTransactionId(), getPrincipalIdentity(session, transactionManager, viewName, targetPrincipal), session.getQueryId());
        try {
            accessControl.checkCanCreateView(targetPrincipalContext, viewName);
        }
        catch (AccessDeniedException e) {
            wrapAccessDeniedException(viewName, targetPrincipal, e);
        }
    }

    private Identity getPrincipalIdentity(Session session, TransactionManager transactionManager, QualifiedObjectName viewName, TrinoPrincipal principal)
    {
        switch (principal.getType()) {
            case USER:
                return Identity.ofUser(principal.getName());
            case ROLE:
                // The operation will change the owner to current view's owner + role, but we check for the current
                // user + role, because the logic is that we should allow it if the current user would be able to do it
                // by creating the view having assumed the role.
                Identity.Builder identityBuilder = Identity.forUser(session.getUser());
                SecurityManagement securityManagement = transactionManager
                        .getCatalogMetadata(session.getRequiredTransactionId(), new CatalogName(viewName.getCatalogName()))
                        .getSecurityManagement();
                switch (securityManagement) {
                    case CONNECTOR:
                        identityBuilder = identityBuilder.withConnectorRole(viewName.getCatalogName(), new SelectedRole(SelectedRole.Type.ROLE, Optional.of(principal.getName())));
                        break;
                    case SYSTEM:
                        identityBuilder = identityBuilder.withEnabledRoles(Set.of(principal.getName()));
                        break;
                }
                return identityBuilder.build();
        }
        throw new IllegalArgumentException("Unknown principal type: " + principal.getType());
    }

    private static void wrapAccessDeniedException(QualifiedObjectName objectName, TrinoPrincipal principal, AccessDeniedException e)
    {
        String prefix = AccessDeniedException.PREFIX;
        verify(e.getMessage().startsWith(prefix));
        String msg = e.getMessage().substring(prefix.length());
        denySetViewAuthorization(objectName.toString(), principal, msg);
    }
}
