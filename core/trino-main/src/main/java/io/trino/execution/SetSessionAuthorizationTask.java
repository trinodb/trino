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
import io.trino.client.ClientCapabilities;
import io.trino.execution.warnings.WarningCollector;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.SetSessionAuthorization;
import io.trino.sql.tree.StringLiteral;
import io.trino.transaction.TransactionManager;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class SetSessionAuthorizationTask
        implements DataDefinitionTask<SetSessionAuthorization>
{
    private final AccessControl accessControl;
    private final TransactionManager transactionManager;

    @Inject
    public SetSessionAuthorizationTask(AccessControl accessControl, TransactionManager transactionManager)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public String getName()
    {
        return "SET SESSION AUTHORIZATION";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetSessionAuthorization statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();
        if (!session.getClientCapabilities().contains(ClientCapabilities.SESSION_AUTHORIZATION.toString())) {
            throw new TrinoException(NOT_SUPPORTED, "SET SESSION AUTHORIZATION not supported by client");
        }
        Identity originalIdentity = session.getOriginalIdentity();
        // Set authorization user in the middle of a transaction is disallowed by the SQL spec
        session.getTransactionId().ifPresent(transactionId -> {
            if (!transactionManager.getTransactionInfo(transactionId).isAutoCommitContext()) {
                throw new TrinoException(GENERIC_USER_ERROR, "Can't set authorization user in the middle of a transaction");
            }
        });

        String user;
        Expression userExpression = statement.getUser();
        if (userExpression instanceof Identifier identifier) {
            user = identifier.getValue();
        }
        else if (userExpression instanceof StringLiteral stringLiteral) {
            user = stringLiteral.getValue();
        }
        else {
            throw new IllegalArgumentException("Unsupported user expression: " + userExpression.getClass().getName());
        }
        checkState(user != null && !user.isEmpty(), "Authorization user cannot be null or empty");

        if (!originalIdentity.getUser().equals(user)) {
            accessControl.checkCanSetUser(originalIdentity.getPrincipal(), user);
            accessControl.checkCanImpersonateUser(originalIdentity, user);
        }
        stateMachine.setSetAuthorizationUser(user);
        SelectedRole selectedRole;
        Set<String> enabledRoles = originalIdentity.getEnabledRoles();
        if (enabledRoles.isEmpty()) {
            selectedRole = new SelectedRole(SelectedRole.Type.NONE, Optional.empty());
        }
        else if (enabledRoles.size() == 1) {
            selectedRole = new SelectedRole(SelectedRole.Type.ROLE, Optional.of(enabledRoles.iterator().next()));
        }
        else {
            selectedRole = new SelectedRole(SelectedRole.Type.ALL, Optional.empty());
        }
        stateMachine.addSetOriginalRoles(selectedRole);
        return immediateFuture(null);
    }
}
