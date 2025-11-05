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
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetSession;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static java.util.Objects.requireNonNull;

public class SetSessionTask
        implements DataDefinitionTask<SetSession>
{
    private final SessionPropertyEvaluator sessionEvaluator;
    private final AccessControl accessControl;

    @Inject
    public SetSessionTask(SessionPropertyEvaluator sessionPropertyEvaluator, AccessControl accessControl)
    {
        this.sessionEvaluator = requireNonNull(sessionPropertyEvaluator, "sessionEvaluator is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "SET SESSION";
    }

    @Override
    public ListenableFuture<Void> execute(
            SetSession statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        QualifiedName propertyName = statement.getName();
        List<String> parts = propertyName.getParts();
        Session session = stateMachine.getSession();

        if (parts.size() == 1) {
            accessControl.checkCanSetSystemSessionProperty(session.getIdentity(), session.getQueryId(), parts.getFirst());
        }
        else if (parts.size() == 2) {
            accessControl.checkCanSetCatalogSessionProperty(SecurityContext.of(session), parts.getFirst(), parts.getLast());
        }
        else {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Invalid session property '%s'".formatted(propertyName));
        }

        stateMachine.addSetSessionProperties(
                statement.getName().toString(),
                sessionEvaluator.evaluate(stateMachine.getSession(), statement.getName(), statement.getValue(), bindParameters(statement, parameters)));

        return immediateVoidFuture();
    }
}
