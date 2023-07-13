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
import io.trino.metadata.SessionPropertyManager;
import io.trino.security.AccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SetSession;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.metadata.SessionPropertyManager.evaluatePropertyValue;
import static io.trino.metadata.SessionPropertyManager.serializeSessionProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SetSessionTask
        implements DataDefinitionTask<SetSession>
{
    private final PlannerContext plannerContext;
    private final AccessControl accessControl;
    private final SessionPropertyManager sessionPropertyManager;

    @Inject
    public SetSessionTask(PlannerContext plannerContext, AccessControl accessControl, SessionPropertyManager sessionPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
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
        Session session = stateMachine.getSession();
        QualifiedName propertyName = statement.getName();
        List<String> parts = propertyName.getParts();
        if (parts.size() > 2) {
            throw semanticException(INVALID_SESSION_PROPERTY, statement, "Invalid session property '%s'", propertyName);
        }

        // validate the property name
        PropertyMetadata<?> propertyMetadata;
        if (parts.size() == 1) {
            accessControl.checkCanSetSystemSessionProperty(session.getIdentity(), parts.get(0));
            propertyMetadata = sessionPropertyManager.getSystemSessionPropertyMetadata(parts.get(0))
                    .orElseThrow(() -> semanticException(INVALID_SESSION_PROPERTY, statement, "Session property '%s' does not exist", statement.getName()));
        }
        else {
            CatalogHandle catalogHandle = getRequiredCatalogHandle(plannerContext.getMetadata(), stateMachine.getSession(), statement, parts.get(0));
            accessControl.checkCanSetCatalogSessionProperty(SecurityContext.of(session), parts.get(0), parts.get(1));
            propertyMetadata = sessionPropertyManager.getConnectorSessionPropertyMetadata(catalogHandle, parts.get(1))
                    .orElseThrow(() -> semanticException(INVALID_SESSION_PROPERTY, statement, "Session property '%s' does not exist", statement.getName()));
        }

        Type type = propertyMetadata.getSqlType();
        Object objectValue;

        try {
            objectValue = evaluatePropertyValue(statement.getValue(), type, session, plannerContext, accessControl, bindParameters(statement, parameters));
        }
        catch (TrinoException e) {
            throw new TrinoException(
                    INVALID_SESSION_PROPERTY,
                    format("Unable to set session property '%s' to '%s': %s", propertyName, statement.getValue(), e.getRawMessage()));
        }

        String value = serializeSessionProperty(type, objectValue);

        // verify the SQL value can be decoded by the property
        try {
            propertyMetadata.decode(objectValue);
        }
        catch (RuntimeException e) {
            throw semanticException(INVALID_SESSION_PROPERTY, statement, e.getMessage());
        }

        stateMachine.addSetSessionProperties(propertyName.toString(), value);

        return immediateVoidFuture();
    }
}
