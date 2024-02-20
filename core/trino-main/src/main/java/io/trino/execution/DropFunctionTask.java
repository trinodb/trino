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
import io.trino.metadata.LanguageFunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.tree.DropFunction;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.CreateFunctionTask.defaultFunctionSchema;
import static io.trino.execution.CreateFunctionTask.qualifiedFunctionName;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static java.util.Objects.requireNonNull;

public class DropFunctionTask
        implements DataDefinitionTask<DropFunction>
{
    private final Optional<CatalogSchemaName> functionSchema;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final LanguageFunctionManager languageFunctionManager;

    @Inject
    public DropFunctionTask(
            SqlEnvironmentConfig sqlEnvironmentConfig,
            Metadata metadata,
            AccessControl accessControl,
            LanguageFunctionManager languageFunctionManager)
    {
        this.functionSchema = defaultFunctionSchema(sqlEnvironmentConfig);
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.languageFunctionManager = requireNonNull(languageFunctionManager, "languageFunctionManager is null");
    }

    @Override
    public String getName()
    {
        return "DROP FUNCTION";
    }

    @Override
    public ListenableFuture<Void> execute(DropFunction statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        QualifiedObjectName name = qualifiedFunctionName(functionSchema, statement, statement.getName());

        accessControl.checkCanDropFunction(session.toSecurityContext(), name);

        String signatureToken = languageFunctionManager.getSignatureToken(statement.getParameters());

        if (!metadata.languageFunctionExists(session, name, signatureToken)) {
            if (!statement.isExists()) {
                throw semanticException(NOT_FOUND, statement, "Function not found");
            }
            return immediateVoidFuture();
        }

        metadata.dropLanguageFunction(session, name, signatureToken);

        return immediateVoidFuture();
    }
}
