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
import io.trino.metadata.BranchPropertyManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AccessControl;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.SaveMode;
import io.trino.sql.PlannerContext;
import io.trino.sql.tree.CreateBranch;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.execution.ParameterExtractor.bindParameters;
import static io.trino.metadata.MetadataUtil.createQualifiedObjectName;
import static io.trino.metadata.MetadataUtil.getRequiredCatalogHandle;
import static io.trino.spi.StandardErrorCode.BRANCH_ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.BRANCH_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.tree.SaveMode.FAIL;
import static io.trino.sql.tree.SaveMode.REPLACE;
import static java.util.Objects.requireNonNull;

public class CreateBranchTask
        implements DataDefinitionTask<CreateBranch>
{
    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final AccessControl accessControl;
    private final BranchPropertyManager branchPropertyManager;

    @Inject
    public CreateBranchTask(PlannerContext plannerContext, Metadata metadata, AccessControl accessControl, BranchPropertyManager branchPropertyManager)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.branchPropertyManager = requireNonNull(branchPropertyManager, "branchPropertyManager is null");
    }

    @Override
    public String getName()
    {
        return "CREATE BRANCH";
    }

    @Override
    public ListenableFuture<Void> execute(CreateBranch statement, QueryStateMachine stateMachine, List<Expression> parameters, WarningCollector warningCollector)
    {
        Session session = stateMachine.getSession();

        QualifiedObjectName table = createQualifiedObjectName(session, statement, statement.getTableName());
        String branch = statement.getBranchName().getValue();
        Optional<String> fromBranch = statement.getFromBranch().map(Identifier::getValue);

        if (metadata.isMaterializedView(session, table)) {
            throw semanticException(NOT_SUPPORTED, statement, "Creating branch from materialized view is not supported");
        }
        if (metadata.isView(session, table)) {
            throw semanticException(NOT_SUPPORTED, statement, "Creating branch from view is not supported");
        }
        Optional<TableHandle> tableHandle = metadata.getRedirectionAwareTableHandle(session, table).tableHandle();
        if (tableHandle.isEmpty()) {
            throw semanticException(TABLE_NOT_FOUND, statement, "Table '%s' does not exist", table);
        }

        accessControl.checkCanCreateBranch(session.toSecurityContext(), table, branch);

        if (metadata.branchExists(session, table, branch) && statement.getSaveMode() != REPLACE) {
            if (statement.getSaveMode() == FAIL) {
                throw semanticException(BRANCH_ALREADY_EXISTS, statement, "Branch '%s' already exists", branch);
            }
            return immediateVoidFuture();
        }
        fromBranch.ifPresent(from -> {
            if (!metadata.branchExists(session, table, from)) {
                throw semanticException(BRANCH_NOT_FOUND, statement, "Branch '%s' does not exist", from);
            }
        });

        Map<NodeRef<Parameter>, Expression> parameterLookup = bindParameters(statement, parameters);
        CatalogHandle catalogHandle = getRequiredCatalogHandle(metadata, session, statement, table.catalogName());
        Map<String, Object> properties = branchPropertyManager.getProperties(
                table.catalogName(),
                catalogHandle,
                statement.getProperties(),
                session,
                plannerContext,
                accessControl,
                parameterLookup,
                true);

        metadata.createBranch(session, tableHandle.get(), branch, fromBranch, toConnectorSaveMode(statement.getSaveMode()), properties);

        return immediateVoidFuture();
    }

    private static SaveMode toConnectorSaveMode(io.trino.sql.tree.SaveMode saveMode)
    {
        return switch (saveMode) {
            case FAIL -> SaveMode.FAIL;
            case IGNORE -> SaveMode.IGNORE;
            case REPLACE -> SaveMode.REPLACE;
        };
    }
}
