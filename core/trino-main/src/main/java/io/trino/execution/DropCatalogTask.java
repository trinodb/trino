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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogManager;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.Expression;

import java.util.List;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class DropCatalogTask
        implements DataDefinitionTask<DropCatalog>
{
    private final CatalogManager catalogManager;
    private final AccessControl accessControl;

    @Inject
    public DropCatalogTask(CatalogManager catalogManager, AccessControl accessControl)
    {
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
    }

    @Override
    public String getName()
    {
        return "DROP CATALOG";
    }

    @Override
    public ListenableFuture<Void> execute(
            DropCatalog statement,
            QueryStateMachine stateMachine,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        if (statement.isCascade()) {
            throw new TrinoException(NOT_SUPPORTED, "CASCADE is not yet supported for DROP SCHEMA");
        }

        accessControl.checkCanDropCatalog(stateMachine.getSession().toSecurityContext(), statement.getCatalogName().toString());
        catalogManager.dropCatalog(statement.getCatalogName().toString(), statement.isExists());
        return immediateVoidFuture();
    }
}
