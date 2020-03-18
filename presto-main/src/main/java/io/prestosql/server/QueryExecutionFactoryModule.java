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
package io.prestosql.server;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.prestosql.execution.AddColumnTask;
import io.prestosql.execution.CallTask;
import io.prestosql.execution.CommentTask;
import io.prestosql.execution.CommitTask;
import io.prestosql.execution.CreateMaterializedViewTask;
import io.prestosql.execution.CreateRoleTask;
import io.prestosql.execution.CreateSchemaTask;
import io.prestosql.execution.CreateTableTask;
import io.prestosql.execution.CreateViewTask;
import io.prestosql.execution.DataDefinitionExecution.DataDefinitionExecutionFactory;
import io.prestosql.execution.DataDefinitionTask;
import io.prestosql.execution.DeallocateTask;
import io.prestosql.execution.DropColumnTask;
import io.prestosql.execution.DropMaterializedViewTask;
import io.prestosql.execution.DropRoleTask;
import io.prestosql.execution.DropSchemaTask;
import io.prestosql.execution.DropTableTask;
import io.prestosql.execution.DropViewTask;
import io.prestosql.execution.GrantRolesTask;
import io.prestosql.execution.GrantTask;
import io.prestosql.execution.PrepareTask;
import io.prestosql.execution.QueryExecution.QueryExecutionFactory;
import io.prestosql.execution.RenameColumnTask;
import io.prestosql.execution.RenameSchemaTask;
import io.prestosql.execution.RenameTableTask;
import io.prestosql.execution.RenameViewTask;
import io.prestosql.execution.ResetSessionTask;
import io.prestosql.execution.RevokeRolesTask;
import io.prestosql.execution.RevokeTask;
import io.prestosql.execution.RollbackTask;
import io.prestosql.execution.SetPathTask;
import io.prestosql.execution.SetRoleTask;
import io.prestosql.execution.SetSchemaAuthorizationTask;
import io.prestosql.execution.SetSessionTask;
import io.prestosql.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import io.prestosql.execution.StartTransactionTask;
import io.prestosql.execution.UseTask;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.sql.tree.AddColumn;
import io.prestosql.sql.tree.Call;
import io.prestosql.sql.tree.Comment;
import io.prestosql.sql.tree.Commit;
import io.prestosql.sql.tree.CreateMaterializedView;
import io.prestosql.sql.tree.CreateRole;
import io.prestosql.sql.tree.CreateSchema;
import io.prestosql.sql.tree.CreateTable;
import io.prestosql.sql.tree.CreateView;
import io.prestosql.sql.tree.Deallocate;
import io.prestosql.sql.tree.DropColumn;
import io.prestosql.sql.tree.DropMaterializedView;
import io.prestosql.sql.tree.DropRole;
import io.prestosql.sql.tree.DropSchema;
import io.prestosql.sql.tree.DropTable;
import io.prestosql.sql.tree.DropView;
import io.prestosql.sql.tree.Grant;
import io.prestosql.sql.tree.GrantRoles;
import io.prestosql.sql.tree.Prepare;
import io.prestosql.sql.tree.RenameColumn;
import io.prestosql.sql.tree.RenameSchema;
import io.prestosql.sql.tree.RenameTable;
import io.prestosql.sql.tree.RenameView;
import io.prestosql.sql.tree.ResetSession;
import io.prestosql.sql.tree.Revoke;
import io.prestosql.sql.tree.RevokeRoles;
import io.prestosql.sql.tree.Rollback;
import io.prestosql.sql.tree.SetPath;
import io.prestosql.sql.tree.SetRole;
import io.prestosql.sql.tree.SetSchemaAuthorization;
import io.prestosql.sql.tree.SetSession;
import io.prestosql.sql.tree.StartTransaction;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.Use;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.prestosql.util.StatementUtils.getAllQueryTypes;

public class QueryExecutionFactoryModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        var executionBinder = newMapBinder(binder, new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<QueryExecutionFactory<?>>() {});

        binder.bind(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON);
        getAllQueryTypes().entrySet().stream()
                .filter(entry -> entry.getValue() != QueryType.DATA_DEFINITION)
                .map(Map.Entry::getKey)
                .forEach(statement -> executionBinder.addBinding(statement).to(SqlQueryExecutionFactory.class).in(Scopes.SINGLETON));

        binder.bind(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
        bindDataDefinitionTask(binder, executionBinder, AddColumn.class, AddColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, Call.class, CallTask.class);
        bindDataDefinitionTask(binder, executionBinder, Comment.class, CommentTask.class);
        bindDataDefinitionTask(binder, executionBinder, Commit.class, CommitTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateRole.class, CreateRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateSchema.class, CreateSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateTable.class, CreateTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateView.class, CreateViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, Deallocate.class, DeallocateTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropColumn.class, DropColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropRole.class, DropRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropSchema.class, DropSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropTable.class, DropTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropView.class, DropViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, CreateMaterializedView.class, CreateMaterializedViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, DropMaterializedView.class, DropMaterializedViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, Grant.class, GrantTask.class);
        bindDataDefinitionTask(binder, executionBinder, GrantRoles.class, GrantRolesTask.class);
        bindDataDefinitionTask(binder, executionBinder, Prepare.class, PrepareTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameColumn.class, RenameColumnTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameSchema.class, RenameSchemaTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameTable.class, RenameTableTask.class);
        bindDataDefinitionTask(binder, executionBinder, RenameView.class, RenameViewTask.class);
        bindDataDefinitionTask(binder, executionBinder, ResetSession.class, ResetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, Revoke.class, RevokeTask.class);
        bindDataDefinitionTask(binder, executionBinder, RevokeRoles.class, RevokeRolesTask.class);
        bindDataDefinitionTask(binder, executionBinder, Rollback.class, RollbackTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetPath.class, SetPathTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetRole.class, SetRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSchemaAuthorization.class, SetSchemaAuthorizationTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSession.class, SetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, StartTransaction.class, StartTransactionTask.class);
        bindDataDefinitionTask(binder, executionBinder, Use.class, UseTask.class);
    }

    private static <T extends Statement> void bindDataDefinitionTask(
            Binder binder,
            MapBinder<Class<? extends Statement>, QueryExecutionFactory<?>> executionBinder,
            Class<T> statement,
            Class<? extends DataDefinitionTask<T>> task)
    {
        checkArgument(getAllQueryTypes().get(statement) == QueryType.DATA_DEFINITION);
        var taskBinder = newMapBinder(binder, new TypeLiteral<Class<? extends Statement>>() {}, new TypeLiteral<DataDefinitionTask<?>>() {});
        taskBinder.addBinding(statement).to(task).in(Scopes.SINGLETON);
        executionBinder.addBinding(statement).to(DataDefinitionExecutionFactory.class).in(Scopes.SINGLETON);
    }
}
