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
package io.trino.server;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import io.trino.execution.AddColumnTask;
import io.trino.execution.CallTask;
import io.trino.execution.CommentTask;
import io.trino.execution.CommitTask;
import io.trino.execution.CreateMaterializedViewTask;
import io.trino.execution.CreateRoleTask;
import io.trino.execution.CreateSchemaTask;
import io.trino.execution.CreateTableTask;
import io.trino.execution.CreateViewTask;
import io.trino.execution.DataDefinitionExecution.DataDefinitionExecutionFactory;
import io.trino.execution.DataDefinitionTask;
import io.trino.execution.DeallocateTask;
import io.trino.execution.DropColumnTask;
import io.trino.execution.DropMaterializedViewTask;
import io.trino.execution.DropRoleTask;
import io.trino.execution.DropSchemaTask;
import io.trino.execution.DropTableTask;
import io.trino.execution.DropViewTask;
import io.trino.execution.GrantRolesTask;
import io.trino.execution.GrantTask;
import io.trino.execution.PrepareTask;
import io.trino.execution.QueryExecution.QueryExecutionFactory;
import io.trino.execution.RenameColumnTask;
import io.trino.execution.RenameSchemaTask;
import io.trino.execution.RenameTableTask;
import io.trino.execution.RenameViewTask;
import io.trino.execution.ResetSessionTask;
import io.trino.execution.RevokeRolesTask;
import io.trino.execution.RevokeTask;
import io.trino.execution.RollbackTask;
import io.trino.execution.SetPathTask;
import io.trino.execution.SetRoleTask;
import io.trino.execution.SetSchemaAuthorizationTask;
import io.trino.execution.SetSessionTask;
import io.trino.execution.SetTableAuthorizationTask;
import io.trino.execution.SetTimeZoneTask;
import io.trino.execution.SetViewAuthorizationTask;
import io.trino.execution.SqlQueryExecution.SqlQueryExecutionFactory;
import io.trino.execution.StartTransactionTask;
import io.trino.execution.UseTask;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSchemaAuthorization;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetTableAuthorization;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Use;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.trino.util.StatementUtils.getAllQueryTypes;

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
        bindDataDefinitionTask(binder, executionBinder, SetTimeZone.class, SetTimeZoneTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetRole.class, SetRoleTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSchemaAuthorization.class, SetSchemaAuthorizationTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetSession.class, SetSessionTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetTableAuthorization.class, SetTableAuthorizationTask.class);
        bindDataDefinitionTask(binder, executionBinder, SetViewAuthorization.class, SetViewAuthorizationTask.class);
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
