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
package io.trino.util;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.trino.execution.AddColumnTask;
import io.trino.execution.CallTask;
import io.trino.execution.CommentTask;
import io.trino.execution.CommitTask;
import io.trino.execution.CreateCatalogTask;
import io.trino.execution.CreateMaterializedViewTask;
import io.trino.execution.CreateRoleTask;
import io.trino.execution.CreateSchemaTask;
import io.trino.execution.CreateTableTask;
import io.trino.execution.CreateViewTask;
import io.trino.execution.DataDefinitionTask;
import io.trino.execution.DeallocateTask;
import io.trino.execution.DenyTask;
import io.trino.execution.DropCatalogTask;
import io.trino.execution.DropColumnTask;
import io.trino.execution.DropMaterializedViewTask;
import io.trino.execution.DropRoleTask;
import io.trino.execution.DropSchemaTask;
import io.trino.execution.DropTableTask;
import io.trino.execution.DropViewTask;
import io.trino.execution.GrantRolesTask;
import io.trino.execution.GrantTask;
import io.trino.execution.PrepareTask;
import io.trino.execution.RenameColumnTask;
import io.trino.execution.RenameMaterializedViewTask;
import io.trino.execution.RenameSchemaTask;
import io.trino.execution.RenameTableTask;
import io.trino.execution.RenameViewTask;
import io.trino.execution.ResetSessionTask;
import io.trino.execution.RevokeRolesTask;
import io.trino.execution.RevokeTask;
import io.trino.execution.RollbackTask;
import io.trino.execution.SetColumnTypeTask;
import io.trino.execution.SetPathTask;
import io.trino.execution.SetPropertiesTask;
import io.trino.execution.SetRoleTask;
import io.trino.execution.SetSchemaAuthorizationTask;
import io.trino.execution.SetSessionTask;
import io.trino.execution.SetTableAuthorizationTask;
import io.trino.execution.SetTimeZoneTask;
import io.trino.execution.SetViewAuthorizationTask;
import io.trino.execution.StartTransactionTask;
import io.trino.execution.TruncateTableTask;
import io.trino.execution.UseTask;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.SetColumnType;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetProperties;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSchemaAuthorization;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetTableAuthorization;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.SetViewAuthorization;
import io.trino.sql.tree.ShowCatalogs;
import io.trino.sql.tree.ShowColumns;
import io.trino.sql.tree.ShowCreate;
import io.trino.sql.tree.ShowFunctions;
import io.trino.sql.tree.ShowGrants;
import io.trino.sql.tree.ShowRoleGrants;
import io.trino.sql.tree.ShowRoles;
import io.trino.sql.tree.ShowSchemas;
import io.trino.sql.tree.ShowSession;
import io.trino.sql.tree.ShowStats;
import io.trino.sql.tree.ShowTables;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.TruncateTable;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.Use;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.resourcegroups.QueryType.ALTER_TABLE_EXECUTE;
import static io.trino.spi.resourcegroups.QueryType.ANALYZE;
import static io.trino.spi.resourcegroups.QueryType.DATA_DEFINITION;
import static io.trino.spi.resourcegroups.QueryType.DELETE;
import static io.trino.spi.resourcegroups.QueryType.DESCRIBE;
import static io.trino.spi.resourcegroups.QueryType.EXPLAIN;
import static io.trino.spi.resourcegroups.QueryType.INSERT;
import static io.trino.spi.resourcegroups.QueryType.MERGE;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static io.trino.spi.resourcegroups.QueryType.UPDATE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public final class StatementUtils
{
    private StatementUtils() {}

    private static final Map<Class<? extends Statement>, StatementTypeInfo<? extends Statement>> STATEMENT_QUERY_TYPES = ImmutableList.<StatementTypeInfo<?>>builder()
            // SELECT
            .add(basicStatement(Query.class, SELECT))
            // EXPLAIN
            .add(basicStatement(Explain.class, EXPLAIN))
            // DESCRIBE
            .add(basicStatement(DescribeInput.class, DESCRIBE))
            .add(basicStatement(DescribeOutput.class, DESCRIBE))
            .add(basicStatement(ShowCatalogs.class, DESCRIBE))
            .add(basicStatement(ShowColumns.class, DESCRIBE))
            .add(basicStatement(ShowCreate.class, DESCRIBE))
            .add(basicStatement(ShowFunctions.class, DESCRIBE))
            .add(basicStatement(ShowGrants.class, DESCRIBE))
            .add(basicStatement(ShowRoleGrants.class, DESCRIBE))
            .add(basicStatement(ShowRoles.class, DESCRIBE))
            .add(basicStatement(ShowSchemas.class, DESCRIBE))
            .add(basicStatement(ShowSession.class, DESCRIBE))
            .add(basicStatement(ShowStats.class, DESCRIBE))
            .add(basicStatement(ShowTables.class, DESCRIBE))
            // Table Procedure
            .add(basicStatement(TableExecute.class, ALTER_TABLE_EXECUTE))
            // DML
            .add(basicStatement(CreateTableAsSelect.class, INSERT))
            .add(basicStatement(RefreshMaterializedView.class, INSERT))
            .add(basicStatement(Insert.class, INSERT))
            .add(basicStatement(Update.class, UPDATE))
            .add(basicStatement(Delete.class, DELETE))
            .add(basicStatement(Merge.class, MERGE))
            .add(basicStatement(Analyze.class, ANALYZE))
            // DDL
            .add(dataDefinitionStatement(AddColumn.class, AddColumnTask.class))
            .add(dataDefinitionStatement(Call.class, CallTask.class))
            .add(dataDefinitionStatement(Comment.class, CommentTask.class))
            .add(dataDefinitionStatement(Commit.class, CommitTask.class))
            .add(dataDefinitionStatement(CreateMaterializedView.class, CreateMaterializedViewTask.class))
            .add(dataDefinitionStatement(CreateCatalog.class, CreateCatalogTask.class))
            .add(dataDefinitionStatement(CreateRole.class, CreateRoleTask.class))
            .add(dataDefinitionStatement(CreateSchema.class, CreateSchemaTask.class))
            .add(dataDefinitionStatement(CreateTable.class, CreateTableTask.class))
            .add(dataDefinitionStatement(CreateView.class, CreateViewTask.class))
            .add(dataDefinitionStatement(Deallocate.class, DeallocateTask.class))
            .add(dataDefinitionStatement(Deny.class, DenyTask.class))
            .add(dataDefinitionStatement(DropCatalog.class, DropCatalogTask.class))
            .add(dataDefinitionStatement(DropColumn.class, DropColumnTask.class))
            .add(dataDefinitionStatement(DropMaterializedView.class, DropMaterializedViewTask.class))
            .add(dataDefinitionStatement(DropRole.class, DropRoleTask.class))
            .add(dataDefinitionStatement(DropSchema.class, DropSchemaTask.class))
            .add(dataDefinitionStatement(DropTable.class, DropTableTask.class))
            .add(dataDefinitionStatement(DropView.class, DropViewTask.class))
            .add(dataDefinitionStatement(TruncateTable.class, TruncateTableTask.class))
            .add(dataDefinitionStatement(Grant.class, GrantTask.class))
            .add(dataDefinitionStatement(GrantRoles.class, GrantRolesTask.class))
            .add(dataDefinitionStatement(Prepare.class, PrepareTask.class))
            .add(dataDefinitionStatement(RenameColumn.class, RenameColumnTask.class))
            .add(dataDefinitionStatement(RenameMaterializedView.class, RenameMaterializedViewTask.class))
            .add(dataDefinitionStatement(RenameSchema.class, RenameSchemaTask.class))
            .add(dataDefinitionStatement(RenameTable.class, RenameTableTask.class))
            .add(dataDefinitionStatement(RenameView.class, RenameViewTask.class))
            .add(dataDefinitionStatement(ResetSession.class, ResetSessionTask.class))
            .add(dataDefinitionStatement(Revoke.class, RevokeTask.class))
            .add(dataDefinitionStatement(RevokeRoles.class, RevokeRolesTask.class))
            .add(dataDefinitionStatement(Rollback.class, RollbackTask.class))
            .add(dataDefinitionStatement(SetColumnType.class, SetColumnTypeTask.class))
            .add(dataDefinitionStatement(SetPath.class, SetPathTask.class))
            .add(dataDefinitionStatement(SetRole.class, SetRoleTask.class))
            .add(dataDefinitionStatement(SetSchemaAuthorization.class, SetSchemaAuthorizationTask.class))
            .add(dataDefinitionStatement(SetSession.class, SetSessionTask.class))
            .add(dataDefinitionStatement(SetProperties.class, SetPropertiesTask.class))
            .add(dataDefinitionStatement(SetTableAuthorization.class, SetTableAuthorizationTask.class))
            .add(dataDefinitionStatement(SetTimeZone.class, SetTimeZoneTask.class))
            .add(dataDefinitionStatement(SetViewAuthorization.class, SetViewAuthorizationTask.class))
            .add(dataDefinitionStatement(StartTransaction.class, StartTransactionTask.class))
            .add(dataDefinitionStatement(Use.class, UseTask.class))
            .build().stream()
            .collect(toImmutableMap(StatementTypeInfo::getStatementType, identity()));

    public static Optional<QueryType> getQueryType(Statement statement)
    {
        if (statement instanceof ExplainAnalyze) {
            return getQueryType(((ExplainAnalyze) statement).getStatement());
        }
        return Optional.ofNullable(STATEMENT_QUERY_TYPES.get(statement.getClass()))
                .map(StatementTypeInfo::getQueryType);
    }

    public static Set<Class<? extends Statement>> getNonDataDefinitionStatements()
    {
        // ExplainAnalyze is special because it has the type of the target query.
        // It is thus not in STATEMENT_QUERY_TYPES and must be added here.
        return Stream.concat(
                Stream.of(ExplainAnalyze.class),
                STATEMENT_QUERY_TYPES.entrySet().stream()
                        .filter(entry -> entry.getValue().getQueryType() != DATA_DEFINITION)
                        .map(Map.Entry::getKey))
                .collect(toImmutableSet());
    }

    public static boolean isDataDefinitionStatement(Class<? extends Statement> statement)
    {
        StatementTypeInfo<? extends Statement> info = STATEMENT_QUERY_TYPES.get(statement);
        return info != null && info.getQueryType() == DATA_DEFINITION;
    }

    public static boolean isTransactionControlStatement(Statement statement)
    {
        return statement instanceof StartTransaction || statement instanceof Commit || statement instanceof Rollback;
    }

    private static <T extends Statement> StatementTypeInfo<T> basicStatement(Class<T> statementType, QueryType queryType)
    {
        return new StatementTypeInfo<>(statementType, queryType, Optional.empty());
    }

    private static <T extends Statement> StatementTypeInfo<T> dataDefinitionStatement(Class<T> statementType, Class<? extends DataDefinitionTask<T>> taskType)
    {
        requireNonNull(taskType, "taskType is null");
        verifyTaskInterfaceType(statementType, taskType, DataDefinitionTask.class);
        return new StatementTypeInfo<>(statementType, DATA_DEFINITION, Optional.of(taskType));
    }

    private static <T extends Statement> void verifyTaskInterfaceType(Class<T> statementType, Class<?> taskType, Class<?> expectedInterfaceType)
    {
        for (Type genericInterface : taskType.getGenericInterfaces()) {
            if (genericInterface instanceof ParameterizedType parameterizedInterface) {
                if (parameterizedInterface.getRawType().equals(expectedInterfaceType)) {
                    Type actualStatementType = parameterizedInterface.getActualTypeArguments()[0];
                    checkArgument(actualStatementType.equals(statementType), "Expected %s statement type to be %s", statementType.getSimpleName(), taskType.getSimpleName());
                    return;
                }
            }
        }
        throw new VerifyException(format("%s does not implement %s", taskType.getSimpleName(), DataDefinitionTask.class.getName()));
    }

    private static class StatementTypeInfo<T extends Statement>
    {
        private final Class<T> statementType;
        private final QueryType queryType;
        private final Optional<Class<? extends DataDefinitionTask<T>>> taskType;

        private StatementTypeInfo(Class<T> statementType,
                QueryType queryType,
                Optional<Class<? extends DataDefinitionTask<T>>> taskType)
        {
            this.statementType = requireNonNull(statementType, "statementType is null");
            this.queryType = requireNonNull(queryType, "queryType is null");
            this.taskType = requireNonNull(taskType, "taskType is null");
            if (queryType == DATA_DEFINITION) {
                checkArgument(taskType.isPresent(), "taskType is required for " + DATA_DEFINITION);
            }
            else {
                checkArgument(taskType.isEmpty(), "taskType is not allowed for " + queryType);
            }
        }

        public Class<T> getStatementType()
        {
            return statementType;
        }

        public QueryType getQueryType()
        {
            return queryType;
        }

        public Class<? extends DataDefinitionTask<T>> getTaskType()
        {
            return taskType.orElseThrow(() -> new IllegalStateException(queryType + " does not have a task type"));
        }
    }
}
