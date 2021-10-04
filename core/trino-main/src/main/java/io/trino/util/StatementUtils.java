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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
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
import io.trino.sql.tree.SetPath;
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
import io.trino.sql.tree.Update;
import io.trino.sql.tree.Use;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.resourcegroups.QueryType.ANALYZE;
import static io.trino.spi.resourcegroups.QueryType.DATA_DEFINITION;
import static io.trino.spi.resourcegroups.QueryType.DELETE;
import static io.trino.spi.resourcegroups.QueryType.DESCRIBE;
import static io.trino.spi.resourcegroups.QueryType.EXPLAIN;
import static io.trino.spi.resourcegroups.QueryType.INSERT;
import static io.trino.spi.resourcegroups.QueryType.SELECT;
import static io.trino.spi.resourcegroups.QueryType.UPDATE;

public final class StatementUtils
{
    private StatementUtils() {}

    private static final Map<Class<? extends Statement>, QueryType> STATEMENT_QUERY_TYPES = ImmutableMap.<Class<? extends Statement>, QueryType>builder()
            // SELECT
            .put(Query.class, SELECT)
            // EXPLAIN
            .put(Explain.class, EXPLAIN)
            // DESCRIBE
            .put(DescribeInput.class, DESCRIBE)
            .put(DescribeOutput.class, DESCRIBE)
            .put(ShowCatalogs.class, DESCRIBE)
            .put(ShowColumns.class, DESCRIBE)
            .put(ShowCreate.class, DESCRIBE)
            .put(ShowFunctions.class, DESCRIBE)
            .put(ShowGrants.class, DESCRIBE)
            .put(ShowRoleGrants.class, DESCRIBE)
            .put(ShowRoles.class, DESCRIBE)
            .put(ShowSchemas.class, DESCRIBE)
            .put(ShowSession.class, DESCRIBE)
            .put(ShowStats.class, DESCRIBE)
            .put(ShowTables.class, DESCRIBE)
            // DML
            .put(CreateTableAsSelect.class, INSERT)
            .put(RefreshMaterializedView.class, INSERT)
            .put(Insert.class, INSERT)
            .put(Update.class, UPDATE)
            .put(Delete.class, DELETE)
            .put(Analyze.class, ANALYZE)
            // DDL
            .put(AddColumn.class, DATA_DEFINITION)
            .put(Call.class, DATA_DEFINITION)
            .put(Comment.class, DATA_DEFINITION)
            .put(Commit.class, DATA_DEFINITION)
            .put(CreateMaterializedView.class, DATA_DEFINITION)
            .put(CreateRole.class, DATA_DEFINITION)
            .put(CreateSchema.class, DATA_DEFINITION)
            .put(CreateTable.class, DATA_DEFINITION)
            .put(CreateView.class, DATA_DEFINITION)
            .put(Deallocate.class, DATA_DEFINITION)
            .put(DropColumn.class, DATA_DEFINITION)
            .put(DropMaterializedView.class, DATA_DEFINITION)
            .put(DropRole.class, DATA_DEFINITION)
            .put(DropSchema.class, DATA_DEFINITION)
            .put(DropTable.class, DATA_DEFINITION)
            .put(DropView.class, DATA_DEFINITION)
            .put(Grant.class, DATA_DEFINITION)
            .put(GrantRoles.class, DATA_DEFINITION)
            .put(Prepare.class, DATA_DEFINITION)
            .put(RenameColumn.class, DATA_DEFINITION)
            .put(RenameMaterializedView.class, DATA_DEFINITION)
            .put(RenameSchema.class, DATA_DEFINITION)
            .put(RenameTable.class, DATA_DEFINITION)
            .put(RenameView.class, DATA_DEFINITION)
            .put(ResetSession.class, DATA_DEFINITION)
            .put(Revoke.class, DATA_DEFINITION)
            .put(RevokeRoles.class, DATA_DEFINITION)
            .put(Rollback.class, DATA_DEFINITION)
            .put(SetPath.class, DATA_DEFINITION)
            .put(SetRole.class, DATA_DEFINITION)
            .put(SetSchemaAuthorization.class, DATA_DEFINITION)
            .put(SetSession.class, DATA_DEFINITION)
            .put(SetTableAuthorization.class, DATA_DEFINITION)
            .put(SetTimeZone.class, DATA_DEFINITION)
            .put(SetViewAuthorization.class, DATA_DEFINITION)
            .put(StartTransaction.class, DATA_DEFINITION)
            .put(Use.class, DATA_DEFINITION)
            .build();

    public static Optional<QueryType> getQueryType(Statement statement)
    {
        if (statement instanceof ExplainAnalyze) {
            return getQueryType(((ExplainAnalyze) statement).getStatement());
        }
        return Optional.ofNullable(STATEMENT_QUERY_TYPES.get(statement.getClass()));
    }

    public static Set<Class<? extends Statement>> getNonDataDefinitionStatements()
    {
        // ExplainAnalyze is special because it has the type of the target query.
        // It is thus not in STATEMENT_QUERY_TYPES and must be added here.
        return Stream.concat(
                Stream.of(ExplainAnalyze.class),
                STATEMENT_QUERY_TYPES.entrySet().stream()
                        .filter(entry -> entry.getValue() != DATA_DEFINITION)
                        .map(Map.Entry::getKey))
                .collect(toImmutableSet());
    }

    public static boolean isDataDefinitionStatement(Class<? extends Statement> statement)
    {
        return STATEMENT_QUERY_TYPES.get(statement) == DATA_DEFINITION;
    }

    public static boolean isTransactionControlStatement(Statement statement)
    {
        return statement instanceof StartTransaction || statement instanceof Commit || statement instanceof Rollback;
    }
}
