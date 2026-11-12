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
package io.trino.sql.tree;

import java.util.Optional;

public abstract sealed class Statement
        extends Node
        permits AddColumn,
                Analyze,
                Call,
                Comment,
                Commit,
                CreateBranch,
                CreateCatalog,
                CreateFunction,
                CreateMaterializedView,
                CreateRole,
                CreateSchema,
                CreateTable,
                CreateTableAsSelect,
                CreateView,
                Deallocate,
                Delete,
                Deny,
                DescribeInput,
                DescribeOutput,
                DropBranch,
                DropCatalog,
                DropColumn,
                DropDefaultValue,
                DropFunction,
                DropMaterializedView,
                DropNotNullConstraint,
                DropRole,
                DropSchema,
                DropTable,
                DropView,
                Execute,
                ExecuteImmediate,
                Explain,
                ExplainAnalyze,
                FastForwardBranch,
                Grant,
                GrantRoles,
                Insert,
                MaterializedViewExecute,
                Merge,
                Prepare,
                Query,
                RefreshMaterializedView,
                RefreshView,
                RenameColumn,
                RenameMaterializedView,
                RenameSchema,
                RenameTable,
                RenameView,
                ResetSession,
                ResetSessionAuthorization,
                Revoke,
                RevokeRoles,
                Rollback,
                SetAuthorizationStatement,
                SetColumnType,
                SetDefaultValue,
                SetPath,
                SetProperties,
                SetRole,
                SetSession,
                SetSessionAuthorization,
                SetTimeZone,
                ShowBranches,
                ShowCatalogs,
                ShowColumns,
                ShowCreate,
                ShowFunctions,
                ShowGrants,
                ShowRoleGrants,
                ShowRoles,
                ShowSchemas,
                ShowSession,
                ShowStats,
                ShowTables,
                StartTransaction,
                TableExecute,
                TruncateTable,
                Update,
                Use
{
    @Deprecated
    protected Statement(Optional<NodeLocation> location)
    {
        super(location);
    }

    protected Statement(NodeLocation location)
    {
        super(location);
    }
}
