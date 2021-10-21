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

package io.trino.sql.parser;

import io.trino.sql.tree.Node;

import static java.lang.String.format;

public class AstRestrictedBuilder
        extends AstBuilder
{
    private final ParsingException restrictedNodeException;

    AstRestrictedBuilder(ParsingOptions parsingOptions)
    {
        super(parsingOptions);
        restrictedNodeException = new ParsingException(format("Unexpected CRUD operation. %s mode is enabled", parsingOptions.getSqlParserMode().toString()));
    }

    // ******************* restricted statements **********************

    @Override
    public Node visitUse(SqlBaseParser.UseContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCreateSchema(SqlBaseParser.CreateSchemaContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDropSchema(SqlBaseParser.DropSchemaContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRenameSchema(SqlBaseParser.RenameSchemaContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitSetSchemaAuthorization(SqlBaseParser.SetSchemaAuthorizationContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCreateTableAsSelect(SqlBaseParser.CreateTableAsSelectContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCreateMaterializedView(SqlBaseParser.CreateMaterializedViewContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRefreshMaterializedView(SqlBaseParser.RefreshMaterializedViewContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDropMaterializedView(SqlBaseParser.DropMaterializedViewContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDropView(SqlBaseParser.DropViewContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitInsertInto(SqlBaseParser.InsertIntoContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDelete(SqlBaseParser.DeleteContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitUpdate(SqlBaseParser.UpdateContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitUpdateAssignment(SqlBaseParser.UpdateAssignmentContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitMerge(SqlBaseParser.MergeContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitMergeInsert(SqlBaseParser.MergeInsertContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitMergeUpdate(SqlBaseParser.MergeUpdateContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitMergeDelete(SqlBaseParser.MergeDeleteContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRenameTable(SqlBaseParser.RenameTableContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCommentTable(SqlBaseParser.CommentTableContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCommentColumn(SqlBaseParser.CommentColumnContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRenameColumn(SqlBaseParser.RenameColumnContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitAnalyze(SqlBaseParser.AnalyzeContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitAddColumn(SqlBaseParser.AddColumnContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitSetTableAuthorization(SqlBaseParser.SetTableAuthorizationContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDropColumn(SqlBaseParser.DropColumnContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRenameView(SqlBaseParser.RenameViewContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitSetViewAuthorization(SqlBaseParser.SetViewAuthorizationContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitStartTransaction(SqlBaseParser.StartTransactionContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCommit(SqlBaseParser.CommitContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRollback(SqlBaseParser.RollbackContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitTransactionAccessMode(SqlBaseParser.TransactionAccessModeContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitIsolationLevel(SqlBaseParser.IsolationLevelContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCall(SqlBaseParser.CallContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDescribeOutput(SqlBaseParser.DescribeOutputContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDescribeInput(SqlBaseParser.DescribeInputContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitExplainAnalyze(SqlBaseParser.ExplainAnalyzeContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitExplainFormat(SqlBaseParser.ExplainFormatContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitExplainType(SqlBaseParser.ExplainTypeContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowSession(SqlBaseParser.ShowSessionContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitSetSession(SqlBaseParser.SetSessionContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitResetSession(SqlBaseParser.ResetSessionContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitCreateRole(SqlBaseParser.CreateRoleContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitDropRole(SqlBaseParser.DropRoleContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitGrantRoles(SqlBaseParser.GrantRolesContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRevokeRoles(SqlBaseParser.RevokeRolesContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitSetRole(SqlBaseParser.SetRoleContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitGrant(SqlBaseParser.GrantContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitRevoke(SqlBaseParser.RevokeContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowGrants(SqlBaseParser.ShowGrantsContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowRoles(SqlBaseParser.ShowRolesContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowRoleGrants(SqlBaseParser.ShowRoleGrantsContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitSetPath(SqlBaseParser.SetPathContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitSetTimeZone(SqlBaseParser.SetTimeZoneContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowStats(SqlBaseParser.ShowStatsContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowStatsForQuery(SqlBaseParser.ShowStatsForQueryContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowCreateSchema(SqlBaseParser.ShowCreateSchemaContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowCreateView(SqlBaseParser.ShowCreateViewContext context)
    {
        return restrictNode();
    }

    @Override
    public Node visitShowCreateMaterializedView(SqlBaseParser.ShowCreateMaterializedViewContext context)
    {
        return restrictNode();
    }

    public Node restrictNode() throws ParsingException
    {
        throw restrictedNodeException;
    }
}
