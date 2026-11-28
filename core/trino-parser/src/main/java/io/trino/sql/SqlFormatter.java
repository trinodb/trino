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
package io.trino.sql;

import com.google.common.base.Joiner;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.ArrayWildcardSubscript;
import io.trino.sql.tree.AssignmentStatement;
import io.trino.sql.tree.AtLocal;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.CaseStatement;
import io.trino.sql.tree.CaseStatementWhenClause;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.ColumnPosition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.CommentCharacteristic;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.CompoundStatement;
import io.trino.sql.tree.ControlStatement;
import io.trino.sql.tree.Corresponding;
import io.trino.sql.tree.CreateBranch;
import io.trino.sql.tree.CreateCatalog;
import io.trino.sql.tree.CreateFunction;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentDate;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentTimestamp;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DeterministicCharacteristic;
import io.trino.sql.tree.DropBranch;
import io.trino.sql.tree.DropCatalog;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropDefaultValue;
import io.trino.sql.tree.DropFunction;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropNotNullConstraint;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.ElseClause;
import io.trino.sql.tree.ElseIfClause;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExecuteImmediate;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FastForwardBranch;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.FieldReference;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionSpecification;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantObject;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GrantorSpecification;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.IfStatement;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IntervalValueExpression;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.IterateStatement;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.JsonArray;
import io.trino.sql.tree.JsonConstructor;
import io.trino.sql.tree.JsonExists;
import io.trino.sql.tree.JsonObject;
import io.trino.sql.tree.JsonQuery;
import io.trino.sql.tree.JsonSerialize;
import io.trino.sql.tree.JsonTable;
import io.trino.sql.tree.JsonTableColumnDefinition;
import io.trino.sql.tree.JsonTableDefaultPlan;
import io.trino.sql.tree.JsonValue;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LanguageCharacteristic;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LeaveStatement;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LocalTime;
import io.trino.sql.tree.LocalTimestamp;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LoopStatement;
import io.trino.sql.tree.MaterializedViewExecute;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeCase;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.MethodCall;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Nearest;
import io.trino.sql.tree.NestedColumns;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullInputCharacteristic;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.OrdinalityColumn;
import io.trino.sql.tree.Overlay;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.ParameterDeclaration;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.Pivot;
import io.trino.sql.tree.PivotAggregation;
import io.trino.sql.tree.PivotValueGroup;
import io.trino.sql.tree.PlanLeaf;
import io.trino.sql.tree.PlanParentChild;
import io.trino.sql.tree.PlanSiblings;
import io.trino.sql.tree.Predicated;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.PropertiesCharacteristic;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryColumn;
import io.trino.sql.tree.QueryPeriod;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.RefreshView;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.RepeatStatement;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.ResetSessionAuthorization;
import io.trino.sql.tree.ReturnStatement;
import io.trino.sql.tree.ReturnsClause;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.RoutineCharacteristic;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.SecurityCharacteristic;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SessionProperty;
import io.trino.sql.tree.SetAuthorizationStatement;
import io.trino.sql.tree.SetColumnType;
import io.trino.sql.tree.SetDefaultValue;
import io.trino.sql.tree.SetPath;
import io.trino.sql.tree.SetProperties;
import io.trino.sql.tree.SetRole;
import io.trino.sql.tree.SetSession;
import io.trino.sql.tree.SetSessionAuthorization;
import io.trino.sql.tree.SetTimeZone;
import io.trino.sql.tree.ShowBranches;
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
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.StaticMethodCall;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.TableFunctionArgument;
import io.trino.sql.tree.TableFunctionDescriptorArgument;
import io.trino.sql.tree.TableFunctionInvocation;
import io.trino.sql.tree.TableFunctionTableArgument;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.TransactionMode;
import io.trino.sql.tree.Trim;
import io.trino.sql.tree.TruncateTable;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.UniquePredicate;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.Use;
import io.trino.sql.tree.ValueColumn;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.VariableDeclaration;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.WhileStatement;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WithQuery;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.ExpressionFormatter.formatGroupBy;
import static io.trino.sql.ExpressionFormatter.formatJsonPathInvocation;
import static io.trino.sql.ExpressionFormatter.formatOrderBy;
import static io.trino.sql.ExpressionFormatter.formatSkipTo;
import static io.trino.sql.ExpressionFormatter.formatStringLiteral;
import static io.trino.sql.ExpressionFormatter.formatWindowSpecification;
import static io.trino.sql.RowPatternFormatter.formatPattern;
import static io.trino.sql.tree.SaveMode.IGNORE;
import static io.trino.sql.tree.SaveMode.REPLACE;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class SqlFormatter
{
    private static final String INDENT = "   ";

    private SqlFormatter() {}

    public static String formatSql(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    private static String formatName(Identifier identifier)
    {
        return ExpressionFormatter.formatExpression(identifier);
    }

    public static String formatName(QualifiedName name)
    {
        return name.getOriginalParts().stream()
                .map(SqlFormatter::formatName)
                .collect(joining("."));
    }

    private static String formatExpression(Expression expression)
    {
        return ExpressionFormatter.formatExpression(expression);
    }

    private static class Formatter
    {
        public void process(Node node)
        {
            process(node, null);
        }

        public void process(Node root, Integer context)
        {
            switch (root) {
                case AddColumn node -> visitAddColumn(node);
                case AliasedRelation node -> visitAliasedRelation(node, context);
                case AllColumns node -> visitAllColumns(node);
                case AllRows node -> visitExpression(node, context);
                case Analyze node -> visitAnalyze(node);
                case ArithmeticBinaryExpression node -> visitExpression(node, context);
                case ArithmeticUnaryExpression node -> visitExpression(node, context);
                case Array node -> visitExpression(node, context);
                case ArrayWildcardSubscript node -> visitExpression(node, context);
                case AssignmentStatement node -> visitAssignmentStatement(node, context);
                case AtLocal node -> visitExpression(node, context);
                case AtTimeZone node -> visitExpression(node, context);
                case Call node -> visitCall(node, context);
                case CallArgument node -> visitCallArgument(node);
                case CaseStatement node -> visitCaseStatement(node, context);
                case CaseStatementWhenClause node -> visitCaseStatementWhenClause(node, context);
                case Cast node -> visitExpression(node, context);
                case CoalesceExpression node -> visitExpression(node, context);
                case Comment node -> visitComment(node);
                case CommentCharacteristic node -> visitCommentCharacteristic(node, context);
                case Commit _ -> builder.append("COMMIT");
                case CompoundStatement node -> visitCompoundStatement(node, context);
                case CreateBranch node -> visitCreateBranch(node);
                case CreateCatalog node -> visitCreateCatalog(node);
                case CreateFunction node -> visitCreateFunction(node, context);
                case CreateMaterializedView node -> visitCreateMaterializedView(node, context);
                case CreateRole node -> visitCreateRole(node);
                case CreateSchema node -> visitCreateSchema(node);
                case CreateTable node -> visitCreateTable(node, context);
                case CreateTableAsSelect node -> visitCreateTableAsSelect(node, context);
                case CreateView node -> visitCreateView(node, context);
                case CurrentCatalog node -> visitExpression(node, context);
                case CurrentDate node -> visitExpression(node, context);
                case CurrentPath node -> visitExpression(node, context);
                case CurrentSchema node -> visitExpression(node, context);
                case CurrentTime node -> visitExpression(node, context);
                case CurrentTimestamp node -> visitExpression(node, context);
                case CurrentUser node -> visitExpression(node, context);
                case DataType node -> visitExpression(node, context);
                case Deallocate node -> visitDeallocate(node, context);
                case Delete node -> visitDelete(node);
                case Deny node -> visitDeny(node);
                case DereferenceExpression node -> visitExpression(node, context);
                case DescribeInput node -> visitDescribeInput(node, context);
                case DescribeOutput node -> visitDescribeOutput(node, context);
                case DeterministicCharacteristic node -> append(context, (node.isDeterministic() ? "" : "NOT ") + "DETERMINISTIC");
                case DropBranch node -> visitDropBranch(node);
                case DropCatalog node -> visitDropCatalog(node);
                case DropColumn node -> visitDropColumn(node);
                case DropDefaultValue node -> visitDropDefaultValue(node);
                case DropFunction node -> visitDropFunction(node, context);
                case DropMaterializedView node -> visitDropMaterializedView(node);
                case DropNotNullConstraint node -> visitDropNotNullConstraint(node);
                case DropRole node -> visitDropRole(node);
                case DropSchema node -> visitDropSchema(node);
                case DropTable node -> visitDropTable(node);
                case DropView node -> visitDropView(node);
                case ElseClause node -> visitElseClause(node, context);
                case ElseIfClause node -> visitElseIfClause(node, context);
                case Except node -> visitExcept(node, context);
                case Execute node -> visitExecute(node, context);
                case ExecuteImmediate node -> visitExecuteImmediate(node, context);
                case ExistsPredicate node -> visitExpression(node, context);
                case Explain node -> visitExplain(node, context);
                case ExplainAnalyze node -> visitExplainAnalyze(node, context);
                case Extract node -> visitExpression(node, context);
                case FastForwardBranch node -> visitFastForwardBranch(node);
                case FetchFirst node -> visitFetchFirst(node, context);
                case FieldReference node -> visitExpression(node, context);
                case Format node -> visitExpression(node, context);
                case FunctionCall node -> visitExpression(node, context);
                case FunctionSpecification node -> visitFunctionSpecification(node, context);
                case Grant node -> visitGrant(node);
                case GrantRoles node -> visitGrantRoles(node);
                case GroupingOperation node -> visitExpression(node, context);
                case Identifier node -> visitExpression(node, context);
                case IfExpression node -> visitExpression(node, context);
                case IfStatement node -> visitIfStatement(node, context);
                case InListExpression node -> visitExpression(node, context);
                case Insert node -> visitInsert(node, context);
                case Intersect node -> visitIntersect(node, context);
                case IntervalValueExpression node -> visitExpression(node, context);
                case Isolation node -> builder.append("ISOLATION LEVEL ").append(node.getLevel().getText());
                case IterateStatement node -> visitIterateStatement(node, context);
                case Join node -> visitJoin(node, context);
                case JsonArray node -> visitExpression(node, context);
                case JsonConstructor node -> visitExpression(node, context);
                case JsonExists node -> visitExpression(node, context);
                case JsonObject node -> visitExpression(node, context);
                case JsonQuery node -> visitExpression(node, context);
                case JsonSerialize node -> visitExpression(node, context);
                case JsonTable node -> visitJsonTable(node, context);
                case JsonTableDefaultPlan node -> visitJsonTableDefaultPlan(node);
                case JsonValue node -> visitExpression(node, context);
                case LambdaArgumentDeclaration node -> visitExpression(node, context);
                case LambdaExpression node -> visitExpression(node, context);
                case LanguageCharacteristic node -> visitLanguageCharacteristic(node, context);
                case Lateral node -> visitLateral(node, context);
                case LeaveStatement node -> visitLeaveStatement(node, context);
                case Limit node -> visitLimit(node, context);
                case Literal node -> visitExpression(node, context);
                case LocalTime node -> visitExpression(node, context);
                case LocalTimestamp node -> visitExpression(node, context);
                case LogicalExpression node -> visitExpression(node, context);
                case LoopStatement node -> visitLoopStatement(node, context);
                case MaterializedViewExecute node -> visitMaterializedViewExecute(node, context);
                case Merge node -> visitMerge(node, context);
                case MergeDelete node -> visitMergeDelete(node, context);
                case MergeInsert node -> visitMergeInsert(node, context);
                case MergeUpdate node -> visitMergeUpdate(node, context);
                case MethodCall node -> visitExpression(node, context);
                case Nearest node -> visitNearest(node, context);
                case NestedColumns node -> visitNestedColumns(node, context);
                case NotExpression node -> visitExpression(node, context);
                case NullIfExpression node -> visitExpression(node, context);
                case NullInputCharacteristic node -> visitNullInputCharacteristic(node, context);
                case Offset node -> visitOffset(node, context);
                case OrderBy node -> visitOrderBy(node, context);
                case OrdinalityColumn node -> append(context, formatName(node.getName()) + " FOR ORDINALITY");
                case Overlay node -> visitExpression(node, context);
                case Parameter node -> visitExpression(node, context);
                case ParameterDeclaration node -> visitParameterDeclaration(node);
                case PatternRecognitionRelation node -> visitPatternRecognitionRelation(node, context);
                case Pivot node -> visitPivot(node, context);
                case PlanLeaf node -> builder.append(formatName(node.getName()));
                case PlanParentChild node -> visitPlanParentChild(node);
                case PlanSiblings node -> visitPlanSiblings(node);
                case Predicated node -> visitExpression(node, context);
                case Prepare node -> visitPrepare(node, context);
                case PropertiesCharacteristic node -> visitPropertiesCharacteristic(node, context);
                case Query node -> visitQuery(node, context);
                case QueryColumn node -> visitQueryColumn(node, context);
                case QueryPeriod node -> visitQueryPeriod(node);
                case QuerySpecification node -> visitQuerySpecification(node, context);
                case RefreshMaterializedView node -> visitRefreshMaterializedView(node);
                case RefreshView node -> visitRefreshView(node);
                case RenameColumn node -> visitRenameColumn(node);
                case RenameMaterializedView node -> visitRenameMaterializedView(node);
                case RenameSchema node -> visitRenameSchema(node);
                case RenameTable node -> visitRenameTable(node);
                case RenameView node -> visitRenameView(node);
                case RepeatStatement node -> visitRepeatStatement(node, context);
                case ResetSession node -> visitResetSession(node);
                case ResetSessionAuthorization _ -> builder.append("RESET SESSION AUTHORIZATION");
                case ReturnStatement node -> visitReturnStatement(node, context);
                case ReturnsClause node -> visitReturnClause(node, context);
                case Revoke node -> visitRevoke(node);
                case RevokeRoles node -> visitRevokeRoles(node);
                case Rollback _ -> builder.append("ROLLBACK");
                case Row node -> visitRow(node, context);
                case Row.Field node -> visitRowField(node);
                case RowPattern node -> visitRowPattern(node, context);
                case SampledRelation node -> visitSampledRelation(node, context);
                case SearchedCaseExpression node -> visitExpression(node, context);
                case SecurityCharacteristic node -> visitSecurityCharacteristic(node, context);
                case Select node -> visitSelect(node, context);
                case SessionProperty node -> visitSessionProperty(node, context);
                case SetAuthorizationStatement node -> visitSetAuthorization(node);
                case SetColumnType node -> visitSetColumnType(node);
                case SetDefaultValue node -> visitSetDefaultValue(node);
                case SetPath node -> visitSetPath(node);
                case SetProperties node -> visitSetProperties(node);
                case SetRole node -> visitSetRole(node);
                case SetSession node -> visitSetSession(node);
                case SetSessionAuthorization node -> visitSetSessionAuthorization(node);
                case SetTimeZone node -> visitSetTimeZone(node);
                case ShowBranches node -> visitShowBranches(node);
                case ShowCatalogs node -> visitShowCatalogs(node);
                case ShowColumns node -> visitShowColumns(node);
                case ShowCreate node -> visitShowCreate(node);
                case ShowFunctions node -> visitShowFunctions(node);
                case ShowGrants node -> visitShowGrants(node);
                case ShowRoleGrants node -> visitShowRoleGrants(node);
                case ShowRoles node -> visitShowRoles(node);
                case ShowSchemas node -> visitShowSchemas(node);
                case ShowSession node -> visitShowSession(node);
                case ShowStats node -> visitShowStats(node);
                case ShowTables node -> visitShowTables(node);
                case SimpleCaseExpression node -> visitExpression(node, context);
                case SingleColumn node -> visitSingleColumn(node);
                case StartTransaction node -> visitStartTransaction(node, context);
                case StaticMethodCall node -> visitExpression(node, context);
                case SubqueryExpression node -> visitExpression(node, context);
                case SubscriptExpression node -> visitExpression(node, context);
                case Table node -> visitTable(node);
                case TableExecute node -> visitTableExecute(node, context);
                case TableFunctionDescriptorArgument node -> visitDescriptorArgument(node);
                case TableFunctionInvocation node -> visitTableFunctionInvocation(node, context);
                case TableFunctionTableArgument node -> visitTableArgument(node, context);
                case TableSubquery node -> visitTableSubquery(node, context);
                case TransactionAccessMode node -> builder.append(node.isReadOnly() ? "READ ONLY" : "READ WRITE");
                case Trim node -> visitExpression(node, context);
                case TruncateTable node -> visitTruncateTable(node);
                case TryExpression node -> visitExpression(node, context);
                case Union node -> visitUnion(node, context);
                case UniquePredicate node -> visitExpression(node, context);
                case Unnest node -> visitUnnest(node);
                case Update node -> visitUpdate(node, context);
                case Use node -> visitUse(node);
                case ValueColumn node -> visitValueColumn(node, context);
                case Values node -> visitValues(node, context);
                case VariableDeclaration node -> visitVariableDeclaration(node, context);
                case WhenClause node -> visitExpression(node, context);
                case WhileStatement node -> visitWhileStatement(node, context);
                case WindowOperation node -> visitExpression(node, context);
                default -> throw new UnsupportedOperationException("not yet implemented: " + root);
            }
        }

        private static class SqlBuilder
        {
            private final StringBuilder builder;

            public SqlBuilder(StringBuilder builder)
            {
                this.builder = requireNonNull(builder, "builder is null");
            }

            @CanIgnoreReturnValue
            public SqlBuilder append(CharSequence value)
            {
                builder.append(value);
                return this;
            }

            @CanIgnoreReturnValue
            public SqlBuilder append(char c)
            {
                builder.append(c);
                return this;
            }
        }

        private final SqlBuilder builder;

        public Formatter(StringBuilder builder)
        {
            this.builder = new SqlBuilder(builder);
        }

        private void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node));
        }

        private void visitRowPattern(RowPattern node, Integer indent)
        {
            checkArgument(indent == 0, "visitRowPattern should only be called at root");
            builder.append(formatPattern(node));
        }

        private void visitUnnest(Unnest node)
        {
            builder.append("UNNEST(")
                    .append(node.getExpressions().stream()
                            .map(SqlFormatter::formatExpression)
                            .collect(joining(", ")))
                    .append(")");
            if (node.isWithOrdinality()) {
                builder.append(" WITH ORDINALITY");
            }
        }

        private void visitJsonTable(JsonTable node, Integer indent)
        {
            builder.append("JSON_TABLE (")
                    .append(formatJsonPathInvocation(node.getJsonPathInvocation()))
                    .append("\n");
            appendJsonTableColumns(node.getColumns(), indent + 1);
            node.getPlan().ifPresent(plan -> {
                builder.append("\n");
                if (plan instanceof JsonTableDefaultPlan) {
                    append(indent + 1, "PLAN DEFAULT (");
                }
                else {
                    append(indent + 1, "PLAN (");
                }
                process(plan, indent + 1);
                builder.append(")");
            });
            node.getErrorBehavior().ifPresent(behavior -> {
                builder.append("\n");
                append(indent + 1, behavior + " ON ERROR");
            });
            builder.append(")\n");
        }

        private void appendJsonTableColumns(List<JsonTableColumnDefinition> columns, int indent)
        {
            append(indent, "COLUMNS (\n");
            for (int i = 0; i < columns.size() - 1; i++) {
                process(columns.get(i), indent + 1);
                builder.append(",\n");
            }
            process(columns.getLast(), indent + 1);
            builder.append(")");
        }

        private void visitValueColumn(ValueColumn node, Integer indent)
        {
            append(indent, formatName(node.getName()))
                    .append(" ")
                    .append(formatExpression(node.getType()));
            node.getJsonPath().ifPresent(path ->
                    builder.append(" PATH ")
                            .append(formatExpression(path)));
            builder.append(" ")
                    .append(node.getEmptyBehavior().name())
                    .append(node.getEmptyDefault().map(expression -> " " + formatExpression(expression)).orElse(""))
                    .append(" ON EMPTY");
            node.getErrorBehavior().ifPresent(behavior ->
                    builder.append(" ")
                            .append(behavior.name())
                            .append(node.getErrorDefault().map(expression -> " " + formatExpression(expression)).orElse(""))
                            .append(" ON ERROR"));
        }

        private void visitQueryColumn(QueryColumn node, Integer indent)
        {
            append(indent, formatName(node.getName()))
                    .append(" ")
                    .append(formatExpression(node.getType()))
                    .append(" FORMAT ")
                    .append(node.getFormat().toString());
            node.getJsonPath().ifPresent(path ->
                    builder.append(" PATH ")
                            .append(formatExpression(path)));
            builder.append(switch (node.getWrapperBehavior()) {
                case WITHOUT -> " WITHOUT ARRAY WRAPPER";
                case CONDITIONAL -> " WITH CONDITIONAL ARRAY WRAPPER";
                case UNCONDITIONAL -> " WITH UNCONDITIONAL ARRAY WRAPPER";
            });

            if (node.getQuotesBehavior().isPresent()) {
                builder.append(switch (node.getQuotesBehavior().get()) {
                    case KEEP -> " KEEP QUOTES ON SCALAR STRING";
                    case OMIT -> " OMIT QUOTES ON SCALAR STRING";
                });
            }
            builder.append(" ")
                    .append(node.getEmptyBehavior().toString())
                    .append(" ON EMPTY");
            node.getErrorBehavior().ifPresent(behavior ->
                    builder.append(" ")
                            .append(behavior.toString())
                            .append(" ON ERROR"));
        }

        private void visitNestedColumns(NestedColumns node, Integer indent)
        {
            append(indent, "NESTED PATH ")
                    .append(formatExpression(node.getJsonPath()));
            node.getPathName().ifPresent(name ->
                    builder.append(" AS ")
                            .append(formatName(name)));
            builder.append("\n");
            appendJsonTableColumns(node.getColumns(), indent + 1);
        }

        private void visitJsonTableDefaultPlan(JsonTableDefaultPlan node)
        {
            builder.append(node.getParentChild().name())
                    .append(", ")
                    .append(node.getSiblings().name());
        }

        private void visitPlanParentChild(PlanParentChild node)
        {
            process(node.getParent());
            builder.append(" ")
                    .append(node.getType().name())
                    .append(" (");
            process(node.getChild());
            builder.append(")");
        }

        private void visitPlanSiblings(PlanSiblings node)
        {
            for (int i = 0; i < node.getSiblings().size() - 1; i++) {
                builder.append("(");
                process(node.getSiblings().get(i));
                builder.append(") ")
                        .append(node.getType().name())
                        .append(" ");
            }
            builder.append("(");
            process(node.getSiblings().getLast());
            builder.append(")");
        }

        private void visitLateral(Lateral node, Integer indent)
        {
            append(indent, "LATERAL (");
            process(node.getQuery(), indent + 1);
            append(indent, ")");
        }

        private void visitNearest(Nearest node, Integer indent)
        {
            append(indent, "NEAREST (");
            append(indent + 1, "FROM ");
            process(node.getRelation(), indent + 1);
            node.getWhere().ifPresent(where -> append(indent + 1, "WHERE ")
                    .append(formatExpression(where))
                    .append('\n'));
            append(indent + 1, "MATCH ")
                    .append(formatExpression(node.getMatch()))
                    .append('\n');
            append(indent, ")");
        }

        private void visitTableFunctionInvocation(TableFunctionInvocation node, Integer indent)
        {
            append(indent, "TABLE(");
            appendTableFunctionInvocation(node, indent + 1);
            builder.append(")");
        }

        private void appendTableFunctionInvocation(TableFunctionInvocation node, Integer indent)
        {
            builder.append(formatName(node.getName()))
                    .append("(\n");
            appendTableFunctionArguments(node.getArguments(), indent + 1);
            if (!node.getCopartitioning().isEmpty()) {
                builder.append("\n");
                append(indent + 1, "COPARTITION ");
                builder.append(node.getCopartitioning().stream()
                        .map(tableList -> tableList.stream()
                                .map(SqlFormatter::formatName)
                                .collect(joining(", ", "(", ")")))
                        .collect(joining(", ")));
            }
            builder.append(")");
        }

        private void appendTableFunctionArguments(List<TableFunctionArgument> arguments, int indent)
        {
            for (int i = 0; i < arguments.size(); i++) {
                TableFunctionArgument argument = arguments.get(i);
                if (argument.getName().isPresent()) {
                    append(indent, formatName(argument.getName().get()));
                    builder.append(" => ");
                }
                else {
                    append(indent, "");
                }
                Node value = argument.getValue();
                if (value instanceof Expression expression) {
                    builder.append(formatExpression(expression));
                }
                else {
                    process(value, indent + 1);
                }
                if (i < arguments.size() - 1) {
                    builder.append(",\n");
                }
            }
        }

        private void visitTableArgument(TableFunctionTableArgument node, Integer indent)
        {
            Relation relation = node.getTable();
            Node unaliased = relation instanceof AliasedRelation aliasedRelation ? aliasedRelation.getRelation() : relation;
            if (unaliased instanceof TableSubquery) {
                // unpack the relation from TableSubquery to avoid adding another pair of parentheses
                unaliased = ((TableSubquery) unaliased).getQuery();
            }
            builder.append("TABLE(");
            process(unaliased, indent);
            builder.append(")");
            if (relation instanceof AliasedRelation aliasedRelation) {
                builder.append(" AS ")
                        .append(formatName(aliasedRelation.getAlias()));
                appendAliasColumns(builder, aliasedRelation.getColumnNames());
            }
            if (node.getPartitionBy().isPresent()) {
                builder.append("\n");
                append(indent, "PARTITION BY ")
                        .append(node.getPartitionBy().get().stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", ")));
            }
            node.getEmptyTableTreatment().ifPresent(treatment -> {
                builder.append("\n");
                append(indent, treatment.getTreatment().name() + " WHEN EMPTY");
            });
            node.getOrderBy().ifPresent(orderBy -> {
                builder.append("\n");
                append(indent, formatOrderBy(orderBy));
            });
        }

        private void visitDescriptorArgument(TableFunctionDescriptorArgument node)
        {
            if (node.getDescriptor().isPresent()) {
                builder.append(node.getDescriptor().get().getFields().stream()
                        .map(field -> {
                            String formattedField = formatName(field.getName());
                            if (field.getType().isPresent()) {
                                formattedField = formattedField + " " + formatExpression(field.getType().get());
                            }
                            return formattedField;
                        })
                        .collect(joining(", ", "DESCRIPTOR(", ")")));
            }
            else {
                builder.append("CAST (NULL AS DESCRIPTOR)");
            }
        }

        private void visitPrepare(Prepare node, Integer indent)
        {
            append(indent, "PREPARE ");
            builder.append(formatName(node.getName()));
            builder.append(" FROM");
            builder.append("\n");
            process(node.getStatement(), indent + 1);
        }

        private void visitDeallocate(Deallocate node, Integer indent)
        {
            append(indent, "DEALLOCATE PREPARE ");
            builder.append(formatName(node.getName()));
        }

        private void visitExecute(Execute node, Integer indent)
        {
            append(indent, "EXECUTE ");
            builder.append(formatName(node.getName()));
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append(" USING ");
                builder.append(parameters.stream()
                        .map(SqlFormatter::formatExpression)
                        .collect(joining(", ")));
            }
        }

        private void visitExecuteImmediate(ExecuteImmediate node, Integer indent)
        {
            append(indent, "EXECUTE IMMEDIATE\n")
                    .append(formatStringLiteral(node.getStatement().getValue()));
            List<Expression> parameters = node.getParameters();
            if (!parameters.isEmpty()) {
                builder.append("\nUSING ");
                builder.append(parameters.stream()
                        .map(SqlFormatter::formatExpression)
                        .collect(joining(", ")));
            }
        }

        private void visitDescribeOutput(DescribeOutput node, Integer indent)
        {
            append(indent, "DESCRIBE OUTPUT ");

            builder.append(switch (node.getTarget()) {
                case DescribeOutput.Target.PreparedStatement(Identifier name) -> formatName(name);
                case DescribeOutput.Target.InlineQuery(Query query) -> '(' + formatSql(query) + ')';
            });
        }

        private void visitDescribeInput(DescribeInput node, Integer indent)
        {
            append(indent, "DESCRIBE INPUT ");
            builder.append(formatName(node.getName()));
        }

        private void visitQuery(Query node, Integer indent)
        {
            if (!node.getSessionProperties().isEmpty()) {
                builder.append("WITH SESSION\n");
                Iterator<SessionProperty> sessionProperties = node.getSessionProperties().iterator();
                while (sessionProperties.hasNext()) {
                    process(sessionProperties.next(), indent + 1);
                    if (sessionProperties.hasNext()) {
                        builder.append(',');
                    }
                    builder.append('\n');
                }
            }
            if (!node.getFunctions().isEmpty()) {
                builder.append("WITH\n");
                Iterator<FunctionSpecification> functions = node.getFunctions().iterator();
                while (functions.hasNext()) {
                    process(functions.next(), indent + 1);
                    if (functions.hasNext()) {
                        builder.append(',');
                    }
                    builder.append('\n');
                }
            }

            node.getWith().ifPresent(with -> {
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, formatName(query.getName()));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(builder, columnNames));
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            });

            processRelation(node.getQueryBody(), indent);
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            node.getOffset().ifPresent(offset -> process(offset, indent));
            node.getLimit().ifPresent(limit -> process(limit, indent));
        }

        private void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            node.getFrom().ifPresent(from -> {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(from, indent);
            });

            builder.append('\n');

            node.getWhere().ifPresent(where ->
                    append(indent, "WHERE " + formatExpression(where)).append('\n'));

            node.getGroupBy().ifPresent(groupBy ->
                    append(indent, "GROUP BY " + (groupBy.isDistinct() ? " DISTINCT " : "") + formatGroupBy(groupBy.getGroupingElements())).append('\n'));

            node.getHaving().ifPresent(having -> append(indent, "HAVING " + formatExpression(having))
                    .append('\n'));

            if (!node.getWindows().isEmpty()) {
                append(indent, "WINDOW");
                formatDefinitionList(node.getWindows().stream()
                        .map(definition -> formatName(definition.getName()) + " AS " + formatWindowSpecification(definition.getWindow()))
                        .collect(toImmutableList()), indent + 1);
            }

            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent));
            node.getOffset().ifPresent(offset -> process(offset, indent));
            node.getLimit().ifPresent(limit -> process(limit, indent));
        }

        private void visitOrderBy(OrderBy node, Integer indent)
        {
            append(indent, formatOrderBy(node))
                    .append('\n');
        }

        private void visitOffset(Offset node, Integer indent)
        {
            append(indent, "OFFSET ")
                    .append(formatExpression(node.getRowCount()))
                    .append(" ROWS\n");
        }

        private void visitFetchFirst(FetchFirst node, Integer indent)
        {
            append(indent, "FETCH FIRST " + node.getRowCount().map(count -> formatExpression(count) + " ROWS ").orElse("ROW "))
                    .append(node.isWithTies() ? "WITH TIES" : "ONLY")
                    .append('\n');
        }

        private void visitLimit(Limit node, Integer indent)
        {
            append(indent, "LIMIT ")
                    .append(formatExpression(node.getRowCount()))
                    .append('\n');
        }

        private void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');
        }

        private void visitSingleColumn(SingleColumn node)
        {
            builder.append(formatExpression(node.getExpression()));
            node.getAlias().ifPresent(alias -> builder
                    .append(' ')
                    .append(formatName(alias)));
        }

        private void visitAllColumns(AllColumns node)
        {
            node.getTarget().ifPresent(value -> builder
                    .append(formatExpression(value))
                    .append("."));
            builder.append("*");

            if (!node.getAliases().isEmpty()) {
                builder.append(" AS (")
                        .append(Joiner.on(", ").join(node.getAliases().stream()
                                .map(SqlFormatter::formatName)
                                .collect(toImmutableList())))
                        .append(")");
            }
        }

        private void visitTable(Table node)
        {
            builder.append(formatName(node.getName()));
            node.getQueryPeriod().ifPresent(queryPeriod -> builder
                    .append(" " + queryPeriod));
        }

        private void visitQueryPeriod(QueryPeriod node)
        {
            builder.append("FOR " + node.getRangeType().name() + " AS OF " + formatExpression(node.getEnd().get()));
        }

        private void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                switch (criteria) {
                    case JoinUsing using -> builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                    case JoinOn on -> builder.append(" ON ")
                            .append(formatExpression(on.getExpression()));
                    case NaturalJoin _ -> {}
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }
        }

        private void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(' ')
                    .append(formatName(node.getAlias()));
            appendAliasColumns(builder, node.getColumnNames());
        }

        private void visitPatternRecognitionRelation(PatternRecognitionRelation node, Integer indent)
        {
            processRelationSuffix(node.getInput(), indent);

            builder.append(" MATCH_RECOGNIZE (\n");
            if (!node.getPartitionBy().isEmpty()) {
                append(indent + 1, "PARTITION BY ")
                        .append(node.getPartitionBy().stream()
                                .map(SqlFormatter::formatExpression)
                                .collect(joining(", ")))
                        .append("\n");
            }
            node.getOrderBy().ifPresent(orderBy -> process(orderBy, indent + 1));

            if (!node.getMeasures().isEmpty()) {
                append(indent + 1, "MEASURES");
                formatDefinitionList(node.getMeasures().stream()
                        .map(measure -> formatExpression(measure.getExpression()) + " AS " + formatName(measure.getName()))
                        .collect(toImmutableList()), indent + 2);
            }

            node.getRowsPerMatch().ifPresent(rowsPerMatch -> {
                String rowsPerMatchDescription = switch (rowsPerMatch) {
                    case ONE -> "ONE ROW PER MATCH";
                    case ALL_SHOW_EMPTY -> "ALL ROWS PER MATCH SHOW EMPTY MATCHES";
                    case ALL_OMIT_EMPTY -> "ALL ROWS PER MATCH OMIT EMPTY MATCHES";
                    case ALL_WITH_UNMATCHED -> "ALL ROWS PER MATCH WITH UNMATCHED ROWS";
                    default -> // RowsPerMatch of type WINDOW cannot occur in MATCH_RECOGNIZE clause
                            throw new IllegalStateException("unexpected rowsPerMatch: " + node.getRowsPerMatch().get());
                };
                append(indent + 1, rowsPerMatchDescription)
                        .append("\n");
            });

            node.getAfterMatchSkipTo().ifPresent(afterMatchSkipTo -> {
                String skipTo = formatSkipTo(afterMatchSkipTo);
                append(indent + 1, skipTo)
                        .append("\n");
            });

            node.getPatternSearchMode().ifPresent(patternSearchMode ->
                    append(indent + 1, patternSearchMode.getMode().name())
                            .append("\n"));

            append(indent + 1, "PATTERN (")
                    .append(formatPattern(node.getPattern()))
                    .append(")\n");
            if (!node.getSubsets().isEmpty()) {
                append(indent + 1, "SUBSET");
                formatDefinitionList(node.getSubsets().stream()
                        .map(subset -> formatName(subset.getName()) + " = " + subset.getIdentifiers().stream()
                                .map(SqlFormatter::formatName).collect(joining(", ", "(", ")")))
                        .collect(toImmutableList()), indent + 2);
            }
            append(indent + 1, "DEFINE");
            formatDefinitionList(node.getVariableDefinitions().stream()
                    .map(variable -> formatName(variable.getName()) + " AS " + formatExpression(variable.getExpression()))
                    .collect(toImmutableList()), indent + 2);

            builder.append(")");
        }

        private void visitSampledRelation(SampledRelation node, Integer indent)
        {
            processRelationSuffix(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType().name())
                    .append(" (")
                    .append(formatExpression(node.getSamplePercentage()))
                    .append(')');
        }

        private void processRelationSuffix(Relation relation, Integer indent)
        {
            if ((relation instanceof AliasedRelation) || (relation instanceof SampledRelation) || (relation instanceof PatternRecognitionRelation) || (relation instanceof Pivot)) {
                builder.append("( ");
                process(relation, indent + 1);
                append(indent, ")");
            }
            else {
                process(relation, indent);
            }
        }

        private void visitPivot(Pivot node, Integer indent)
        {
            processRelationSuffix(node.getInput(), indent);

            builder.append(" PIVOT (\n");
            append(indent + 1, node.getAggregations().stream()
                    .map(this::formatPivotAggregation)
                    .collect(joining(", ")))
                    .append("\n");
            append(indent + 1, "FOR ")
                    .append(formatPivotColumns(node.getPivotColumns()))
                    .append(" IN ")
                    .append(node.getValueGroups().stream()
                            .map(this::formatPivotValueGroup)
                            .collect(joining(", ", "(", ")")))
                    .append("\n");
            node.getGroupBy().ifPresent(groupBy ->
                    append(indent + 1, "GROUP BY " + (groupBy.isDistinct() ? "DISTINCT " : "") + formatGroupBy(groupBy.getGroupingElements()))
                            .append("\n"));
            append(indent, ")");
        }

        private String formatPivotAggregation(PivotAggregation aggregation)
        {
            String result = formatExpression(aggregation.getExpression());
            if (aggregation.getAlias().isPresent()) {
                result += " AS " + formatName(aggregation.getAlias().get());
            }
            return result;
        }

        private static String formatPivotColumns(List<Expression> pivotColumns)
        {
            return formatPivotTuple(pivotColumns);
        }

        private String formatPivotValueGroup(PivotValueGroup valueGroup)
        {
            String formatted = formatPivotTuple(valueGroup.getValues());
            if (valueGroup.getAlias().isPresent()) {
                formatted += " AS " + formatName(valueGroup.getAlias().get());
            }
            return formatted;
        }

        // A single element is bare; two or more are wrapped in parentheses, matching the grammar.
        private static String formatPivotTuple(List<Expression> expressions)
        {
            if (expressions.size() == 1) {
                return formatExpression(expressions.getFirst());
            }
            return expressions.stream()
                    .map(SqlFormatter::formatExpression)
                    .collect(joining(", ", "(", ")"));
        }

        private void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row));
                first = false;
            }
            builder.append('\n');
        }

        private void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");
        }

        private void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                    appendCorresponding(node.getCorresponding());
                }
            }
        }

        private void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }
            appendCorresponding(node.getCorresponding());

            processRelation(node.getRight(), indent);
        }

        private void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                    appendCorresponding(node.getCorresponding());
                }
            }
        }

        private void appendCorresponding(Optional<Corresponding> node)
        {
            node.ifPresent(corresponding -> {
                builder.append("CORRESPONDING ");
                if (!corresponding.getColumns().isEmpty()) {
                    builder.append("BY ");
                    builder.append(corresponding.getColumns().stream()
                            .map(SqlFormatter::formatName)
                            .collect(joining(", ", "(", ") ")));
                }
            });
        }

        private void visitMerge(Merge node, Integer indent)
        {
            builder.append("MERGE INTO ")
                    .append(formatName(node.getTargetTable().getName()));

            node.getTargetTable().getBranch().ifPresent(branch -> builder.append("@").append(formatName(branch)));

            node.getTargetAlias().ifPresent(value -> builder
                    .append(' ')
                    .append(formatName(value)));
            builder.append("\n");

            append(indent + 1, "USING ");

            if (node.getSource() instanceof Table table) {
                builder.append(formatName(table.getName()));
            }
            else {
                processRelation(node.getSource(), indent + 2);
            }

            builder.append("\n");
            append(indent + 1, "ON ");
            builder.append(formatExpression(node.getPredicate()));

            for (MergeCase mergeCase : node.getMergeCases()) {
                builder.append("\n");
                process(mergeCase, indent);
            }
        }

        private void visitMergeInsert(MergeInsert node, Integer indent)
        {
            appendMergeCaseWhen(node);
            append(indent + 1, "THEN INSERT ");

            if (!node.getColumns().isEmpty()) {
                builder.append(node.getColumns().stream()
                        .map(SqlFormatter::formatName)
                        .collect(joining(", ", "(", ")")));
            }

            builder.append("VALUES ");
            builder.append(node.getValues().stream()
                    .map(SqlFormatter::formatExpression)
                    .collect(joining(", ", "(", ")")));
        }

        private void visitMergeUpdate(MergeUpdate node, Integer indent)
        {
            appendMergeCaseWhen(node);
            append(indent + 1, "THEN UPDATE SET");

            boolean first = true;
            for (MergeUpdate.Assignment assignment : node.getAssignments()) {
                builder.append("\n");
                append(indent + 1, first ? "  " : ", ");
                builder.append(formatName(assignment.getTarget()))
                        .append(" = ")
                        .append(formatExpression(assignment.getValue()));
                first = false;
            }
        }

        private void visitMergeDelete(MergeDelete node, Integer indent)
        {
            appendMergeCaseWhen(node);
            append(indent + 1, "THEN DELETE");
        }

        private void appendMergeCaseWhen(MergeCase mergeCase)
        {
            if (mergeCase instanceof MergeInsert) {
                builder.append("WHEN NOT MATCHED");
            }
            else if (mergeCase.isNotMatchedBySource()) {
                builder.append("WHEN NOT MATCHED BY SOURCE");
            }
            else {
                builder.append("WHEN MATCHED");
            }
            mergeCase.getExpression().ifPresent(value -> builder
                    .append(" AND ")
                    .append(formatExpression(value)));
            builder.append("\n");
        }

        private void visitCreateView(CreateView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ");

            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }

            builder.append(formatName(node.getName()));

            node.getComment().ifPresent(comment -> builder
                    .append(" COMMENT ")
                    .append(formatStringLiteral(comment)));

            node.getSecurity().ifPresent(security -> builder
                    .append(" SECURITY ")
                    .append(security.name()));

            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS\n");

            process(node.getQuery(), indent);
        }

        private void visitRenameView(RenameView node)
        {
            builder.append("ALTER VIEW ")
                    .append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));
        }

        private void visitRenameMaterializedView(RenameMaterializedView node)
        {
            builder.append("ALTER MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));
        }

        private void visitSetAuthorization(SetAuthorizationStatement node)
        {
            builder.append("ALTER ")
                    .append(node.getOwnedEntityKind())
                    .append(" ")
                    .append(formatName(node.getSource()))
                    .append(" SET AUTHORIZATION ")
                    .append(formatPrincipal(node.getPrincipal()));
        }

        private void visitCreateMaterializedView(CreateMaterializedView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("MATERIALIZED VIEW ");

            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }

            builder.append(formatName(node.getName()));
            node.getGracePeriod().ifPresent(interval ->
                    builder.append("\nGRACE PERIOD ").append(formatExpression(interval)));
            node.getWhenStaleBehavior().ifPresent(whenStale ->
                    builder.append("\nWHEN STALE ").append(whenStale.name()));
            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            builder.append(" AS\n");

            process(node.getQuery(), indent);
        }

        private void visitRefreshMaterializedView(RefreshMaterializedView node)
        {
            builder.append("REFRESH MATERIALIZED VIEW ");
            builder.append(formatName(node.getName()));
        }

        private void visitRefreshView(RefreshView node)
        {
            builder.append("ALTER VIEW ");
            builder.append(formatName(node.getName()));
            builder.append(" REFRESH");
        }

        private void visitDropMaterializedView(DropMaterializedView node)
        {
            builder.append("DROP MATERIALIZED VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
        }

        private void visitDropView(DropView node)
        {
            builder.append("DROP VIEW ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
        }

        private void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType explainType) {
                    options.add("TYPE " + explainType.getType());
                }
                else if (option instanceof ExplainFormat explainFormat) {
                    options.add("FORMAT " + explainFormat.getType());
                }
                else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append(options.stream()
                        .collect(joining(", ", "(", ")")));
            }

            builder.append("\n");

            process(node.getStatement(), indent);
        }

        private void visitExplainAnalyze(ExplainAnalyze node, Integer indent)
        {
            builder.append("EXPLAIN ANALYZE");
            if (node.isVerbose()) {
                builder.append(" VERBOSE");
            }
            builder.append("\n");

            process(node.getStatement(), indent);
        }

        private void visitShowCatalogs(ShowCatalogs node)
        {
            builder.append("SHOW CATALOGS");

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));
        }

        private void visitShowSchemas(ShowSchemas node)
        {
            builder.append("SHOW SCHEMAS");

            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(formatName(catalog)));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));
        }

        private void visitShowTables(ShowTables node)
        {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent(value -> builder
                    .append(" FROM ")
                    .append(formatName(value)));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));
        }

        private void visitShowCreate(ShowCreate node)
        {
            builder.append("SHOW CREATE ")
                    .append(switch (node.getType()) {
                        case SCHEMA -> "SCHEMA";
                        case TABLE -> "TABLE";
                        case VIEW -> "VIEW";
                        case MATERIALIZED_VIEW -> "MATERIALIZED VIEW";
                        case FUNCTION -> "FUNCTION";
                    })
                    .append(" ")
                    .append(formatName(node.getName()));
        }

        private void visitShowColumns(ShowColumns node)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(formatName(node.getTable()));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));
        }

        private void visitShowStats(ShowStats node)
        {
            builder.append("SHOW STATS FOR ");
            process(node.getRelation(), 0);
        }

        private void visitShowFunctions(ShowFunctions node)
        {
            builder.append("SHOW FUNCTIONS");

            node.getSchema().ifPresent(value -> builder
                    .append(" FROM ")
                    .append(formatName(value)));

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));
        }

        private void visitShowSession(ShowSession node)
        {
            builder.append("SHOW SESSION");

            node.getLikePattern().ifPresent(value -> builder
                    .append(" LIKE ")
                    .append(formatStringLiteral(value)));

            node.getEscape().ifPresent(value -> builder
                    .append(" ESCAPE ")
                    .append(formatStringLiteral(value)));
        }

        private void visitDelete(Delete node)
        {
            builder.append("DELETE FROM ")
                    .append(formatName(node.getTable().getName()));

            node.getTable().getBranch().ifPresent(branch -> builder.append("@").append(formatName(branch)));

            node.getWhere().ifPresent(where -> builder
                    .append(" WHERE ")
                    .append(formatExpression(where)));
        }

        private void visitCreateCatalog(CreateCatalog node)
        {
            builder.append("CREATE CATALOG ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getCatalogName()));
            builder.append(" USING ").append(formatName(node.getConnectorName()));
            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            node.getPrincipal().ifPresent(principal -> builder
                    .append("\nAUTHORIZATION ")
                    .append(formatPrincipal(principal)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
        }

        private void visitDropCatalog(DropCatalog node)
        {
            builder.append("DROP CATALOG ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getCatalogName()))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");
        }

        private void visitCreateSchema(CreateSchema node)
        {
            builder.append("CREATE SCHEMA ");
            if (node.isNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()));
            node.getPrincipal().ifPresent(principal -> builder
                    .append("\nAUTHORIZATION ")
                    .append(formatPrincipal(principal)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
        }

        private void visitDropSchema(DropSchema node)
        {
            builder.append("DROP SCHEMA ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSchemaName()))
                    .append(" ")
                    .append(node.isCascade() ? "CASCADE" : "RESTRICT");
        }

        private void visitRenameSchema(RenameSchema node)
        {
            builder.append("ALTER SCHEMA ")
                    .append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));
        }

        private void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.getSaveMode() == REPLACE) {
                builder.append("OR REPLACE ");
            }
            builder.append("TABLE ");
            if (node.getSaveMode() == IGNORE) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getName()));

            node.getColumnAliases().ifPresent(columnAliases -> {
                builder.append(columnAliases.stream()
                        .map(SqlFormatter::formatName)
                        .collect(joining(", ", "( ", " )")));
            });

            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));
            builder.append(formatPropertiesMultiLine(node.getProperties()));

            builder.append(" AS ");
            process(node.getQuery(), indent);

            if (!node.isWithData()) {
                builder.append(" WITH NO DATA");
            }
        }

        private void visitCreateTable(CreateTable node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.getSaveMode() == REPLACE) {
                builder.append("OR REPLACE ");
            }
            builder.append("TABLE ");
            if (node.getSaveMode() == IGNORE) {
                builder.append("IF NOT EXISTS ");
            }
            String tableName = formatName(node.getName());
            builder.append(tableName).append(" (\n");

            String elementIndent = indentString(indent + 1);
            String columnList = node.getElements().stream()
                    .map(element -> {
                        if (element instanceof ColumnDefinition column) {
                            return elementIndent + formatColumnDefinition(column);
                        }
                        if (element instanceof LikeClause likeClause) {
                            StringBuilder builder = new StringBuilder(elementIndent);
                            builder.append("LIKE ")
                                    .append(formatName(likeClause.getTableName()));

                            likeClause.getPropertiesOption().ifPresent(propertiesOption -> builder
                                    .append(" ")
                                    .append(propertiesOption.name())
                                    .append(" PROPERTIES"));

                            return builder.toString();
                        }
                        throw new UnsupportedOperationException("unknown table element: " + element);
                    })
                    .collect(joining(",\n"));
            builder.append(columnList);
            builder.append("\n").append(")");

            node.getComment().ifPresent(comment -> builder
                    .append("\nCOMMENT ")
                    .append(formatStringLiteral(comment)));

            builder.append(formatPropertiesMultiLine(node.getProperties()));
        }

        private static String formatPropertiesMultiLine(List<Property> properties)
        {
            if (properties.isEmpty()) {
                return "";
            }

            return properties.stream()
                    .map(property -> INDENT + formatProperty(property))
                    .collect(joining(",\n", "\nWITH (\n", "\n)"));
        }

        private static String formatColumnDefinition(ColumnDefinition column)
        {
            StringBuilder builder = new StringBuilder()
                    .append(formatName(column.getName()))
                    .append(" ").append(column.getType());
            column.getDefaultValue().ifPresent(defaultValue -> builder
                    .append(" DEFAULT ")
                    .append(formatExpression(defaultValue)));
            if (!column.isNullable()) {
                builder.append(" NOT NULL");
            }
            column.getComment().ifPresent(comment -> builder
                    .append(" COMMENT ")
                    .append(formatStringLiteral(comment)));
            if (!column.getProperties().isEmpty()) {
                builder.append(" WITH (")
                        .append(joinProperties(column.getProperties()))
                        .append(")");
            }
            return builder.toString();
        }

        private static String formatGrantor(GrantorSpecification grantor)
        {
            GrantorSpecification.Type type = grantor.type();
            return switch (type) {
                case CURRENT_ROLE, CURRENT_USER -> type.name();
                case PRINCIPAL -> formatPrincipal(grantor.principal().get());
            };
        }

        private static String formatPrincipal(PrincipalSpecification principal)
        {
            PrincipalSpecification.Type type = principal.type();
            return switch (type) {
                case UNSPECIFIED -> principal.name().toString();
                case USER, ROLE -> type.name() + " " + principal.name();
            };
        }

        private void visitDropTable(DropTable node)
        {
            builder.append("DROP TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()));
        }

        private void visitRenameTable(RenameTable node)
        {
            builder.append("ALTER TABLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSource()))
                    .append(" RENAME TO ")
                    .append(formatName(node.getTarget()));
        }

        private void visitSetProperties(SetProperties node)
        {
            SetProperties.Type type = node.getType();
            builder.append("ALTER ");
            builder.append(switch (type) {
                case TABLE -> "TABLE ";
                case MATERIALIZED_VIEW -> "MATERIALIZED VIEW ";
            });

            builder.append(formatName(node.getName()))
                    .append(" SET PROPERTIES ")
                    .append(joinProperties(node.getProperties()));
        }

        private static String joinProperties(List<Property> properties)
        {
            return properties.stream()
                    .map(Formatter::formatProperty)
                    .collect(joining(", "));
        }

        private static String formatProperty(Property property)
        {
            return formatName(property.getName()) + " = " +
                    (property.isSetToDefault() ? "DEFAULT" : formatExpression(property.getNonDefaultValue()));
        }

        private void visitComment(Comment node)
        {
            String comment = node.getComment()
                    .map(ExpressionFormatter::formatStringLiteral)
                    .orElse("NULL");

            String type = switch (node.getType()) {
                case TABLE -> "TABLE";
                case VIEW -> "VIEW";
                case MATERIALIZED_VIEW -> "MATERIALIZED VIEW";
                case COLUMN -> "COLUMN";
            };

            builder.append("COMMENT ON " + type + " " + formatName(node.getName()) + " IS " + comment);
        }

        private void visitRenameColumn(RenameColumn node)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable()))
                    .append(" RENAME COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getSource()))
                    .append(" TO ")
                    .append(formatName(node.getTarget()));
        }

        private void visitDropColumn(DropColumn node)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable()))
                    .append(" DROP COLUMN ");
            if (node.isColumnExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getField()));
        }

        private void visitTableExecute(TableExecute node, Integer indent)
        {
            builder.append("ALTER TABLE ");
            builder.append(formatName(node.getTable().getName()));
            builder.append(" EXECUTE ");
            builder.append(formatName(node.getProcedureName()));
            if (!node.getArguments().isEmpty()) {
                builder.append("(");
                formatCallArguments(indent, node.getArguments());
                builder.append(")");
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ")
                    .append(formatExpression(where)));
        }

        private void visitMaterializedViewExecute(MaterializedViewExecute node, Integer indent)
        {
            builder.append("ALTER MATERIALIZED VIEW ");
            builder.append(formatName(node.getTable().getName()));
            builder.append(" EXECUTE ");
            builder.append(formatName(node.getProcedureName()));
            if (!node.getArguments().isEmpty()) {
                builder.append("(");
                formatCallArguments(indent, node.getArguments());
                builder.append(")");
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ")
                    .append(formatExpression(where)));
        }

        private void visitAnalyze(Analyze node)
        {
            builder.append("ANALYZE ")
                    .append(formatName(node.getTableName()));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
        }

        private void visitAddColumn(AddColumn node)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()))
                    .append(" ADD COLUMN ");
            if (node.isColumnNotExists()) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatColumnDefinition(node.getColumn()));

            node.getPosition().ifPresent(position -> {
                switch (position) {
                    case ColumnPosition.First _ -> builder.append(" FIRST");
                    case ColumnPosition.After after -> builder.append(" AFTER ").append(formatName(after.column()));
                    case ColumnPosition.Last _ -> builder.append(" LAST");
                }
            });
        }

        private void visitSetDefaultValue(SetDefaultValue node)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()))
                    .append(" ALTER COLUMN ")
                    .append(formatName(node.getColumnName()))
                    .append(" SET DEFAULT ")
                    .append(formatExpression(node.getDefaultValue()));
        }

        private void visitDropDefaultValue(DropDefaultValue node)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()))
                    .append(" ALTER COLUMN ")
                    .append(formatName(node.getColumnName()))
                    .append(" DROP DEFAULT");
        }

        private void visitSetColumnType(SetColumnType node)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTableName()))
                    .append(" ALTER COLUMN ")
                    .append(formatName(node.getColumnName()))
                    .append(" SET DATA TYPE ")
                    .append(node.getType().toString());
        }

        private void visitDropNotNullConstraint(DropNotNullConstraint node)
        {
            builder.append("ALTER TABLE ");
            if (node.isTableExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getTable()))
                    .append(" ALTER COLUMN ")
                    .append(formatName(node.getColumn()))
                    .append(" DROP NOT NULL");
        }

        private void visitInsert(Insert node, Integer indent)
        {
            builder.append("INSERT INTO ")
                    .append(formatName(node.getTarget()));

            node.getTable().getBranch().ifPresent(branch -> builder.append("@").append(formatName(branch)));

            node.getColumns().ifPresent(columns -> builder
                    .append(" (")
                    .append(Joiner.on(", ").join(columns))
                    .append(")"));

            builder.append("\n");

            process(node.getQuery(), indent);
        }

        private void visitUpdate(Update node, Integer indent)
        {
            builder.append("UPDATE ")
                    .append(formatName(node.getTable().getName()));
            node.getTable().getBranch().ifPresent(branch -> builder.append("@").append(formatName(branch)));

            builder.append(" SET");

            int setCounter = node.getAssignments().size() - 1;
            for (UpdateAssignment assignment : node.getAssignments()) {
                builder.append("\n")
                        .append(indentString(indent + 1))
                        .append(assignment.getName().getValue())
                        .append(" = ")
                        .append(formatExpression(assignment.getValue()));
                if (setCounter > 0) {
                    builder.append(",");
                }
                setCounter--;
            }
            node.getWhere().ifPresent(where -> builder
                    .append("\n")
                    .append(indentString(indent))
                    .append("WHERE ").append(formatExpression(where)));
        }

        private void visitTruncateTable(TruncateTable node)
        {
            builder.append("TRUNCATE TABLE ");
            builder.append(formatName(node.getTableName()));
        }

        private void visitSetSession(SetSession node)
        {
            builder.append("SET SESSION ")
                    .append(formatName(node.getName()))
                    .append(" = ")
                    .append(formatExpression(node.getValue()));
        }

        private void visitResetSession(ResetSession node)
        {
            builder.append("RESET SESSION ")
                    .append(formatName(node.getName()));
        }

        private void visitUse(Use node)
        {
            builder.append("USE ");
            node.getCatalog().ifPresent(catalog -> builder
                    .append(formatName(catalog))
                    .append("."));
            builder.append(formatName(node.getSchema()));
        }

        private void visitSetSessionAuthorization(SetSessionAuthorization node)
        {
            builder.append("SET SESSION AUTHORIZATION ");
            builder.append(formatExpression(node.getUser()));
        }

        private void visitCallArgument(CallArgument node)
        {
            node.getName().ifPresent(name -> builder
                    .append(formatName(name))
                    .append(" => "));
            builder.append(formatExpression(node.getValue()));
        }

        private void visitCall(Call node, Integer indent)
        {
            builder.append("CALL ")
                    .append(formatName(node.getName()))
                    .append("(");
            formatCallArguments(indent, node.getArguments());
            builder.append(")");
        }

        private void formatCallArguments(Integer indent, List<CallArgument> arguments)
        {
            Iterator<CallArgument> iterator = arguments.iterator();
            while (iterator.hasNext()) {
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
        }

        private void visitRow(Row node, Integer indent)
        {
            builder.append("ROW(");
            boolean firstItem = true;
            for (Row.Field field : node.getFields()) {
                if (!firstItem) {
                    builder.append(", ");
                }
                process(field, indent);
                firstItem = false;
            }
            builder.append(")");
        }

        private void visitRowField(Row.Field node)
        {
            builder.append(formatExpression(node.getExpression()));
            node.getName().ifPresent(name -> builder.append(" AS ").append(formatName(name)));
        }

        private void visitStartTransaction(StartTransaction node, Integer indent)
        {
            builder.append("START TRANSACTION");

            Iterator<TransactionMode> iterator = node.getTransactionModes().iterator();
            while (iterator.hasNext()) {
                builder.append(" ");
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(",");
                }
            }
        }

        private void visitCreateRole(CreateRole node)
        {
            builder.append("CREATE ROLE ").append(formatName(node.getName()));
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" WITH ADMIN ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
        }

        private void visitDropRole(DropRole node)
        {
            builder.append("DROP ROLE ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
        }

        private void visitGrantRoles(GrantRoles node)
        {
            builder.append("GRANT ");
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" TO ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            if (node.isAdminOption()) {
                builder.append(" WITH ADMIN OPTION");
            }
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" GRANTED BY ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
        }

        private void visitRevokeRoles(RevokeRoles node)
        {
            builder.append("REVOKE ");
            if (node.isAdminOption()) {
                builder.append("ADMIN OPTION FOR ");
            }
            builder.append(node.getRoles().stream()
                    .map(Identifier::toString)
                    .collect(joining(", ")));
            builder.append(" FROM ");
            builder.append(node.getGrantees().stream()
                    .map(Formatter::formatPrincipal)
                    .collect(joining(", ")));
            node.getGrantor().ifPresent(grantor -> builder
                    .append(" GRANTED BY ")
                    .append(formatGrantor(grantor)));
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
        }

        private void visitSetRole(SetRole node)
        {
            builder.append("SET ROLE ");
            SetRole.Type type = node.getType();
            builder.append(switch (type) {
                case ALL, NONE -> type.name();
                case ROLE -> formatName(node.getRole().get());
            });
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" IN ")
                    .append(formatName(catalog)));
        }

        private void visitGrant(Grant node)
        {
            builder.append("GRANT ");

            if (node.getPrivileges().isEmpty()) {
                builder.append("ALL PRIVILEGES");
            }
            else {
                builder.append(node.getPrivileges()
                        .map(privileges -> join(", ", privileges))
                        .orElseThrow());
            }
            builder.append(" ON ")
                    .append(formatGrantScope(node.getGrantObject()))
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));
            if (node.isWithGrantOption()) {
                builder.append(" WITH GRANT OPTION");
            }
        }

        private void visitDeny(Deny node)
        {
            builder.append("DENY ");

            if (node.getPrivileges().isPresent()) {
                builder.append(join(", ", node.getPrivileges().get()));
            }
            else {
                builder.append("ALL PRIVILEGES");
            }

            builder.append(" ON ")
                    .append(formatGrantScope(node.getGrantObject()))
                    .append(" TO ")
                    .append(formatPrincipal(node.getGrantee()));
        }

        private void visitRevoke(Revoke node)
        {
            builder.append("REVOKE ");

            if (node.isGrantOptionFor()) {
                builder.append("GRANT OPTION FOR ");
            }

            if (node.getPrivileges().isEmpty()) {
                builder.append("ALL PRIVILEGES");
            }
            else {
                builder.append(node.getPrivileges()
                        .map(privileges -> join(", ", privileges))
                        .orElseThrow());
            }

            builder.append(" ON ")
                    .append(formatGrantScope(node.getGrantObject()))
                    .append(" FROM ")
                    .append(formatPrincipal(node.getGrantee()));
        }

        private void visitShowGrants(ShowGrants node)
        {
            builder.append("SHOW GRANTS ");

            node.getGrantObject().ifPresent(scope -> builder.append("ON ")
                    .append(formatGrantScope(scope)));
        }

        private void visitShowRoles(ShowRoles node)
        {
            builder.append("SHOW ");
            if (node.isCurrent()) {
                builder.append("CURRENT ");
            }
            builder.append("ROLES");
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(formatName(catalog)));
        }

        private void visitShowRoleGrants(ShowRoleGrants node)
        {
            builder.append("SHOW ROLE GRANTS");
            node.getCatalog().ifPresent(catalog -> builder
                    .append(" FROM ")
                    .append(formatName(catalog)));
        }

        private void visitSetPath(SetPath node)
        {
            builder.append("SET PATH ");
            builder.append(Joiner.on(", ").join(node.getPathSpecification().getPath()));
        }

        private void visitSetTimeZone(SetTimeZone node)
        {
            builder.append("SET TIME ZONE ");
            builder.append(node.getTimeZone().map(SqlFormatter::formatExpression).orElse("LOCAL"));
        }

        private void visitCreateFunction(CreateFunction node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            process(node.getSpecification(), indent);
        }

        private void visitDropFunction(DropFunction node, Integer indent)
        {
            builder.append("DROP FUNCTION ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getName()));
            processParameters(node.getParameters(), indent);
        }

        private void visitFunctionSpecification(FunctionSpecification node, Integer indent)
        {
            append(indent, "FUNCTION ")
                    .append(formatName(node.getName()));
            processParameters(node.getParameters(), indent);
            builder.append("\n");
            process(node.getReturnsClause(), indent);
            builder.append("\n");
            for (RoutineCharacteristic characteristic : node.getRoutineCharacteristics()) {
                process(characteristic, indent);
                builder.append("\n");
            }
            node.getStatement().ifPresent(statement -> process(statement, indent));
            node.getDefinition().map(StringLiteral::getValue).ifPresent(definition -> {
                append(indent, "AS ");
                builder.append("$$\n").append(definition).append("$$");
            });
        }

        private void visitCreateBranch(CreateBranch node)
        {
            builder.append("CREATE ");
            if (node.getSaveMode() == REPLACE) {
                builder.append("OR REPLACE ");
            }
            builder.append("BRANCH ");
            if (node.getSaveMode() == IGNORE) {
                builder.append("IF NOT EXISTS ");
            }
            builder.append(formatName(node.getBranchName()));
            builder.append(formatPropertiesMultiLine(node.getProperties()));
            builder.append(" IN TABLE ");
            builder.append(formatName(node.getTableName()));
            if (node.getFromBranch().isPresent()) {
                builder.append(" FROM " + formatName(node.getFromBranch().get()));
            }
        }

        private void visitDropBranch(DropBranch node)
        {
            builder.append("DROP BRANCH ");
            if (node.isExists()) {
                builder.append("IF EXISTS ");
            }
            builder.append(formatName(node.getBranchName()));
            builder.append(" IN TABLE ");
            builder.append(formatName(node.getTableName()));
        }

        private void visitFastForwardBranch(FastForwardBranch node)
        {
            builder.append("ALTER BRANCH ");
            builder.append(formatName(node.getSourceBranchName()));
            builder.append(" IN TABLE ");
            builder.append(formatName(node.getTableName()));
            builder.append(" FAST FORWARD TO ");
            builder.append(formatName(node.getTargetBranchName()));
        }

        private void visitShowBranches(ShowBranches node)
        {
            builder.append("SHOW BRANCHES FROM TABLE ")
                    .append(formatName(node.getTableName()));
        }

        private void visitSessionProperty(SessionProperty node, Integer indent)
        {
            append(indent, formatName(node.getName()))
                    .append(" = ")
                    .append(formatExpression(node.getValue()));
        }

        private void visitParameterDeclaration(ParameterDeclaration node)
        {
            node.getName().ifPresent(value ->
                    builder.append(formatName(value)).append(" "));
            builder.append(formatExpression(node.getType()));
        }

        private void visitLanguageCharacteristic(LanguageCharacteristic node, Integer indent)
        {
            append(indent, "LANGUAGE ")
                    .append(formatName(node.getLanguage()));
        }

        private void visitNullInputCharacteristic(NullInputCharacteristic node, Integer indent)
        {
            if (node.isCalledOnNull()) {
                append(indent, "CALLED ON NULL INPUT");
            }
            else {
                append(indent, "RETURNS NULL ON NULL INPUT");
            }
        }

        private void visitSecurityCharacteristic(SecurityCharacteristic node, Integer indent)
        {
            append(indent, "SECURITY ")
                    .append(node.getSecurity().name());
        }

        private void visitCommentCharacteristic(CommentCharacteristic node, Integer indent)
        {
            append(indent, "COMMENT ")
                    .append(formatStringLiteral(node.getComment()));
        }

        private void visitPropertiesCharacteristic(PropertiesCharacteristic node, Integer indent)
        {
            append(indent, "WITH (\n");
            Iterator<Property> iterator = node.getProperties().iterator();
            while (iterator.hasNext()) {
                Property property = iterator.next();
                append(indent + 1, formatProperty(property));
                builder.append(iterator.hasNext() ? ",\n" : "\n");
            }
            append(indent, ")");
        }

        private void visitReturnClause(ReturnsClause node, Integer indent)
        {
            append(indent, "RETURNS ")
                    .append(formatExpression(node.getReturnType()));
        }

        private void visitReturnStatement(ReturnStatement node, Integer indent)
        {
            append(indent, "RETURN ")
                    .append(formatExpression(node.getValue()));
        }

        private void visitCompoundStatement(CompoundStatement node, Integer indent)
        {
            append(indent, "BEGIN\n");
            for (VariableDeclaration variableDeclaration : node.getVariableDeclarations()) {
                process(variableDeclaration, indent + 1);
                builder.append(";\n");
            }
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END");
        }

        private void visitVariableDeclaration(VariableDeclaration node, Integer indent)
        {
            append(indent, "DECLARE ")
                    .append(node.getNames().stream()
                            .map(SqlFormatter::formatName)
                            .collect(joining(", ")))
                    .append(" ")
                    .append(formatExpression(node.getType()));
            if (node.getDefaultValue().isPresent()) {
                builder.append(" DEFAULT ")
                        .append(formatExpression(node.getDefaultValue().get()));
            }
        }

        private void visitAssignmentStatement(AssignmentStatement node, Integer indent)
        {
            append(indent, "SET ");
            builder.append(formatName(node.getTarget()))
                    .append(" = ")
                    .append(formatExpression(node.getValue()));
        }

        private void visitCaseStatement(CaseStatement node, Integer indent)
        {
            append(indent, "CASE");
            if (node.getExpression().isPresent()) {
                builder.append(" ")
                        .append(formatExpression(node.getExpression().get()));
            }
            builder.append("\n");
            for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                process(whenClause, indent + 1);
            }
            if (node.getElseClause().isPresent()) {
                process(node.getElseClause().get(), indent + 1);
            }
            append(indent, "END CASE");
        }

        private void visitCaseStatementWhenClause(CaseStatementWhenClause node, Integer indent)
        {
            append(indent, "WHEN ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
        }

        private void visitIfStatement(IfStatement node, Integer indent)
        {
            append(indent, "IF ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            for (ElseIfClause elseIfClause : node.getElseIfClauses()) {
                process(elseIfClause, indent);
            }
            if (node.getElseClause().isPresent()) {
                process(node.getElseClause().get(), indent);
            }
            append(indent, "END IF");
        }

        private void visitElseIfClause(ElseIfClause node, Integer indent)
        {
            append(indent, "ELSEIF ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
        }

        private void visitElseClause(ElseClause node, Integer indent)
        {
            append(indent, "ELSE\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
        }

        private void visitIterateStatement(IterateStatement node, Integer indent)
        {
            append(indent, "ITERATE ")
                    .append(formatName(node.getLabel()));
        }

        private void visitLeaveStatement(LeaveStatement node, Integer indent)
        {
            append(indent, "LEAVE ")
                    .append(formatName(node.getLabel()));
        }

        private void visitLoopStatement(LoopStatement node, Integer indent)
        {
            builder.append(indentString(indent));
            appendBeginLabel(node.getLabel());
            builder.append("LOOP\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END LOOP");
        }

        private void visitWhileStatement(WhileStatement node, Integer indent)
        {
            builder.append(indentString(indent));
            appendBeginLabel(node.getLabel());
            builder.append("WHILE ")
                    .append(formatExpression(node.getExpression()))
                    .append(" DO\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END WHILE");
        }

        private void visitRepeatStatement(RepeatStatement node, Integer indent)
        {
            builder.append(indentString(indent));
            appendBeginLabel(node.getLabel());
            builder.append("REPEAT\n");
            for (ControlStatement statement : node.getStatements()) {
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "UNTIL ")
                    .append(formatExpression(node.getCondition()))
                    .append("\n");
            append(indent, "END REPEAT");
        }

        private void appendBeginLabel(Optional<Identifier> label)
        {
            label.ifPresent(value ->
                    builder.append(formatName(value)).append(": "));
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table table) {
                builder.append("TABLE ")
                        .append(formatName(table.getName()))
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
        }

        private void processParameters(List<ParameterDeclaration> parameters, Integer indent)
        {
            builder.append("(");
            Iterator<ParameterDeclaration> iterator = parameters.iterator();
            while (iterator.hasNext()) {
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
            builder.append(")");
        }

        private SqlBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return INDENT.repeat(indent);
        }

        private void formatDefinitionList(List<String> elements, int indent)
        {
            if (elements.size() == 1) {
                builder.append(" ")
                        .append(getOnlyElement(elements))
                        .append("\n");
            }
            else {
                builder.append("\n");
                for (int i = 0; i < elements.size() - 1; i++) {
                    append(indent, elements.get(i))
                            .append(",\n");
                }
                append(indent, elements.getLast())
                        .append("\n");
            }
        }
    }

    private static void appendAliasColumns(Formatter.SqlBuilder builder, List<Identifier> columns)
    {
        if ((columns != null) && !columns.isEmpty()) {
            String formattedColumns = columns.stream()
                    .map(SqlFormatter::formatName)
                    .collect(joining(", "));

            builder.append(" (")
                    .append(formattedColumns)
                    .append(')');
        }
    }

    private static String formatGrantScope(GrantObject grantObject)
    {
        return String.format(
                "%s%s%s",
                grantObject.getBranch().isPresent() ? "BRANCH " + formatName(grantObject.getBranch().get()) + " IN " : "",
                grantObject.getEntityKind().isPresent() ? grantObject.getEntityKind().get() + " " : "",
                formatName(grantObject.getName()));
    }
}
