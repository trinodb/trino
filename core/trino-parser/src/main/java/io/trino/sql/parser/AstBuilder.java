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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.trino.sql.tree.AddColumn;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AllRows;
import io.trino.sql.tree.Analyze;
import io.trino.sql.tree.AnchorPattern;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.AtTimeZone;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.BinaryLiteral;
import io.trino.sql.tree.BindExpression;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.Call;
import io.trino.sql.tree.CallArgument;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.CharLiteral;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.ColumnDefinition;
import io.trino.sql.tree.Comment;
import io.trino.sql.tree.Commit;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.CreateMaterializedView;
import io.trino.sql.tree.CreateRole;
import io.trino.sql.tree.CreateSchema;
import io.trino.sql.tree.CreateTable;
import io.trino.sql.tree.CreateTableAsSelect;
import io.trino.sql.tree.CreateView;
import io.trino.sql.tree.Cube;
import io.trino.sql.tree.CurrentCatalog;
import io.trino.sql.tree.CurrentPath;
import io.trino.sql.tree.CurrentSchema;
import io.trino.sql.tree.CurrentTime;
import io.trino.sql.tree.CurrentUser;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.DataTypeParameter;
import io.trino.sql.tree.DateTimeDataType;
import io.trino.sql.tree.Deallocate;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Delete;
import io.trino.sql.tree.Deny;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.DescribeInput;
import io.trino.sql.tree.DescribeOutput;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.DropColumn;
import io.trino.sql.tree.DropMaterializedView;
import io.trino.sql.tree.DropRole;
import io.trino.sql.tree.DropSchema;
import io.trino.sql.tree.DropTable;
import io.trino.sql.tree.DropView;
import io.trino.sql.tree.EmptyPattern;
import io.trino.sql.tree.Except;
import io.trino.sql.tree.ExcludedPattern;
import io.trino.sql.tree.Execute;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Explain;
import io.trino.sql.tree.ExplainAnalyze;
import io.trino.sql.tree.ExplainFormat;
import io.trino.sql.tree.ExplainOption;
import io.trino.sql.tree.ExplainType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Extract;
import io.trino.sql.tree.FetchFirst;
import io.trino.sql.tree.Format;
import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.FunctionCall.NullTreatment;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Grant;
import io.trino.sql.tree.GrantOnType;
import io.trino.sql.tree.GrantRoles;
import io.trino.sql.tree.GrantorSpecification;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.GroupingOperation;
import io.trino.sql.tree.GroupingSets;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.Insert;
import io.trino.sql.tree.Intersect;
import io.trino.sql.tree.IntervalDayTimeDataType;
import io.trino.sql.tree.IntervalLiteral;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Isolation;
import io.trino.sql.tree.Join;
import io.trino.sql.tree.JoinCriteria;
import io.trino.sql.tree.JoinOn;
import io.trino.sql.tree.JoinUsing;
import io.trino.sql.tree.LambdaArgumentDeclaration;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.Lateral;
import io.trino.sql.tree.LikeClause;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Limit;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.Merge;
import io.trino.sql.tree.MergeCase;
import io.trino.sql.tree.MergeDelete;
import io.trino.sql.tree.MergeInsert;
import io.trino.sql.tree.MergeUpdate;
import io.trino.sql.tree.NaturalJoin;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.NodeLocation;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.NullIfExpression;
import io.trino.sql.tree.NullLiteral;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.Offset;
import io.trino.sql.tree.OneOrMoreQuantifier;
import io.trino.sql.tree.OrderBy;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.PathElement;
import io.trino.sql.tree.PathSpecification;
import io.trino.sql.tree.PatternAlternation;
import io.trino.sql.tree.PatternConcatenation;
import io.trino.sql.tree.PatternPermutation;
import io.trino.sql.tree.PatternQuantifier;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.PatternSearchMode;
import io.trino.sql.tree.PatternVariable;
import io.trino.sql.tree.Prepare;
import io.trino.sql.tree.PrincipalSpecification;
import io.trino.sql.tree.ProcessingMode;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.QuantifiedPattern;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QueryBody;
import io.trino.sql.tree.QueryPeriod;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.RefreshMaterializedView;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.RenameColumn;
import io.trino.sql.tree.RenameMaterializedView;
import io.trino.sql.tree.RenameSchema;
import io.trino.sql.tree.RenameTable;
import io.trino.sql.tree.RenameView;
import io.trino.sql.tree.ResetSession;
import io.trino.sql.tree.Revoke;
import io.trino.sql.tree.RevokeRoles;
import io.trino.sql.tree.Rollback;
import io.trino.sql.tree.Rollup;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.RowDataType;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SampledRelation;
import io.trino.sql.tree.SearchedCaseExpression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
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
import io.trino.sql.tree.SimpleCaseExpression;
import io.trino.sql.tree.SimpleGroupBy;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SortItem;
import io.trino.sql.tree.StartTransaction;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SubqueryExpression;
import io.trino.sql.tree.SubscriptExpression;
import io.trino.sql.tree.SubsetDefinition;
import io.trino.sql.tree.Table;
import io.trino.sql.tree.TableElement;
import io.trino.sql.tree.TableExecute;
import io.trino.sql.tree.TableSubquery;
import io.trino.sql.tree.TimeLiteral;
import io.trino.sql.tree.TimestampLiteral;
import io.trino.sql.tree.TransactionAccessMode;
import io.trino.sql.tree.TransactionMode;
import io.trino.sql.tree.TruncateTable;
import io.trino.sql.tree.TryExpression;
import io.trino.sql.tree.TypeParameter;
import io.trino.sql.tree.Union;
import io.trino.sql.tree.Unnest;
import io.trino.sql.tree.Update;
import io.trino.sql.tree.UpdateAssignment;
import io.trino.sql.tree.Use;
import io.trino.sql.tree.Values;
import io.trino.sql.tree.VariableDefinition;
import io.trino.sql.tree.WhenClause;
import io.trino.sql.tree.Window;
import io.trino.sql.tree.WindowDefinition;
import io.trino.sql.tree.WindowFrame;
import io.trino.sql.tree.WindowOperation;
import io.trino.sql.tree.WindowReference;
import io.trino.sql.tree.WindowSpecification;
import io.trino.sql.tree.With;
import io.trino.sql.tree.WithQuery;
import io.trino.sql.tree.ZeroOrMoreQuantifier;
import io.trino.sql.tree.ZeroOrOneQuantifier;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.sql.parser.SqlBaseParser.TIME;
import static io.trino.sql.parser.SqlBaseParser.TIMESTAMP;
import static io.trino.sql.tree.AnchorPattern.Type.PARTITION_END;
import static io.trino.sql.tree.AnchorPattern.Type.PARTITION_START;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ALL_OMIT_EMPTY;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ALL_SHOW_EMPTY;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ALL_WITH_UNMATCHED;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.PatternSearchMode.Mode.INITIAL;
import static io.trino.sql.tree.PatternSearchMode.Mode.SEEK;
import static io.trino.sql.tree.ProcessingMode.Mode.FINAL;
import static io.trino.sql.tree.ProcessingMode.Mode.RUNNING;
import static io.trino.sql.tree.SkipTo.skipPastLastRow;
import static io.trino.sql.tree.SkipTo.skipToFirst;
import static io.trino.sql.tree.SkipTo.skipToLast;
import static io.trino.sql.tree.SkipTo.skipToNextRow;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class AstBuilder
        extends SqlBaseBaseVisitor<Node>
{
    private int parameterPosition;
    private final ParsingOptions parsingOptions;

    AstBuilder(ParsingOptions parsingOptions)
    {
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context)
    {
        return visit(context.statement());
    }

    @Override
    public Node visitStandaloneExpression(SqlBaseParser.StandaloneExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Node visitStandaloneType(SqlBaseParser.StandaloneTypeContext context)
    {
        return visit(context.type());
    }

    @Override
    public Node visitStandalonePathSpecification(SqlBaseParser.StandalonePathSpecificationContext context)
    {
        return visit(context.pathSpecification());
    }

    @Override
    public Node visitStandaloneRowPattern(SqlBaseParser.StandaloneRowPatternContext context)
    {
        return visit(context.rowPattern());
    }

    // ******************* statements **********************

    @Override
    public Node visitUse(SqlBaseParser.UseContext context)
    {
        return new Use(
                getLocation(context),
                visitIfPresent(context.catalog, Identifier.class),
                (Identifier) visit(context.schema));
    }

    @Override
    public Node visitCreateSchema(SqlBaseParser.CreateSchemaContext context)
    {
        Optional<PrincipalSpecification> principal = Optional.empty();
        if (context.AUTHORIZATION() != null) {
            principal = Optional.of(getPrincipalSpecification(context.principal()));
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }

        return new CreateSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                properties,
                principal);
    }

    @Override
    public Node visitDropSchema(SqlBaseParser.DropSchemaContext context)
    {
        return new DropSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                context.EXISTS() != null,
                context.CASCADE() != null);
    }

    @Override
    public Node visitRenameSchema(SqlBaseParser.RenameSchemaContext context)
    {
        return new RenameSchema(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitSetSchemaAuthorization(SqlBaseParser.SetSchemaAuthorizationContext context)
    {
        return new SetSchemaAuthorization(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.principal()));
    }

    @Override
    public Node visitCreateTableAsSelect(SqlBaseParser.CreateTableAsSelectContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        Optional<List<Identifier>> columnAliases = Optional.empty();
        if (context.columnAliases() != null) {
            columnAliases = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }

        return new CreateTableAsSelect(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.EXISTS() != null,
                properties,
                context.NO() == null,
                columnAliases,
                comment);
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }
        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }
        return new CreateTable(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                visit(context.tableElement(), TableElement.class),
                context.EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitCreateMaterializedView(SqlBaseParser.CreateMaterializedViewContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }

        return new CreateMaterializedView(
                Optional.of(getLocation(context)),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.REPLACE() != null,
                context.EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitRefreshMaterializedView(SqlBaseParser.RefreshMaterializedViewContext context)
    {
        return new RefreshMaterializedView(
                Optional.of(getLocation(context)),
                new Table(getQualifiedName(context.qualifiedName())));
    }

    @Override
    public Node visitDropMaterializedView(SqlBaseParser.DropMaterializedViewContext context)
    {
        return new DropMaterializedView(
                getLocation(context), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.TABLE, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitDropTable(SqlBaseParser.DropTableContext context)
    {
        return new DropTable(getLocation(context), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitDropView(SqlBaseParser.DropViewContext context)
    {
        return new DropView(getLocation(context), getQualifiedName(context.qualifiedName()), context.EXISTS() != null);
    }

    @Override
    public Node visitInsertInto(SqlBaseParser.InsertIntoContext context)
    {
        Optional<List<Identifier>> columnAliases = Optional.empty();
        if (context.columnAliases() != null) {
            columnAliases = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new Insert(
                new Table(getQualifiedName(context.qualifiedName())),
                columnAliases,
                (Query) visit(context.query()));
    }

    @Override
    public Node visitDelete(SqlBaseParser.DeleteContext context)
    {
        return new Delete(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitUpdate(SqlBaseParser.UpdateContext context)
    {
        return new Update(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visit(context.updateAssignment(), UpdateAssignment.class),
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitUpdateAssignment(SqlBaseParser.UpdateAssignmentContext context)
    {
        return new UpdateAssignment((Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitTruncateTable(SqlBaseParser.TruncateTableContext context)
    {
        return new TruncateTable(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitMerge(SqlBaseParser.MergeContext context)
    {
        return new Merge(
                getLocation(context),
                new Table(getLocation(context), getQualifiedName(context.qualifiedName())),
                visitIfPresent(context.identifier(), Identifier.class),
                (Relation) visit(context.relation()),
                (Expression) visit(context.expression()),
                visit(context.mergeCase(), MergeCase.class));
    }

    @Override
    public Node visitMergeInsert(SqlBaseParser.MergeInsertContext context)
    {
        return new MergeInsert(
                getLocation(context),
                visitIfPresent(context.condition, Expression.class),
                visitIdentifiers(context.targets),
                visit(context.values, Expression.class));
    }

    private List<Identifier> visitIdentifiers(List<SqlBaseParser.IdentifierContext> identifiers)
    {
        return identifiers.stream()
                .map(identifier -> (Identifier) visit(identifier))
                .collect(toImmutableList());
    }

    @Override
    public Node visitMergeUpdate(SqlBaseParser.MergeUpdateContext context)
    {
        ImmutableList.Builder<MergeUpdate.Assignment> assignments = ImmutableList.builder();
        for (int i = 0; i < context.targets.size(); i++) {
            assignments.add(new MergeUpdate.Assignment(
                    (Identifier) visit(context.targets.get(i)),
                    (Expression) visit(context.values.get(i))));
        }

        return new MergeUpdate(getLocation(context), visitIfPresent(context.condition, Expression.class), assignments.build());
    }

    @Override
    public Node visitMergeDelete(SqlBaseParser.MergeDeleteContext context)
    {
        return new MergeDelete(getLocation(context), visitIfPresent(context.condition, Expression.class));
    }

    @Override
    public Node visitRenameTable(SqlBaseParser.RenameTableContext context)
    {
        return new RenameTable(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to), context.EXISTS() != null);
    }

    @Override
    public Node visitSetTableProperties(SqlBaseParser.SetTablePropertiesContext context)
    {
        List<Property> properties = ImmutableList.of();
        if (context.propertyAssignments() != null) {
            properties = visit(context.propertyAssignments().property(), Property.class);
        }

        return new SetProperties(getLocation(context), SetProperties.Type.TABLE, getQualifiedName(context.qualifiedName()), properties);
    }

    @Override
    public Node visitCommentTable(SqlBaseParser.CommentTableContext context)
    {
        Optional<String> comment = Optional.empty();

        if (context.string() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        return new Comment(getLocation(context), Comment.Type.TABLE, getQualifiedName(context.qualifiedName()), comment);
    }

    @Override
    public Node visitCommentColumn(SqlBaseParser.CommentColumnContext context)
    {
        Optional<String> comment = Optional.empty();

        if (context.string() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        return new Comment(getLocation(context), Comment.Type.COLUMN, getQualifiedName(context.qualifiedName()), comment);
    }

    @Override
    public Node visitRenameColumn(SqlBaseParser.RenameColumnContext context)
    {
        return new RenameColumn(
                getLocation(context),
                getQualifiedName(context.tableName),
                (Identifier) visit(context.from),
                (Identifier) visit(context.to),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() < context.COLUMN().getSymbol().getTokenIndex()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() > context.COLUMN().getSymbol().getTokenIndex()));
    }

    @Override
    public Node visitAnalyze(SqlBaseParser.AnalyzeContext context)
    {
        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }
        return new Analyze(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                properties);
    }

    @Override
    public Node visitAddColumn(SqlBaseParser.AddColumnContext context)
    {
        return new AddColumn(getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (ColumnDefinition) visit(context.columnDefinition()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() < context.COLUMN().getSymbol().getTokenIndex()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() > context.COLUMN().getSymbol().getTokenIndex()));
    }

    @Override
    public Node visitSetTableAuthorization(SqlBaseParser.SetTableAuthorizationContext context)
    {
        return new SetTableAuthorization(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.principal()));
    }

    @Override
    public Node visitDropColumn(SqlBaseParser.DropColumnContext context)
    {
        return new DropColumn(getLocation(context),
                getQualifiedName(context.tableName),
                (Identifier) visit(context.column),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() < context.COLUMN().getSymbol().getTokenIndex()),
                context.EXISTS().stream().anyMatch(node -> node.getSymbol().getTokenIndex() > context.COLUMN().getSymbol().getTokenIndex()));
    }

    @Override
    public Node visitTableExecute(SqlBaseParser.TableExecuteContext context)
    {
        List<CallArgument> arguments = ImmutableList.of();
        if (context.callArgument() != null) {
            arguments = this.visit(context.callArgument(), CallArgument.class);
        }

        return new TableExecute(
                new Table(getLocation(context), getQualifiedName(context.tableName)),
                (Identifier) visit(context.procedureName),
                arguments,
                visitIfPresent(context.booleanExpression(), Expression.class));
    }

    @Override
    public Node visitCreateView(SqlBaseParser.CreateViewContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        Optional<CreateView.Security> security = Optional.empty();
        if (context.DEFINER() != null) {
            security = Optional.of(CreateView.Security.DEFINER);
        }
        else if (context.INVOKER() != null) {
            security = Optional.of(CreateView.Security.INVOKER);
        }

        return new CreateView(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                (Query) visit(context.query()),
                context.REPLACE() != null,
                comment,
                security);
    }

    @Override
    public Node visitRenameView(SqlBaseParser.RenameViewContext context)
    {
        return new RenameView(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to));
    }

    @Override
    public Node visitRenameMaterializedView(SqlBaseParser.RenameMaterializedViewContext context)
    {
        return new RenameMaterializedView(getLocation(context), getQualifiedName(context.from), getQualifiedName(context.to), context.EXISTS() != null);
    }

    @Override
    public Node visitSetViewAuthorization(SqlBaseParser.SetViewAuthorizationContext context)
    {
        return new SetViewAuthorization(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.principal()));
    }

    @Override
    public Node visitSetMaterializedViewProperties(SqlBaseParser.SetMaterializedViewPropertiesContext context)
    {
        return new SetProperties(
                getLocation(context),
                SetProperties.Type.MATERIALIZED_VIEW,
                getQualifiedName(context.qualifiedName()),
                visit(context.propertyAssignments().property(), Property.class));
    }

    @Override
    public Node visitStartTransaction(SqlBaseParser.StartTransactionContext context)
    {
        return new StartTransaction(visit(context.transactionMode(), TransactionMode.class));
    }

    @Override
    public Node visitCommit(SqlBaseParser.CommitContext context)
    {
        return new Commit(getLocation(context));
    }

    @Override
    public Node visitRollback(SqlBaseParser.RollbackContext context)
    {
        return new Rollback(getLocation(context));
    }

    @Override
    public Node visitTransactionAccessMode(SqlBaseParser.TransactionAccessModeContext context)
    {
        return new TransactionAccessMode(getLocation(context), context.accessMode.getType() == SqlBaseLexer.ONLY);
    }

    @Override
    public Node visitIsolationLevel(SqlBaseParser.IsolationLevelContext context)
    {
        return visit(context.levelOfIsolation());
    }

    @Override
    public Node visitReadUncommitted(SqlBaseParser.ReadUncommittedContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.READ_UNCOMMITTED);
    }

    @Override
    public Node visitReadCommitted(SqlBaseParser.ReadCommittedContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.READ_COMMITTED);
    }

    @Override
    public Node visitRepeatableRead(SqlBaseParser.RepeatableReadContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.REPEATABLE_READ);
    }

    @Override
    public Node visitSerializable(SqlBaseParser.SerializableContext context)
    {
        return new Isolation(getLocation(context), Isolation.Level.SERIALIZABLE);
    }

    @Override
    public Node visitCall(SqlBaseParser.CallContext context)
    {
        return new Call(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                visit(context.callArgument(), CallArgument.class));
    }

    @Override
    public Node visitPrepare(SqlBaseParser.PrepareContext context)
    {
        return new Prepare(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                (Statement) visit(context.statement()));
    }

    @Override
    public Node visitDeallocate(SqlBaseParser.DeallocateContext context)
    {
        return new Deallocate(
                getLocation(context),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitExecute(SqlBaseParser.ExecuteContext context)
    {
        return new Execute(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitDescribeOutput(SqlBaseParser.DescribeOutputContext context)
    {
        return new DescribeOutput(
                getLocation(context),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitDescribeInput(SqlBaseParser.DescribeInputContext context)
    {
        return new DescribeInput(
                getLocation(context),
                (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitProperty(SqlBaseParser.PropertyContext context)
    {
        NodeLocation location = getLocation(context);
        Identifier name = (Identifier) visit(context.identifier());
        SqlBaseParser.PropertyValueContext valueContext = context.propertyValue();
        if (valueContext instanceof SqlBaseParser.DefaultPropertyValueContext) {
            return new Property(location, name);
        }
        Expression value = (Expression) visit(((SqlBaseParser.NonDefaultPropertyValueContext) valueContext).expression());
        return new Property(location, name, value);
    }

    // ********************** query expressions ********************

    @Override
    public Node visitQuery(SqlBaseParser.QueryContext context)
    {
        Query body = (Query) visit(context.queryNoWith());

        return new Query(
                getLocation(context),
                visitIfPresent(context.with(), With.class),
                body.getQueryBody(),
                body.getOrderBy(),
                body.getOffset(),
                body.getLimit());
    }

    @Override
    public Node visitWith(SqlBaseParser.WithContext context)
    {
        return new With(getLocation(context), context.RECURSIVE() != null, visit(context.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitNamedQuery(SqlBaseParser.NamedQueryContext context)
    {
        Optional<List<Identifier>> columns = Optional.empty();
        if (context.columnAliases() != null) {
            columns = Optional.of(visit(context.columnAliases().identifier(), Identifier.class));
        }

        return new WithQuery(
                getLocation(context),
                (Identifier) visit(context.name),
                (Query) visit(context.query()),
                columns);
    }

    @Override
    public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context)
    {
        QueryBody term = (QueryBody) visit(context.queryTerm());

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        Optional<Offset> offset = Optional.empty();
        if (context.OFFSET() != null) {
            Expression rowCount;
            if (context.offset.INTEGER_VALUE() != null) {
                rowCount = new LongLiteral(getLocation(context.offset.INTEGER_VALUE()), context.offset.getText());
            }
            else {
                rowCount = new Parameter(getLocation(context.offset.QUESTION_MARK()), parameterPosition);
                parameterPosition++;
            }
            offset = Optional.of(new Offset(Optional.of(getLocation(context.OFFSET())), rowCount));
        }

        Optional<Node> limit = Optional.empty();
        if (context.FETCH() != null) {
            Optional<Expression> rowCount = Optional.empty();
            if (context.fetchFirst != null) {
                if (context.fetchFirst.INTEGER_VALUE() != null) {
                    rowCount = Optional.of(new LongLiteral(getLocation(context.fetchFirst.INTEGER_VALUE()), context.fetchFirst.getText()));
                }
                else {
                    rowCount = Optional.of(new Parameter(getLocation(context.fetchFirst.QUESTION_MARK()), parameterPosition));
                    parameterPosition++;
                }
            }
            limit = Optional.of(new FetchFirst(Optional.of(getLocation(context.FETCH())), rowCount, context.TIES() != null));
        }
        else if (context.LIMIT() != null) {
            if (context.limit == null) {
                throw new IllegalStateException("Missing LIMIT value");
            }
            Expression rowCount;
            if (context.limit.ALL() != null) {
                rowCount = new AllRows(getLocation(context.limit.ALL()));
            }
            else if (context.limit.rowCount().INTEGER_VALUE() != null) {
                rowCount = new LongLiteral(getLocation(context.limit.rowCount().INTEGER_VALUE()), context.limit.getText());
            }
            else {
                rowCount = new Parameter(getLocation(context.limit.rowCount().QUESTION_MARK()), parameterPosition);
                parameterPosition++;
            }

            limit = Optional.of(new Limit(Optional.of(getLocation(context.LIMIT())), rowCount));
        }

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by, offset, limit or fetch,
            // fold the order by, limit, offset or fetch clauses
            // into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification) term;

            return new Query(
                    getLocation(context),
                    Optional.empty(),
                    new QuerySpecification(
                            getLocation(context),
                            query.getSelect(),
                            query.getFrom(),
                            query.getWhere(),
                            query.getGroupBy(),
                            query.getHaving(),
                            query.getWindows(),
                            orderBy,
                            offset,
                            limit),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }

        return new Query(
                getLocation(context),
                Optional.empty(),
                term,
                orderBy,
                offset,
                limit);
    }

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context)
    {
        Optional<Relation> from = Optional.empty();
        List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

        List<Relation> relations = visit(context.relation(), Relation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<Relation> iterator = relations.iterator();
            Relation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(), Optional.empty());
            }

            from = Optional.of(relation);
        }

        return new QuerySpecification(
                getLocation(context),
                new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
                from,
                visitIfPresent(context.where, Expression.class),
                visitIfPresent(context.groupBy(), GroupBy.class),
                visitIfPresent(context.having, Expression.class),
                visit(context.windowDefinition(), WindowDefinition.class),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    @Override
    public Node visitGroupBy(SqlBaseParser.GroupByContext context)
    {
        return new GroupBy(getLocation(context), isDistinct(context.setQuantifier()), visit(context.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context)
    {
        return new SimpleGroupBy(getLocation(context), visit(context.groupingSet().expression(), Expression.class));
    }

    @Override
    public Node visitRollup(SqlBaseParser.RollupContext context)
    {
        return new Rollup(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCube(SqlBaseParser.CubeContext context)
    {
        return new Cube(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitMultipleGroupingSets(SqlBaseParser.MultipleGroupingSetsContext context)
    {
        return new GroupingSets(getLocation(context), context.groupingSet().stream()
                .map(groupingSet -> visit(groupingSet.expression(), Expression.class))
                .collect(toList()));
    }

    @Override
    public Node visitWindowSpecification(SqlBaseParser.WindowSpecificationContext context)
    {
        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        return new WindowSpecification(
                getLocation(context),
                visitIfPresent(context.existingWindowName, Identifier.class),
                visit(context.partition, Expression.class),
                orderBy,
                visitIfPresent(context.windowFrame(), WindowFrame.class));
    }

    @Override
    public Node visitWindowDefinition(SqlBaseParser.WindowDefinitionContext context)
    {
        return new WindowDefinition(
                getLocation(context),
                (Identifier) visit(context.name),
                (WindowSpecification) visit(context.windowSpecification()));
    }

    @Override
    public Node visitSetOperation(SqlBaseParser.SetOperationContext context)
    {
        QueryBody left = (QueryBody) visit(context.left);
        QueryBody right = (QueryBody) visit(context.right);

        boolean distinct = context.setQuantifier() == null || context.setQuantifier().DISTINCT() != null;

        switch (context.operator.getType()) {
            case SqlBaseLexer.UNION:
                return new Union(getLocation(context.UNION()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.INTERSECT:
                return new Intersect(getLocation(context.INTERSECT()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.EXCEPT:
                return new Except(getLocation(context.EXCEPT()), left, right, distinct);
        }

        throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
    }

    @Override
    public Node visitSelectAll(SqlBaseParser.SelectAllContext context)
    {
        List<Identifier> aliases = ImmutableList.of();
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AllColumns(
                getLocation(context),
                visitIfPresent(context.primaryExpression(), Expression.class),
                aliases);
    }

    @Override
    public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context)
    {
        return new SingleColumn(
                getLocation(context),
                (Expression) visit(context.expression()),
                visitIfPresent(context.identifier(), Identifier.class));
    }

    @Override
    public Node visitTable(SqlBaseParser.TableContext context)
    {
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubquery(SqlBaseParser.SubqueryContext context)
    {
        return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
    }

    @Override
    public Node visitInlineTable(SqlBaseParser.InlineTableContext context)
    {
        return new Values(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext context)
    {
        return new Explain(getLocation(context), (Statement) visit(context.statement()), visit(context.explainOption(), ExplainOption.class));
    }

    @Override
    public Node visitExplainAnalyze(SqlBaseParser.ExplainAnalyzeContext context)
    {
        return new ExplainAnalyze(getLocation(context), context.VERBOSE() != null, (Statement) visit(context.statement()));
    }

    @Override
    public Node visitExplainFormat(SqlBaseParser.ExplainFormatContext context)
    {
        switch (context.value.getType()) {
            case SqlBaseLexer.GRAPHVIZ:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.GRAPHVIZ);
            case SqlBaseLexer.TEXT:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.TEXT);
            case SqlBaseLexer.JSON:
                return new ExplainFormat(getLocation(context), ExplainFormat.Type.JSON);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN format: " + context.value.getText());
    }

    @Override
    public Node visitExplainType(SqlBaseParser.ExplainTypeContext context)
    {
        switch (context.value.getType()) {
            case SqlBaseLexer.LOGICAL:
                return new ExplainType(getLocation(context), ExplainType.Type.LOGICAL);
            case SqlBaseLexer.DISTRIBUTED:
                return new ExplainType(getLocation(context), ExplainType.Type.DISTRIBUTED);
            case SqlBaseLexer.VALIDATE:
                return new ExplainType(getLocation(context), ExplainType.Type.VALIDATE);
            case SqlBaseLexer.IO:
                return new ExplainType(getLocation(context), ExplainType.Type.IO);
        }

        throw new IllegalArgumentException("Unsupported EXPLAIN type: " + context.value.getText());
    }

    @Override
    public Node visitShowTables(SqlBaseParser.ShowTablesContext context)
    {
        return new ShowTables(
                getLocation(context),
                Optional.ofNullable(context.qualifiedName())
                        .map(this::getQualifiedName),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowSchemas(SqlBaseParser.ShowSchemasContext context)
    {
        return new ShowSchemas(
                getLocation(context),
                visitIfPresent(context.identifier(), Identifier.class),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowCatalogs(SqlBaseParser.ShowCatalogsContext context)
    {
        return new ShowCatalogs(getLocation(context),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowColumns(SqlBaseParser.ShowColumnsContext context)
    {
        return new ShowColumns(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowStats(SqlBaseParser.ShowStatsContext context)
    {
        return new ShowStats(Optional.of(getLocation(context)), new Table(getQualifiedName(context.qualifiedName())));
    }

    @Override
    public Node visitShowStatsForQuery(SqlBaseParser.ShowStatsForQueryContext context)
    {
        Query query = (Query) visit(context.query());
        return new ShowStats(Optional.of(getLocation(context)), new TableSubquery(query));
    }

    @Override
    public Node visitShowCreateSchema(SqlBaseParser.ShowCreateSchemaContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.SCHEMA, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowCreateView(SqlBaseParser.ShowCreateViewContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.VIEW, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowCreateMaterializedView(SqlBaseParser.ShowCreateMaterializedViewContext context)
    {
        return new ShowCreate(getLocation(context), ShowCreate.Type.MATERIALIZED_VIEW, getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitShowFunctions(SqlBaseParser.ShowFunctionsContext context)
    {
        return new ShowFunctions(getLocation(context),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitShowSession(SqlBaseParser.ShowSessionContext context)
    {
        return new ShowSession(getLocation(context),
                getTextIfPresent(context.pattern)
                        .map(AstBuilder::unquote),
                getTextIfPresent(context.escape)
                        .map(AstBuilder::unquote));
    }

    @Override
    public Node visitSetSession(SqlBaseParser.SetSessionContext context)
    {
        return new SetSession(getLocation(context), getQualifiedName(context.qualifiedName()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitResetSession(SqlBaseParser.ResetSessionContext context)
    {
        return new ResetSession(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitCreateRole(SqlBaseParser.CreateRoleContext context)
    {
        return new CreateRole(
                getLocation(context),
                (Identifier) visit(context.name),
                getGrantorSpecificationIfPresent(context.grantor()),
                visitIfPresent(context.catalog, Identifier.class));
    }

    @Override
    public Node visitDropRole(SqlBaseParser.DropRoleContext context)
    {
        return new DropRole(
                getLocation(context),
                (Identifier) visit(context.name),
                visitIfPresent(context.catalog, Identifier.class));
    }

    @Override
    public Node visitGrantRoles(SqlBaseParser.GrantRolesContext context)
    {
        return new GrantRoles(
                getLocation(context),
                ImmutableSet.copyOf(getIdentifiers(context.roles().identifier())),
                ImmutableSet.copyOf(getPrincipalSpecifications(context.principal())),
                context.OPTION() != null,
                getGrantorSpecificationIfPresent(context.grantor()),
                visitIfPresent(context.catalog, Identifier.class));
    }

    @Override
    public Node visitRevokeRoles(SqlBaseParser.RevokeRolesContext context)
    {
        return new RevokeRoles(
                getLocation(context),
                ImmutableSet.copyOf(getIdentifiers(context.roles().identifier())),
                ImmutableSet.copyOf(getPrincipalSpecifications(context.principal())),
                context.OPTION() != null,
                getGrantorSpecificationIfPresent(context.grantor()),
                visitIfPresent(context.catalog, Identifier.class));
    }

    @Override
    public Node visitSetRole(SqlBaseParser.SetRoleContext context)
    {
        SetRole.Type type = SetRole.Type.ROLE;
        if (context.ALL() != null) {
            type = SetRole.Type.ALL;
        }
        else if (context.NONE() != null) {
            type = SetRole.Type.NONE;
        }
        return new SetRole(
                getLocation(context),
                type,
                getIdentifierIfPresent(context.role),
                visitIfPresent(context.catalog, Identifier.class));
    }

    @Override
    public Node visitGrant(SqlBaseParser.GrantContext context)
    {
        Optional<List<String>> privileges;
        if (context.ALL() != null) {
            privileges = Optional.empty();
        }
        else {
            privileges = Optional.of(context.privilege().stream()
                    .map(SqlBaseParser.PrivilegeContext::getText)
                    .collect(toList()));
        }

        Optional<GrantOnType> type;
        if (context.SCHEMA() != null) {
            type = Optional.of(GrantOnType.SCHEMA);
        }
        else if (context.TABLE() != null) {
            type = Optional.of(GrantOnType.TABLE);
        }
        else {
            type = Optional.empty();
        }

        return new Grant(
                getLocation(context),
                privileges,
                type,
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.grantee),
                context.OPTION() != null);
    }

    @Override
    public Node visitDeny(SqlBaseParser.DenyContext context)
    {
        Optional<List<String>> privileges;
        if (context.ALL() != null) {
            privileges = Optional.empty();
        }
        else {
            privileges = Optional.of(context.privilege().stream()
                    .map(SqlBaseParser.PrivilegeContext::getText)
                    .collect(toList()));
        }

        Optional<GrantOnType> type;
        if (context.SCHEMA() != null) {
            type = Optional.of(GrantOnType.SCHEMA);
        }
        else if (context.TABLE() != null) {
            type = Optional.of(GrantOnType.TABLE);
        }
        else {
            type = Optional.empty();
        }

        return new Deny(
                getLocation(context),
                privileges,
                type,
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.grantee));
    }

    @Override
    public Node visitRevoke(SqlBaseParser.RevokeContext context)
    {
        Optional<List<String>> privileges;
        if (context.ALL() != null) {
            privileges = Optional.empty();
        }
        else {
            privileges = Optional.of(context.privilege().stream()
                    .map(SqlBaseParser.PrivilegeContext::getText)
                    .collect(toList()));
        }

        Optional<GrantOnType> type;
        if (context.SCHEMA() != null) {
            type = Optional.of(GrantOnType.SCHEMA);
        }
        else if (context.TABLE() != null) {
            type = Optional.of(GrantOnType.TABLE);
        }
        else {
            type = Optional.empty();
        }

        return new Revoke(
                getLocation(context),
                context.OPTION() != null,
                privileges,
                type,
                getQualifiedName(context.qualifiedName()),
                getPrincipalSpecification(context.grantee));
    }

    @Override
    public Node visitShowGrants(SqlBaseParser.ShowGrantsContext context)
    {
        Optional<QualifiedName> tableName = Optional.empty();

        if (context.qualifiedName() != null) {
            tableName = Optional.of(getQualifiedName(context.qualifiedName()));
        }

        return new ShowGrants(
                getLocation(context),
                context.TABLE() != null,
                tableName);
    }

    @Override
    public Node visitShowRoles(SqlBaseParser.ShowRolesContext context)
    {
        return new ShowRoles(
                getLocation(context),
                getIdentifierIfPresent(context.identifier()),
                context.CURRENT() != null);
    }

    @Override
    public Node visitShowRoleGrants(SqlBaseParser.ShowRoleGrantsContext context)
    {
        return new ShowRoleGrants(
                getLocation(context),
                getIdentifierIfPresent(context.identifier()));
    }

    @Override
    public Node visitSetPath(SqlBaseParser.SetPathContext context)
    {
        return new SetPath(getLocation(context), (PathSpecification) visit(context.pathSpecification()));
    }

    @Override
    public Node visitSetTimeZone(SqlBaseParser.SetTimeZoneContext context)
    {
        Optional<Expression> timeZone = Optional.empty();
        if (context.expression() != null) {
            timeZone = Optional.of((Expression) visit(context.expression()));
        }
        return new SetTimeZone(getLocation(context), timeZone);
    }

    // ***************** boolean expressions ******************

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context)
    {
        return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
    }

    @Override
    public Node visitOr(SqlBaseParser.OrContext context)
    {
        List<ParserRuleContext> terms = flatten(context, element -> {
            if (element instanceof SqlBaseParser.OrContext) {
                SqlBaseParser.OrContext or = (SqlBaseParser.OrContext) element;
                return Optional.of(or.booleanExpression());
            }

            return Optional.empty();
        });

        return new LogicalExpression(getLocation(context), LogicalExpression.Operator.OR, visit(terms, Expression.class));
    }

    @Override
    public Node visitAnd(SqlBaseParser.AndContext context)
    {
        List<ParserRuleContext> terms = flatten(context, element -> {
            if (element instanceof SqlBaseParser.AndContext) {
                SqlBaseParser.AndContext and = (SqlBaseParser.AndContext) element;
                return Optional.of(and.booleanExpression());
            }

            return Optional.empty();
        });

        return new LogicalExpression(getLocation(context), LogicalExpression.Operator.AND, visit(terms, Expression.class));
    }

    private static List<ParserRuleContext> flatten(ParserRuleContext root, Function<ParserRuleContext, Optional<List<? extends ParserRuleContext>>> extractChildren)
    {
        List<ParserRuleContext> result = new ArrayList<>();
        Deque<ParserRuleContext> pending = new ArrayDeque<>();
        pending.push(root);

        while (!pending.isEmpty()) {
            ParserRuleContext next = pending.pop();

            Optional<List<? extends ParserRuleContext>> children = extractChildren.apply(next);
            if (!children.isPresent()) {
                result.add(next);
            }
            else {
                for (int i = children.get().size() - 1; i >= 0; i--) {
                    pending.push(children.get().get(i));
                }
            }
        }

        return result;
    }

    // *************** from clause *****************

    @Override
    public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context)
    {
        Relation left = (Relation) visit(context.left);
        Relation right;

        if (context.CROSS() != null) {
            right = (Relation) visit(context.right);
            return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.empty());
        }

        JoinCriteria criteria;
        if (context.NATURAL() != null) {
            right = (Relation) visit(context.right);
            criteria = new NaturalJoin();
        }
        else {
            right = (Relation) visit(context.rightRelation);
            if (context.joinCriteria().ON() != null) {
                criteria = new JoinOn((Expression) visit(context.joinCriteria().booleanExpression()));
            }
            else if (context.joinCriteria().USING() != null) {
                criteria = new JoinUsing(visit(context.joinCriteria().identifier(), Identifier.class));
            }
            else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        Join.Type joinType;
        if (context.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        }
        else if (context.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        }
        else if (context.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        }
        else {
            joinType = Join.Type.INNER;
        }

        return new Join(getLocation(context), joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitSampledRelation(SqlBaseParser.SampledRelationContext context)
    {
        Relation child = (Relation) visit(context.patternRecognition());

        if (context.TABLESAMPLE() == null) {
            return child;
        }

        return new SampledRelation(
                getLocation(context),
                child,
                getSamplingMethod((Token) context.sampleType().getChild(0).getPayload()),
                (Expression) visit(context.percentage));
    }

    @Override
    public Node visitPatternRecognition(SqlBaseParser.PatternRecognitionContext context)
    {
        Relation child = (Relation) visit(context.aliasedRelation());

        if (context.MATCH_RECOGNIZE() == null) {
            return child;
        }

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
        }

        Optional<PatternSearchMode> searchMode = Optional.empty();
        if (context.INITIAL() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.INITIAL()), INITIAL));
        }
        else if (context.SEEK() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.SEEK()), SEEK));
        }

        PatternRecognitionRelation relation = new PatternRecognitionRelation(
                getLocation(context),
                child,
                visit(context.partition, Expression.class),
                orderBy,
                visit(context.measureDefinition(), MeasureDefinition.class),
                getRowsPerMatch(context.rowsPerMatch()),
                visitIfPresent(context.skipTo(), SkipTo.class),
                searchMode,
                (RowPattern) visit(context.rowPattern()),
                visit(context.subsetDefinition(), SubsetDefinition.class),
                visit(context.variableDefinition(), VariableDefinition.class));

        if (context.identifier() == null) {
            return relation;
        }

        List<Identifier> aliases = null;
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(getLocation(context), relation, (Identifier) visit(context.identifier()), aliases);
    }

    @Override
    public Node visitMeasureDefinition(SqlBaseParser.MeasureDefinitionContext context)
    {
        return new MeasureDefinition(getLocation(context), (Expression) visit(context.expression()), (Identifier) visit(context.identifier()));
    }

    private Optional<RowsPerMatch> getRowsPerMatch(SqlBaseParser.RowsPerMatchContext context)
    {
        if (context == null) {
            return Optional.empty();
        }

        if (context.ONE() != null) {
            return Optional.of(ONE);
        }

        if (context.emptyMatchHandling() == null) {
            return Optional.of(ALL_SHOW_EMPTY);
        }

        if (context.emptyMatchHandling().SHOW() != null) {
            return Optional.of(ALL_SHOW_EMPTY);
        }

        if (context.emptyMatchHandling().OMIT() != null) {
            return Optional.of(ALL_OMIT_EMPTY);
        }

        return Optional.of(ALL_WITH_UNMATCHED);
    }

    @Override
    public Node visitSkipTo(SqlBaseParser.SkipToContext context)
    {
        if (context.PAST() != null) {
            return skipPastLastRow(getLocation(context));
        }

        if (context.NEXT() != null) {
            return skipToNextRow(getLocation(context));
        }

        if (context.FIRST() != null) {
            return skipToFirst(getLocation(context), (Identifier) visit(context.identifier()));
        }

        return skipToLast(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitSubsetDefinition(SqlBaseParser.SubsetDefinitionContext context)
    {
        return new SubsetDefinition(getLocation(context), (Identifier) visit(context.name), visit(context.union, Identifier.class));
    }

    @Override
    public Node visitVariableDefinition(SqlBaseParser.VariableDefinitionContext context)
    {
        return new VariableDefinition(getLocation(context), (Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context)
    {
        Relation child = (Relation) visit(context.relationPrimary());

        if (context.identifier() == null) {
            return child;
        }

        List<Identifier> aliases = null;
        if (context.columnAliases() != null) {
            aliases = visit(context.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(getLocation(context), child, (Identifier) visit(context.identifier()), aliases);
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext context)
    {
        if (context.queryPeriod() != null) {
            return new Table(getLocation(context), getQualifiedName(context.qualifiedName()), (QueryPeriod) visit(context.queryPeriod()));
        }
        return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    @Override
    public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context)
    {
        return new TableSubquery(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitUnnest(SqlBaseParser.UnnestContext context)
    {
        return new Unnest(getLocation(context), visit(context.expression(), Expression.class), context.ORDINALITY() != null);
    }

    @Override
    public Node visitLateral(SqlBaseParser.LateralContext context)
    {
        return new Lateral(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context)
    {
        return visit(context.relation());
    }

    // ********************* predicates *******************

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext context)
    {
        if (context.predicate() != null) {
            return visit(context.predicate());
        }

        return visit(context.valueExpression);
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext context)
    {
        return new ComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context)
    {
        Expression expression = new ComparisonExpression(
                getLocation(context),
                ComparisonExpression.Operator.IS_DISTINCT_FROM,
                (Expression) visit(context.value),
                (Expression) visit(context.right));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitBetween(SqlBaseParser.BetweenContext context)
    {
        Expression expression = new BetweenPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.lower),
                (Expression) visit(context.upper));

        if (context.NOT() != null) {
            expression = new NotExpression(getLocation(context), expression);
        }

        return expression;
    }

    @Override
    public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context)
    {
        Expression child = (Expression) visit(context.value);

        if (context.NOT() == null) {
            return new IsNullPredicate(getLocation(context), child);
        }

        return new IsNotNullPredicate(getLocation(context), child);
    }

    @Override
    public Node visitLike(SqlBaseParser.LikeContext context)
    {
        Expression result = new LikePredicate(
                getLocation(context),
                (Expression) visit(context.value),
                (Expression) visit(context.pattern),
                visitIfPresent(context.escape, Expression.class));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInList(SqlBaseParser.InListContext context)
    {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new InListExpression(getLocation(context), visit(context.expression(), Expression.class)));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitInSubquery(SqlBaseParser.InSubqueryContext context)
    {
        Expression result = new InPredicate(
                getLocation(context),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

        if (context.NOT() != null) {
            result = new NotExpression(getLocation(context), result);
        }

        return result;
    }

    @Override
    public Node visitExists(SqlBaseParser.ExistsContext context)
    {
        return new ExistsPredicate(getLocation(context), new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
    }

    @Override
    public Node visitQuantifiedComparison(SqlBaseParser.QuantifiedComparisonContext context)
    {
        return new QuantifiedComparisonExpression(
                getLocation(context.comparisonOperator()),
                getComparisonOperator(((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
                getComparisonQuantifier(((TerminalNode) context.comparisonQuantifier().getChild(0)).getSymbol()),
                (Expression) visit(context.value),
                new SubqueryExpression(getLocation(context.query()), (Query) visit(context.query())));
    }

    // ************** value expressions **************

    @Override
    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context)
    {
        Expression child = (Expression) visit(context.valueExpression());

        switch (context.operator.getType()) {
            case SqlBaseLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(context), child);
            case SqlBaseLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(context), child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
        }
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context)
    {
        return new ArithmeticBinaryExpression(
                getLocation(context.operator),
                getArithmeticBinaryOperator(context.operator),
                (Expression) visit(context.left),
                (Expression) visit(context.right));
    }

    @Override
    public Node visitConcatenation(SqlBaseParser.ConcatenationContext context)
    {
        return new FunctionCall(
                getLocation(context.CONCAT()),
                QualifiedName.of("concat"), ImmutableList.of(
                (Expression) visit(context.left),
                (Expression) visit(context.right)));
    }

    @Override
    public Node visitAtTimeZone(SqlBaseParser.AtTimeZoneContext context)
    {
        return new AtTimeZone(
                getLocation(context.AT()),
                (Expression) visit(context.valueExpression()),
                (Expression) visit(context.timeZoneSpecifier()));
    }

    @Override
    public Node visitTimeZoneInterval(SqlBaseParser.TimeZoneIntervalContext context)
    {
        return visit(context.interval());
    }

    @Override
    public Node visitTimeZoneString(SqlBaseParser.TimeZoneStringContext context)
    {
        return visit(context.string());
    }

    // ********************* primary expressions **********************

    @Override
    public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext context)
    {
        return visit(context.expression());
    }

    @Override
    public Node visitRowConstructor(SqlBaseParser.RowConstructorContext context)
    {
        return new Row(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitArrayConstructor(SqlBaseParser.ArrayConstructorContext context)
    {
        return new ArrayConstructor(getLocation(context), visit(context.expression(), Expression.class));
    }

    @Override
    public Node visitCast(SqlBaseParser.CastContext context)
    {
        boolean isTryCast = context.TRY_CAST() != null;
        return new Cast(getLocation(context), (Expression) visit(context.expression()), (DataType) visit(context.type()), isTryCast);
    }

    @Override
    public Node visitSpecialDateTimeFunction(SqlBaseParser.SpecialDateTimeFunctionContext context)
    {
        CurrentTime.Function function = getDateTimeFunctionType(context.name);

        if (context.precision != null) {
            return new CurrentTime(getLocation(context), function, Integer.parseInt(context.precision.getText()));
        }

        return new CurrentTime(getLocation(context), function);
    }

    @Override
    public Node visitCurrentCatalog(SqlBaseParser.CurrentCatalogContext context)
    {
        return new CurrentCatalog(getLocation(context.CURRENT_CATALOG()));
    }

    @Override
    public Node visitCurrentSchema(SqlBaseParser.CurrentSchemaContext context)
    {
        return new CurrentSchema(getLocation(context.CURRENT_SCHEMA()));
    }

    @Override
    public Node visitCurrentUser(SqlBaseParser.CurrentUserContext context)
    {
        return new CurrentUser(getLocation(context.CURRENT_USER()));
    }

    @Override
    public Node visitCurrentPath(SqlBaseParser.CurrentPathContext context)
    {
        return new CurrentPath(getLocation(context.CURRENT_PATH()));
    }

    @Override
    public Node visitExtract(SqlBaseParser.ExtractContext context)
    {
        String fieldString = context.identifier().getText();
        Extract.Field field;
        try {
            field = Extract.Field.valueOf(fieldString.toUpperCase(ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw parseError("Invalid EXTRACT field: " + fieldString, context);
        }
        return new Extract(getLocation(context), (Expression) visit(context.valueExpression()), field);
    }

    /**
     * Returns the corresponding {@link FunctionCall} for the `LISTAGG` primary expression.
     * <p>
     * Although the syntax tree should represent the structure of the original parsed query
     * as closely as possible and any semantic interpretation should be part of the
     * analysis/planning phase, in case of `LISTAGG` aggregation function it is more pragmatic
     * now to create a synthetic {@link FunctionCall} expression during the parsing of the syntax tree.
     *
     * @param context `LISTAGG` expression context
     */
    @Override
    public Node visitListagg(SqlBaseParser.ListaggContext context)
    {
        Optional<Window> window = Optional.empty();
        OrderBy orderBy = new OrderBy(visit(context.sortItem(), SortItem.class));
        boolean distinct = isDistinct(context.setQuantifier());

        Expression expression = (Expression) visit(context.expression());
        StringLiteral separator = context.string() == null ? new StringLiteral(getLocation(context), "") : (StringLiteral) (visit(context.string()));
        BooleanLiteral overflowError = new BooleanLiteral(getLocation(context), "true");
        StringLiteral overflowFiller = new StringLiteral(getLocation(context), "...");
        BooleanLiteral showOverflowEntryCount = new BooleanLiteral(getLocation(context), "false");

        SqlBaseParser.ListAggOverflowBehaviorContext overflowBehavior = context.listAggOverflowBehavior();
        if (overflowBehavior != null) {
            if (overflowBehavior.ERROR() != null) {
                overflowError = new BooleanLiteral(getLocation(context), "true");
            }
            else if (overflowBehavior.TRUNCATE() != null) {
                overflowError = new BooleanLiteral(getLocation(context), "false");
                if (overflowBehavior.string() != null) {
                    overflowFiller = (StringLiteral) (visit(overflowBehavior.string()));
                }
                SqlBaseParser.ListaggCountIndicationContext listaggCountIndicationContext = overflowBehavior.listaggCountIndication();
                if (listaggCountIndicationContext.WITH() != null) {
                    showOverflowEntryCount = new BooleanLiteral(getLocation(context), "true");
                }
                else if (listaggCountIndicationContext.WITHOUT() != null) {
                    showOverflowEntryCount = new BooleanLiteral(getLocation(context), "false");
                }
            }
        }

        List<Expression> arguments = ImmutableList.of(expression, separator, overflowError, overflowFiller, showOverflowEntryCount);

        //TODO model this as a ListAgg node in the AST
        return new FunctionCall(
                Optional.of(getLocation(context)),
                QualifiedName.of("LISTAGG"),
                window,
                Optional.empty(),
                Optional.of(orderBy),
                distinct,
                Optional.empty(),
                Optional.empty(),
                arguments);
    }

    @Override
    public Node visitSubstring(SqlBaseParser.SubstringContext context)
    {
        return new FunctionCall(getLocation(context), QualifiedName.of("substr"), visit(context.valueExpression(), Expression.class));
    }

    @Override
    public Node visitPosition(SqlBaseParser.PositionContext context)
    {
        List<Expression> arguments = Lists.reverse(visit(context.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(context), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitNormalize(SqlBaseParser.NormalizeContext context)
    {
        Expression str = (Expression) visit(context.valueExpression());
        String normalForm = Optional.ofNullable(context.normalForm()).map(ParserRuleContext::getText).orElse("NFC");
        return new FunctionCall(
                getLocation(context),
                QualifiedName.of(ImmutableList.of(new Identifier("normalize", true))), // delimited to avoid ambiguity with NORMALIZE SQL construct
                ImmutableList.of(str, new StringLiteral(getLocation(context), normalForm)));
    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext context)
    {
        return new SubscriptExpression(getLocation(context), (Expression) visit(context.value), (Expression) visit(context.index));
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context)
    {
        return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext context)
    {
        return new DereferenceExpression(
                getLocation(context),
                (Expression) visit(context.base),
                (Identifier) visit(context.fieldName));
    }

    @Override
    public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context)
    {
        return visit(context.identifier());
    }

    @Override
    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context)
    {
        return new SimpleCaseExpression(
                getLocation(context),
                (Expression) visit(context.operand),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext context)
    {
        return new SearchedCaseExpression(
                getLocation(context),
                visit(context.whenClause(), WhenClause.class),
                visitIfPresent(context.elseExpression, Expression.class));
    }

    @Override
    public Node visitWhenClause(SqlBaseParser.WhenClauseContext context)
    {
        return new WhenClause(getLocation(context), (Expression) visit(context.condition), (Expression) visit(context.result));
    }

    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context)
    {
        Optional<Expression> filter = visitIfPresent(context.filter(), Expression.class);
        Optional<Window> window = visitIfPresent(context.over(), Window.class);

        Optional<OrderBy> orderBy = Optional.empty();
        if (context.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(visit(context.sortItem(), SortItem.class)));
        }

        QualifiedName name = getQualifiedName(context.qualifiedName());

        boolean distinct = isDistinct(context.setQuantifier());

        SqlBaseParser.NullTreatmentContext nullTreatment = context.nullTreatment();

        SqlBaseParser.ProcessingModeContext processingMode = context.processingMode();

        if (name.toString().equalsIgnoreCase("if")) {
            check(context.expression().size() == 2 || context.expression().size() == 3, "Invalid number of arguments for 'if' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", context);
            check(!distinct, "DISTINCT not valid for 'if' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'if' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'if' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'if' function", context);

            Expression elseExpression = null;
            if (context.expression().size() == 3) {
                elseExpression = (Expression) visit(context.expression(2));
            }

            return new IfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)),
                    elseExpression);
        }

        if (name.toString().equalsIgnoreCase("nullif")) {
            check(context.expression().size() == 2, "Invalid number of arguments for 'nullif' function", context);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", context);
            check(!distinct, "DISTINCT not valid for 'nullif' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'nullif' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'nullif' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'nullif' function", context);

            return new NullIfExpression(
                    getLocation(context),
                    (Expression) visit(context.expression(0)),
                    (Expression) visit(context.expression(1)));
        }

        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(context.expression().size() >= 2, "The 'coalesce' function must have at least two arguments", context);
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", context);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'coalesce' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'coalesce' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'coalesce' function", context);

            return new CoalesceExpression(getLocation(context), visit(context.expression(), Expression.class));
        }

        if (name.toString().equalsIgnoreCase("try")) {
            check(context.expression().size() == 1, "The 'try' function must have exactly one argument", context);
            check(!window.isPresent(), "OVER clause not valid for 'try' function", context);
            check(!distinct, "DISTINCT not valid for 'try' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'try' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'try' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'try' function", context);

            return new TryExpression(getLocation(context), (Expression) visit(getOnlyElement(context.expression())));
        }

        if (name.toString().equalsIgnoreCase("format")) {
            check(context.expression().size() >= 2, "The 'format' function must have at least two arguments", context);
            check(!window.isPresent(), "OVER clause not valid for 'format' function", context);
            check(!distinct, "DISTINCT not valid for 'format' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for 'format' function", context);
            check(processingMode == null, "Running or final semantics not valid for 'format' function", context);
            check(!filter.isPresent(), "FILTER not valid for 'format' function", context);

            return new Format(getLocation(context), visit(context.expression(), Expression.class));
        }

        if (name.toString().equalsIgnoreCase("$internal$bind")) {
            check(context.expression().size() >= 1, "The '$internal$bind' function must have at least one arguments", context);
            check(!window.isPresent(), "OVER clause not valid for '$internal$bind' function", context);
            check(!distinct, "DISTINCT not valid for '$internal$bind' function", context);
            check(nullTreatment == null, "Null treatment clause not valid for '$internal$bind' function", context);
            check(processingMode == null, "Running or final semantics not valid for '$internal$bind' function", context);
            check(!filter.isPresent(), "FILTER not valid for '$internal$bind' function", context);

            int numValues = context.expression().size() - 1;
            List<Expression> arguments = context.expression().stream()
                    .map(this::visit)
                    .map(Expression.class::cast)
                    .collect(toImmutableList());

            return new BindExpression(
                    getLocation(context),
                    arguments.subList(0, numValues),
                    arguments.get(numValues));
        }

        Optional<NullTreatment> nulls = Optional.empty();
        if (nullTreatment != null) {
            if (nullTreatment.IGNORE() != null) {
                nulls = Optional.of(NullTreatment.IGNORE);
            }
            else if (nullTreatment.RESPECT() != null) {
                nulls = Optional.of(NullTreatment.RESPECT);
            }
        }

        Optional<ProcessingMode> mode = Optional.empty();
        if (processingMode != null) {
            if (processingMode.RUNNING() != null) {
                mode = Optional.of(new ProcessingMode(getLocation(processingMode), RUNNING));
            }
            else if (processingMode.FINAL() != null) {
                mode = Optional.of(new ProcessingMode(getLocation(processingMode), FINAL));
            }
        }

        List<Expression> arguments = visit(context.expression(), Expression.class);
        if (context.label != null) {
            arguments = ImmutableList.of(new DereferenceExpression(getLocation(context.label), (Identifier) visit(context.label)));
        }

        return new FunctionCall(
                Optional.of(getLocation(context)),
                name,
                window,
                filter,
                orderBy,
                distinct,
                nulls,
                mode,
                arguments);
    }

    @Override
    public Node visitMeasure(SqlBaseParser.MeasureContext context)
    {
        return new WindowOperation(getLocation(context), (Identifier) visit(context.identifier()), (Window) visit(context.over()));
    }

    @Override
    public Node visitLambda(SqlBaseParser.LambdaContext context)
    {
        List<LambdaArgumentDeclaration> arguments = visit(context.identifier(), Identifier.class).stream()
                .map(LambdaArgumentDeclaration::new)
                .collect(toList());

        Expression body = (Expression) visit(context.expression());

        return new LambdaExpression(getLocation(context), arguments, body);
    }

    @Override
    public Node visitFilter(SqlBaseParser.FilterContext context)
    {
        return visit(context.booleanExpression());
    }

    @Override
    public Node visitOver(SqlBaseParser.OverContext context)
    {
        if (context.windowName != null) {
            return new WindowReference(getLocation(context), (Identifier) visit(context.windowName));
        }

        return visit(context.windowSpecification());
    }

    @Override
    public Node visitColumnDefinition(SqlBaseParser.ColumnDefinitionContext context)
    {
        Optional<String> comment = Optional.empty();
        if (context.COMMENT() != null) {
            comment = Optional.of(((StringLiteral) visit(context.string())).getValue());
        }

        List<Property> properties = ImmutableList.of();
        if (context.properties() != null) {
            properties = visit(context.properties().propertyAssignments().property(), Property.class);
        }

        boolean nullable = context.NOT() == null;

        return new ColumnDefinition(
                getLocation(context),
                (Identifier) visit(context.identifier()),
                (DataType) visit(context.type()),
                nullable,
                properties,
                comment);
    }

    @Override
    public Node visitLikeClause(SqlBaseParser.LikeClauseContext context)
    {
        return new LikeClause(
                getLocation(context),
                getQualifiedName(context.qualifiedName()),
                Optional.ofNullable(context.optionType)
                        .map(AstBuilder::getPropertiesOption));
    }

    @Override
    public Node visitSortItem(SqlBaseParser.SortItemContext context)
    {
        return new SortItem(
                getLocation(context),
                (Expression) visit(context.expression()),
                Optional.ofNullable(context.ordering)
                        .map(AstBuilder::getOrderingType)
                        .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(context.nullOrdering)
                        .map(AstBuilder::getNullOrderingType)
                        .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitWindowFrame(SqlBaseParser.WindowFrameContext context)
    {
        Optional<PatternSearchMode> searchMode = Optional.empty();
        if (context.INITIAL() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.INITIAL()), INITIAL));
        }
        else if (context.SEEK() != null) {
            searchMode = Optional.of(new PatternSearchMode(getLocation(context.SEEK()), SEEK));
        }

        return new WindowFrame(
                getLocation(context),
                getFrameType(context.frameExtent().frameType),
                (FrameBound) visit(context.frameExtent().start),
                visitIfPresent(context.frameExtent().end, FrameBound.class),
                visit(context.measureDefinition(), MeasureDefinition.class),
                visitIfPresent(context.skipTo(), SkipTo.class),
                searchMode,
                visitIfPresent(context.rowPattern(), RowPattern.class),
                visit(context.subsetDefinition(), SubsetDefinition.class),
                visit(context.variableDefinition(), VariableDefinition.class));
    }

    @Override
    public Node visitUnboundedFrame(SqlBaseParser.UnboundedFrameContext context)
    {
        return new FrameBound(getLocation(context), getUnboundedFrameBoundType(context.boundType));
    }

    @Override
    public Node visitBoundedFrame(SqlBaseParser.BoundedFrameContext context)
    {
        return new FrameBound(getLocation(context), getBoundedFrameBoundType(context.boundType), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitCurrentRowBound(SqlBaseParser.CurrentRowBoundContext context)
    {
        return new FrameBound(getLocation(context), FrameBound.Type.CURRENT_ROW);
    }

    @Override
    public Node visitGroupingOperation(SqlBaseParser.GroupingOperationContext context)
    {
        List<QualifiedName> arguments = context.qualifiedName().stream()
                .map(this::getQualifiedName)
                .collect(toList());

        return new GroupingOperation(Optional.of(getLocation(context)), arguments);
    }

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context)
    {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    @Override
    public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context)
    {
        String token = context.getText();
        String identifier = token.substring(1, token.length() - 1)
                .replace("\"\"", "\"");

        return new Identifier(getLocation(context), identifier, true);
    }

    @Override
    public Node visitPatternAlternation(SqlBaseParser.PatternAlternationContext context)
    {
        List<RowPattern> parts = visit(context.rowPattern(), RowPattern.class);
        return new PatternAlternation(getLocation(context), parts);
    }

    @Override
    public Node visitPatternConcatenation(SqlBaseParser.PatternConcatenationContext context)
    {
        List<RowPattern> parts = visit(context.rowPattern(), RowPattern.class);
        return new PatternConcatenation(getLocation(context), parts);
    }

    @Override
    public Node visitQuantifiedPrimary(SqlBaseParser.QuantifiedPrimaryContext context)
    {
        RowPattern primary = (RowPattern) visit(context.patternPrimary());
        if (context.patternQuantifier() != null) {
            return new QuantifiedPattern(getLocation(context), primary, (PatternQuantifier) visit(context.patternQuantifier()));
        }
        return primary;
    }

    @Override
    public Node visitPatternVariable(SqlBaseParser.PatternVariableContext context)
    {
        return new PatternVariable(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitEmptyPattern(SqlBaseParser.EmptyPatternContext context)
    {
        return new EmptyPattern(getLocation(context));
    }

    @Override
    public Node visitPatternPermutation(SqlBaseParser.PatternPermutationContext context)
    {
        return new PatternPermutation(getLocation(context), visit(context.rowPattern(), RowPattern.class));
    }

    @Override
    public Node visitGroupedPattern(SqlBaseParser.GroupedPatternContext context)
    {
        // skip parentheses
        return visit(context.rowPattern());
    }

    @Override
    public Node visitPartitionStartAnchor(SqlBaseParser.PartitionStartAnchorContext context)
    {
        return new AnchorPattern(getLocation(context), PARTITION_START);
    }

    @Override
    public Node visitPartitionEndAnchor(SqlBaseParser.PartitionEndAnchorContext context)
    {
        return new AnchorPattern(getLocation(context), PARTITION_END);
    }

    @Override
    public Node visitExcludedPattern(SqlBaseParser.ExcludedPatternContext context)
    {
        return new ExcludedPattern(getLocation(context), (RowPattern) visit(context.rowPattern()));
    }

    @Override
    public Node visitZeroOrMoreQuantifier(SqlBaseParser.ZeroOrMoreQuantifierContext context)
    {
        boolean greedy = context.reluctant == null;
        return new ZeroOrMoreQuantifier(getLocation(context), greedy);
    }

    @Override
    public Node visitOneOrMoreQuantifier(SqlBaseParser.OneOrMoreQuantifierContext context)
    {
        boolean greedy = context.reluctant == null;
        return new OneOrMoreQuantifier(getLocation(context), greedy);
    }

    @Override
    public Node visitZeroOrOneQuantifier(SqlBaseParser.ZeroOrOneQuantifierContext context)
    {
        boolean greedy = context.reluctant == null;
        return new ZeroOrOneQuantifier(getLocation(context), greedy);
    }

    @Override
    public Node visitRangeQuantifier(SqlBaseParser.RangeQuantifierContext context)
    {
        boolean greedy = context.reluctant == null;

        Optional<LongLiteral> atLeast = Optional.empty();
        Optional<LongLiteral> atMost = Optional.empty();
        if (context.exactly != null) {
            atLeast = Optional.of(new LongLiteral(getLocation(context.exactly), context.exactly.getText()));
            atMost = Optional.of(new LongLiteral(getLocation(context.exactly), context.exactly.getText()));
        }
        if (context.atLeast != null) {
            atLeast = Optional.of(new LongLiteral(getLocation(context.atLeast), context.atLeast.getText()));
        }
        if (context.atMost != null) {
            atMost = Optional.of(new LongLiteral(getLocation(context.atMost), context.atMost.getText()));
        }
        return new RangeQuantifier(getLocation(context), greedy, atLeast, atMost);
    }

    // ************** literals **************

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context)
    {
        return new NullLiteral(getLocation(context));
    }

    @Override
    public Node visitBasicStringLiteral(SqlBaseParser.BasicStringLiteralContext context)
    {
        return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
    }

    @Override
    public Node visitUnicodeStringLiteral(SqlBaseParser.UnicodeStringLiteralContext context)
    {
        return new StringLiteral(getLocation(context), decodeUnicodeLiteral(context));
    }

    @Override
    public Node visitBinaryLiteral(SqlBaseParser.BinaryLiteralContext context)
    {
        String raw = context.BINARY_LITERAL().getText();
        return new BinaryLiteral(getLocation(context), unquote(raw.substring(1)));
    }

    @Override
    public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext context)
    {
        String value = ((StringLiteral) visit(context.string())).getValue();

        if (context.DOUBLE() != null) {
            // TODO: Temporary hack that should be removed with new planner.
            return new GenericLiteral(getLocation(context), "DOUBLE", value);
        }

        String type = context.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(context), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(context), value);
        }

        return new GenericLiteral(getLocation(context), type, value);
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context)
    {
        return new LongLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context)
    {
        switch (parsingOptions.getDecimalLiteralTreatment()) {
            case AS_DOUBLE:
                return new DoubleLiteral(getLocation(context), context.getText());
            case AS_DECIMAL:
                return new DecimalLiteral(getLocation(context), context.getText());
            case REJECT:
                throw new ParsingException("Unexpected decimal literal: " + context.getText());
        }
        throw new AssertionError("Unreachable");
    }

    @Override
    public Node visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext context)
    {
        return new DoubleLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext context)
    {
        return new BooleanLiteral(getLocation(context), context.getText());
    }

    @Override
    public Node visitInterval(SqlBaseParser.IntervalContext context)
    {
        return new IntervalLiteral(
                getLocation(context),
                ((StringLiteral) visit(context.string())).getValue(),
                Optional.ofNullable(context.sign)
                        .map(AstBuilder::getIntervalSign)
                        .orElse(IntervalLiteral.Sign.POSITIVE),
                getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
                Optional.ofNullable(context.to)
                        .map((x) -> x.getChild(0).getPayload())
                        .map(Token.class::cast)
                        .map(AstBuilder::getIntervalFieldType));
    }

    @Override
    public Node visitParameter(SqlBaseParser.ParameterContext context)
    {
        io.trino.sql.tree.Parameter parameter = new io.trino.sql.tree.Parameter(getLocation(context), parameterPosition);
        parameterPosition++;
        return parameter;
    }

    // ***************** arguments *****************

    @Override
    public Node visitPositionalArgument(SqlBaseParser.PositionalArgumentContext context)
    {
        return new CallArgument(getLocation(context), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitNamedArgument(SqlBaseParser.NamedArgumentContext context)
    {
        return new CallArgument(getLocation(context), (Identifier) visit(context.identifier()), (Expression) visit(context.expression()));
    }

    @Override
    public Node visitQualifiedArgument(SqlBaseParser.QualifiedArgumentContext context)
    {
        return new PathElement(getLocation(context), (Identifier) visit(context.identifier(0)), (Identifier) visit(context.identifier(1)));
    }

    @Override
    public Node visitUnqualifiedArgument(SqlBaseParser.UnqualifiedArgumentContext context)
    {
        return new PathElement(getLocation(context), (Identifier) visit(context.identifier()));
    }

    @Override
    public Node visitPathSpecification(SqlBaseParser.PathSpecificationContext context)
    {
        return new PathSpecification(getLocation(context), visit(context.pathElement(), PathElement.class));
    }

    @Override
    public Node visitRowType(SqlBaseParser.RowTypeContext context)
    {
        List<RowDataType.Field> fields = context.rowField().stream()
                .map(this::visit)
                .map(RowDataType.Field.class::cast)
                .collect(toImmutableList());

        return new RowDataType(getLocation(context), fields);
    }

    @Override
    public Node visitRowField(SqlBaseParser.RowFieldContext context)
    {
        return new RowDataType.Field(
                getLocation(context),
                visitIfPresent(context.identifier(), Identifier.class),
                (DataType) visit(context.type()));
    }

    @Override
    public Node visitGenericType(SqlBaseParser.GenericTypeContext context)
    {
        List<DataTypeParameter> parameters = context.typeParameter().stream()
                .map(this::visit)
                .map(DataTypeParameter.class::cast)
                .collect(toImmutableList());

        return new GenericDataType(getLocation(context), (Identifier) visit(context.identifier()), parameters);
    }

    @Override
    public Node visitTypeParameter(SqlBaseParser.TypeParameterContext context)
    {
        if (context.INTEGER_VALUE() != null) {
            return new NumericParameter(getLocation(context), context.getText());
        }

        return new TypeParameter((DataType) visit(context.type()));
    }

    @Override
    public Node visitIntervalType(SqlBaseParser.IntervalTypeContext context)
    {
        String from = context.from.getText();
        String to = getTextIfPresent(context.to)
                .orElse(from);

        return new IntervalDayTimeDataType(
                getLocation(context),
                IntervalDayTimeDataType.Field.valueOf(from.toUpperCase(ENGLISH)),
                IntervalDayTimeDataType.Field.valueOf(to.toUpperCase(ENGLISH)));
    }

    @Override
    public Node visitDateTimeType(SqlBaseParser.DateTimeTypeContext context)
    {
        DateTimeDataType.Type type;

        if (context.base.getType() == TIME) {
            type = DateTimeDataType.Type.TIME;
        }
        else if (context.base.getType() == TIMESTAMP) {
            type = DateTimeDataType.Type.TIMESTAMP;
        }
        else {
            throw new ParsingException("Unexpected datetime type: " + context.getText());
        }

        return new DateTimeDataType(
                getLocation(context),
                type,
                context.WITH() != null,
                visitIfPresent(context.precision, DataTypeParameter.class));
    }

    @Override
    public Node visitDoublePrecisionType(SqlBaseParser.DoublePrecisionTypeContext context)
    {
        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.DOUBLE()), context.DOUBLE().getText(), false),
                ImmutableList.of());
    }

    @Override
    public Node visitLegacyArrayType(SqlBaseParser.LegacyArrayTypeContext context)
    {
        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.ARRAY()), context.ARRAY().getText(), false),
                ImmutableList.of(new TypeParameter((DataType) visit(context.type()))));
    }

    @Override
    public Node visitLegacyMapType(SqlBaseParser.LegacyMapTypeContext context)
    {
        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.MAP()), context.MAP().getText(), false),
                ImmutableList.of(
                        new TypeParameter((DataType) visit(context.keyType)),
                        new TypeParameter((DataType) visit(context.valueType))));
    }

    @Override
    public Node visitArrayType(SqlBaseParser.ArrayTypeContext context)
    {
        if (context.INTEGER_VALUE() != null) {
            throw new UnsupportedOperationException("Explicit array size not supported");
        }

        return new GenericDataType(
                getLocation(context),
                new Identifier(getLocation(context.ARRAY()), context.ARRAY().getText(), false),
                ImmutableList.of(new TypeParameter((DataType) visit(context.type()))));
    }

    @Override
    public Node visitQueryPeriod(SqlBaseParser.QueryPeriodContext context)
    {
        QueryPeriod.RangeType type = getRangeType((Token) context.rangeType().getChild(0).getPayload());
        Expression marker = (Expression) visit(context.valueExpression());
        return new QueryPeriod(getLocation(context), type, marker);
    }

    // ***************** helpers *****************

    @Override
    protected Node defaultResult()
    {
        return null;
    }

    @Override
    protected Node aggregateResult(Node aggregate, Node nextResult)
    {
        if (nextResult == null) {
            throw new UnsupportedOperationException("not yet implemented");
        }

        if (aggregate == null) {
            return nextResult;
        }

        throw new UnsupportedOperationException("not yet implemented");
    }

    private enum UnicodeDecodeState
    {
        EMPTY,
        ESCAPED,
        UNICODE_SEQUENCE
    }

    private static String decodeUnicodeLiteral(SqlBaseParser.UnicodeStringLiteralContext context)
    {
        char escape;
        if (context.UESCAPE() != null) {
            String escapeString = unquote(context.STRING().getText());
            check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
            check(escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString, context);
            escape = escapeString.charAt(0);
            check(isValidUnicodeEscape(escape), "Invalid Unicode escape character: " + escapeString, context);
        }
        else {
            escape = '\\';
        }

        String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
        StringBuilder unicodeStringBuilder = new StringBuilder();
        StringBuilder escapedCharacterBuilder = new StringBuilder();
        int charactersNeeded = 0;
        UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
        for (int i = 0; i < rawContent.length(); i++) {
            char ch = rawContent.charAt(i);
            switch (state) {
                case EMPTY:
                    if (ch == escape) {
                        state = UnicodeDecodeState.ESCAPED;
                    }
                    else {
                        unicodeStringBuilder.append(ch);
                    }
                    break;
                case ESCAPED:
                    if (ch == escape) {
                        unicodeStringBuilder.append(escape);
                        state = UnicodeDecodeState.EMPTY;
                    }
                    else if (ch == '+') {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 6;
                    }
                    else if (isHexDigit(ch)) {
                        state = UnicodeDecodeState.UNICODE_SEQUENCE;
                        charactersNeeded = 4;
                        escapedCharacterBuilder.append(ch);
                    }
                    else {
                        throw parseError("Invalid hexadecimal digit: " + ch, context);
                    }
                    break;
                case UNICODE_SEQUENCE:
                    check(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
                    escapedCharacterBuilder.append(ch);
                    if (charactersNeeded == escapedCharacterBuilder.length()) {
                        String currentEscapedCode = escapedCharacterBuilder.toString();
                        escapedCharacterBuilder.setLength(0);
                        int codePoint = Integer.parseInt(currentEscapedCode, 16);
                        check(Character.isValidCodePoint(codePoint), "Invalid escaped character: " + currentEscapedCode, context);
                        if (Character.isSupplementaryCodePoint(codePoint)) {
                            unicodeStringBuilder.appendCodePoint(codePoint);
                        }
                        else {
                            char currentCodePoint = (char) codePoint;
                            check(!Character.isSurrogate(currentCodePoint), format("Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.", currentEscapedCode), context);
                            unicodeStringBuilder.append(currentCodePoint);
                        }
                        state = UnicodeDecodeState.EMPTY;
                        charactersNeeded = -1;
                    }
                    else {
                        check(charactersNeeded > escapedCharacterBuilder.length(), "Unexpected escape sequence length: " + escapedCharacterBuilder.length(), context);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        check(state == UnicodeDecodeState.EMPTY, "Incomplete escape sequence: " + escapedCharacterBuilder.toString(), context);
        return unicodeStringBuilder.toString();
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    private static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    private static LikeClause.PropertiesOption getPropertiesOption(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.INCLUDING:
                return LikeClause.PropertiesOption.INCLUDING;
            case SqlBaseLexer.EXCLUDING:
                return LikeClause.PropertiesOption.EXCLUDING;
        }
        throw new IllegalArgumentException("Unsupported LIKE option type: " + token.getText());
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        return QualifiedName.of(visit(context.identifier(), Identifier.class));
    }

    private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    private static boolean isHexDigit(char c)
    {
        return ((c >= '0') && (c <= '9')) ||
                ((c >= 'A') && (c <= 'F')) ||
                ((c >= 'a') && (c <= 'f'));
    }

    private static boolean isValidUnicodeEscape(char c)
    {
        return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
    }

    private static Optional<String> getTextIfPresent(ParserRuleContext context)
    {
        return Optional.ofNullable(context)
                .map(ParseTree::getText);
    }

    private Optional<Identifier> getIdentifierIfPresent(ParserRuleContext context)
    {
        return Optional.ofNullable(context).map(c -> (Identifier) visit(c));
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case SqlBaseLexer.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case SqlBaseLexer.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case SqlBaseLexer.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case SqlBaseLexer.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case SqlBaseLexer.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case SqlBaseLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case SqlBaseLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case SqlBaseLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case SqlBaseLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case SqlBaseLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case SqlBaseLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static CurrentTime.Function getDateTimeFunctionType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.CURRENT_DATE:
                return CurrentTime.Function.DATE;
            case SqlBaseLexer.CURRENT_TIME:
                return CurrentTime.Function.TIME;
            case SqlBaseLexer.CURRENT_TIMESTAMP:
                return CurrentTime.Function.TIMESTAMP;
            case SqlBaseLexer.LOCALTIME:
                return CurrentTime.Function.LOCALTIME;
            case SqlBaseLexer.LOCALTIMESTAMP:
                return CurrentTime.Function.LOCALTIMESTAMP;
        }

        throw new IllegalArgumentException("Unsupported special function: " + token.getText());
    }

    private static IntervalLiteral.IntervalField getIntervalFieldType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.YEAR:
                return IntervalLiteral.IntervalField.YEAR;
            case SqlBaseLexer.MONTH:
                return IntervalLiteral.IntervalField.MONTH;
            case SqlBaseLexer.DAY:
                return IntervalLiteral.IntervalField.DAY;
            case SqlBaseLexer.HOUR:
                return IntervalLiteral.IntervalField.HOUR;
            case SqlBaseLexer.MINUTE:
                return IntervalLiteral.IntervalField.MINUTE;
            case SqlBaseLexer.SECOND:
                return IntervalLiteral.IntervalField.SECOND;
        }

        throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
    }

    private static IntervalLiteral.Sign getIntervalSign(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.MINUS:
                return IntervalLiteral.Sign.NEGATIVE;
            case SqlBaseLexer.PLUS:
                return IntervalLiteral.Sign.POSITIVE;
        }

        throw new IllegalArgumentException("Unsupported sign: " + token.getText());
    }

    private static WindowFrame.Type getFrameType(Token type)
    {
        switch (type.getType()) {
            case SqlBaseLexer.RANGE:
                return WindowFrame.Type.RANGE;
            case SqlBaseLexer.ROWS:
                return WindowFrame.Type.ROWS;
            case SqlBaseLexer.GROUPS:
                return WindowFrame.Type.GROUPS;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    private static FrameBound.Type getBoundedFrameBoundType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static FrameBound.Type getUnboundedFrameBoundType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.UNBOUNDED_PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.UNBOUNDED_FOLLOWING;
        }

        throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
    }

    private static SampledRelation.Type getSamplingMethod(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.BERNOULLI:
                return SampledRelation.Type.BERNOULLI;
            case SqlBaseLexer.SYSTEM:
                return SampledRelation.Type.SYSTEM;
        }

        throw new IllegalArgumentException("Unsupported sampling method: " + token.getText());
    }

    private static SortItem.NullOrdering getNullOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.FIRST:
                return SortItem.NullOrdering.FIRST;
            case SqlBaseLexer.LAST:
                return SortItem.NullOrdering.LAST;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static SortItem.Ordering getOrderingType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.ASC:
                return SortItem.Ordering.ASCENDING;
            case SqlBaseLexer.DESC:
                return SortItem.Ordering.DESCENDING;
        }

        throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
    }

    private static QuantifiedComparisonExpression.Quantifier getComparisonQuantifier(Token symbol)
    {
        switch (symbol.getType()) {
            case SqlBaseLexer.ALL:
                return QuantifiedComparisonExpression.Quantifier.ALL;
            case SqlBaseLexer.ANY:
                return QuantifiedComparisonExpression.Quantifier.ANY;
            case SqlBaseLexer.SOME:
                return QuantifiedComparisonExpression.Quantifier.SOME;
        }

        throw new IllegalArgumentException("Unsupported quantifier: " + symbol.getText());
    }

    private List<Identifier> getIdentifiers(List<SqlBaseParser.IdentifierContext> identifiers)
    {
        return identifiers.stream().map(context -> (Identifier) visit(context)).collect(toList());
    }

    private List<PrincipalSpecification> getPrincipalSpecifications(List<SqlBaseParser.PrincipalContext> principals)
    {
        return principals.stream().map(this::getPrincipalSpecification).collect(toList());
    }

    private Optional<GrantorSpecification> getGrantorSpecificationIfPresent(SqlBaseParser.GrantorContext context)
    {
        return Optional.ofNullable(context).map(this::getGrantorSpecification);
    }

    private GrantorSpecification getGrantorSpecification(SqlBaseParser.GrantorContext context)
    {
        if (context instanceof SqlBaseParser.SpecifiedPrincipalContext) {
            return new GrantorSpecification(GrantorSpecification.Type.PRINCIPAL, Optional.of(getPrincipalSpecification(((SqlBaseParser.SpecifiedPrincipalContext) context).principal())));
        }
        else if (context instanceof SqlBaseParser.CurrentUserGrantorContext) {
            return new GrantorSpecification(GrantorSpecification.Type.CURRENT_USER, Optional.empty());
        }
        else if (context instanceof SqlBaseParser.CurrentRoleGrantorContext) {
            return new GrantorSpecification(GrantorSpecification.Type.CURRENT_ROLE, Optional.empty());
        }
        else {
            throw new IllegalArgumentException("Unsupported grantor: " + context);
        }
    }

    private PrincipalSpecification getPrincipalSpecification(SqlBaseParser.PrincipalContext context)
    {
        if (context instanceof SqlBaseParser.UnspecifiedPrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.UNSPECIFIED, (Identifier) visit(((SqlBaseParser.UnspecifiedPrincipalContext) context).identifier()));
        }
        else if (context instanceof SqlBaseParser.UserPrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.USER, (Identifier) visit(((SqlBaseParser.UserPrincipalContext) context).identifier()));
        }
        else if (context instanceof SqlBaseParser.RolePrincipalContext) {
            return new PrincipalSpecification(PrincipalSpecification.Type.ROLE, (Identifier) visit(((SqlBaseParser.RolePrincipalContext) context).identifier()));
        }
        else {
            throw new IllegalArgumentException("Unsupported principal: " + context);
        }
    }

    private static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw parseError(message, context);
        }
    }

    public static NodeLocation getLocation(TerminalNode terminalNode)
    {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine() + 1);
    }

    private static ParsingException parseError(String message, ParserRuleContext context)
    {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine() + 1);
    }

    private static QueryPeriod.RangeType getRangeType(Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.TIMESTAMP:
                return QueryPeriod.RangeType.TIMESTAMP;
            case SqlBaseLexer.VERSION:
                return QueryPeriod.RangeType.VERSION;
        }
        throw new IllegalArgumentException("Unsupported query period range type: " + token.getText());
    }
}
