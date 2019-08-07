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
package io.prestosql.sql.parser.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.hivesql.sql.parser.SqlBaseLexer;
import io.hivesql.sql.parser.SqlBaseParser;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.parser.ParsingOptions;
import io.prestosql.sql.tree.*;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HiveAstBuilder extends io.hivesql.sql.parser.SqlBaseBaseVisitor<Node> {

    private final ParsingOptions parsingOptions;

    public HiveAstBuilder(ParsingOptions parsingOptions)
    {
        this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
    }

    @Override
    protected Node aggregateResult(Node currentResult, Node nextResult) {
        if (currentResult == null) {
            return nextResult;
        }
        if (nextResult == null) {
            return currentResult;
        }

        throw new RuntimeException("please check, how should we merge them?");
    }

    //////////////////
    //
    //////////////////
    @Override
    public Node visitSetOperation(SqlBaseParser.SetOperationContext ctx) {
        QueryBody left = (QueryBody) visit(ctx.left);
        QueryBody right = (QueryBody) visit(ctx.right);

        boolean distinct = ctx.setQuantifier() == null || ctx.setQuantifier().DISTINCT() != null;

        switch (ctx.operator.getType()) {
            case SqlBaseLexer.UNION:
                return new Union(getLocation(ctx.UNION()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.INTERSECT:
                return new Intersect(getLocation(ctx.INTERSECT()), ImmutableList.of(left, right), distinct);
            case SqlBaseLexer.EXCEPT:
                return new Except(getLocation(ctx.EXCEPT()), left, right, distinct);
        }

        throw parseError("Unsupported set operation: " + ctx.operator.getText(), ctx);
    }

    @Override public Node visitCreateDatabase(SqlBaseParser.CreateDatabaseContext ctx)
    {
        List<Property> properties = new ArrayList<>();
        SqlBaseParser.TablePropertyListContext tablePropertyListContext = ctx.tablePropertyList();
        if (tablePropertyListContext != null) {
            List<SqlBaseParser.TablePropertyContext> tablePropertyContexts =
                    tablePropertyListContext.tableProperty();
            for (SqlBaseParser.TablePropertyContext tablePropertyContext: tablePropertyContexts) {
                Property property = (Property) visitTableProperty(tablePropertyContext);
                properties.add(property);
            }
        }
        return new CreateSchema(
                getLocation(ctx),
                getQualifiedName(ctx.identifier().getText()),
                ctx.EXISTS() != null,
                properties);
    }
    @Override public Node visitTableProperty(SqlBaseParser.TablePropertyContext ctx)
    {
        Expression value = null;
        Object type = ctx.value.STRING();
        if (type != null) {
            value = new StringLiteral(tryUnquote(ctx.value.getText()));
        }
        type = ctx.value.booleanValue();
        if (type != null) {
            value = new BooleanLiteral(ctx.value.getText());
        }
        type = ctx.value.DECIMAL_VALUE();
        if (type != null) {
            value = new DecimalLiteral(ctx.value.getText());
        }
        type = ctx.value.INTEGER_VALUE();
        if (type != null) {
            value = new LongLiteral(ctx.value.getText());
        }
        return new Property(getLocation(ctx),
                new Identifier(tryUnquote(ctx.key.getText()), false), value);
    }

    @Override public Node visitDropDatabase(SqlBaseParser.DropDatabaseContext ctx)
    {
        return new DropSchema(
                getLocation(ctx),
                getQualifiedName(ctx.identifier().getText()),
                ctx.EXISTS() != null,
                false);
    }
    @Override public Node visitSetDatabaseProperties(SqlBaseParser.SetDatabasePropertiesContext ctx)
    {
        throw parseError("not support alter properties in presto hive sql", ctx);
    }

    @Override
    public Node visitMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx) {
        throw parseError("Don't support MultiInsertQuery", ctx);
    }

    @Override
    public Node visitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx) {
        TableSubquery child = new TableSubquery(getLocation(ctx), (Query) visit(ctx.queryNoWith()));

        return new AliasedRelation(getLocation(ctx), child, (Identifier) visit(ctx.tableAlias()), null);
    }

    @Override
    public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx) {
        return super.visitAliasedRelation(ctx);
    }

    @Override
    public Node visitConstantList(SqlBaseParser.ConstantListContext ctx) {
        return super.visitConstantList(ctx);
    }

    @Override
    public Node visitCreateHiveTable(SqlBaseParser.CreateHiveTableContext ctx) {
        Optional<String> comment = Optional.empty();
        if (ctx.comment != null && ctx.comment.getText() != null) {
            comment = Optional.of(ctx.comment.getText());
        }
        List<Property> properties = new ArrayList<>();
//        if (ctx.tableProps != null) {
//            List<SqlBaseParser.TablePropertyContext> tablePropertyContexts =
//                    ctx.tableProps.tableProperty();
//            for (SqlBaseParser.TablePropertyContext tablePropertyContext: tablePropertyContexts) {
//                Property property = (Property) visitTableProperty(tablePropertyContext);
//                properties.add(property);
//            }
//        }
        if (ctx.partitionColumns != null) {
            List<SqlBaseParser.ColTypeContext> colTypeListContexts =
                    ctx.partitionColumns.colType();
            List<Expression> partitionedCol = new ArrayList<>();
            for (SqlBaseParser.ColTypeContext colTypeContext: colTypeListContexts) {
                partitionedCol.add(new StringLiteral(tryUnquote(colTypeContext.identifier().getText())));
            }
            Property partitionedBy =
                    new Property(new Identifier("partitioned_by", false),
                            new ArrayConstructor(partitionedCol));
            properties.add(partitionedBy);

        }
        if (ctx.rowFormat() != null && ctx.rowFormat(0) != null) {
            SqlBaseParser.RowFormatContext rowFormatContext = ctx.rowFormat(0);
            if (rowFormatContext instanceof SqlBaseParser.RowFormatSerdeContext) {
                if (((SqlBaseParser.RowFormatSerdeContext) rowFormatContext).name.getText().
                        contains("org.apache.hadoop.hive.ql.io.orc.OrcSerde")) {
                    Property property = new Property(new Identifier("format", false),
                            new StringLiteral("ORC"));
                    properties.add(property);
                }
            }
        }


        List<TableElement> elements = new ArrayList<>();
        List<SqlBaseParser.ColTypeContext> colTypeListContexts = ctx.columns.colType();
        if (ctx.partitionColumns != null) {
            colTypeListContexts.addAll(ctx.partitionColumns.colType());
        }
        if (colTypeListContexts.size() > 0) {
            for (SqlBaseParser.ColTypeContext colTypeContext: colTypeListContexts) {
                Optional colComment = Optional.empty();
                if (colTypeContext.COMMENT() != null) {
                    colComment = Optional.of(colTypeContext.COMMENT().getText());
                }
                TableElement tableElement = new ColumnDefinition(
                        new Identifier(getLocation(colTypeContext),
                                tryUnquote(colTypeContext.identifier().getText()), false),
                        colTypeTransform(colTypeContext.dataType().getText()),
                        true,
                        new ArrayList<>(),
                        colComment);
                elements.add(tableElement);
            }
        }
        return new CreateTable(
                getLocation(ctx),
                getQualifiedName(ctx.createTableHeader().tableIdentifier()),
                elements,
                ctx.createTableHeader().EXISTS() != null,
                properties,
                comment);
    }

    @Override
    public Node visitArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx) {
        return super.visitArithmeticOperator(ctx);
    }

    @Override
    public Node visitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx) {
        return super.visitCreateTableLike(ctx);
    }

    @Override
    public Node visitBooleanValue(SqlBaseParser.BooleanValueContext ctx) {
        return super.visitBooleanValue(ctx);
    }

    @Override
    public Node visitCtes(SqlBaseParser.CtesContext ctx) {
        return super.visitCtes(ctx);
    }

    @Override
    public Node visitExplain(SqlBaseParser.ExplainContext ctx) {
        return super.visitExplain(ctx);
    }

    @Override
    public Node visitFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx) {
        return super.visitFunctionIdentifier(ctx);
    }

    @Override
    public Node visitInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {
        return super.visitInsertIntoTable(ctx);
    }

    @Override
    public Node visitInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx) {
        throw parseError("Don't support InsertOverwriteDir", ctx);
    }

    @Override
    public Node visitInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx) {
        return super.visitInsertOverwriteTable(ctx);
    }
    @Override public Node visitShowDatabases(SqlBaseParser.ShowDatabasesContext ctx)
    {
        return new ShowSchemas(
                getLocation(ctx), Optional.empty(), getTextIfPresent(ctx.pattern)
                        .map(HiveAstBuilder::unquote), Optional.empty());
    }
    @Override public Node visitShowTables(SqlBaseParser.ShowTablesContext ctx)
    {
        Optional<QualifiedName> database;
        if (ctx.db != null) {
            database = Optional.of(getQualifiedName(ctx.db.getText()));
        } else {
            database = Optional.empty();
        }
        return new ShowTables(
                getLocation(ctx), database,getTextIfPresent(ctx.pattern)
                .map(HiveAstBuilder::unquote), Optional.empty());
    }

    @Override
    public Node visitRelation(SqlBaseParser.RelationContext ctx) {
        Relation left = (Relation) visit(ctx.relationPrimary());

        left = withLateralView(left, ctx);

        for (SqlBaseParser.JoinRelationContext joinRelationContext : ctx.joinRelation()) {
            left = withJoinRelation(left, joinRelationContext);
        }

        return left;
    }

    private Relation withLateralView(Relation left, SqlBaseParser.RelationContext ctx) {
        RuleContext parent = ctx.parent;
        if (parent instanceof SqlBaseParser.FromClauseContext) {
            SqlBaseParser.FromClauseContext fromClauseContext = (SqlBaseParser.FromClauseContext) parent;

            List<AliasedRelation> unnests = visit(fromClauseContext.lateralView(), AliasedRelation.class);
            if (unnests.size() >= 1) {
                AliasedRelation unnest = unnests.get(0);
                left = new Join(getLocation(ctx), Join.Type.CROSS, left, unnest, Optional.empty());

                for (int i = 1; i < unnests.size(); ++i) {
                    left = new Join(getLocation(ctx), Join.Type.CROSS, left, unnests.get(i), Optional.empty());
                }
            }
        }

        return left;
    }

    private Relation withJoinRelation(Relation left, SqlBaseParser.JoinRelationContext ctx) {
        if (ctx.joinType().ANTI() != null) {
            throw parseError("Don't support joinType: " + ctx.joinType().getText(), ctx);
        }
        else if (ctx.joinType().SEMI() != null) {
            throw parseError("Don't support joinType: " + ctx.joinType().getText(), ctx);
        }

        Relation right = (Relation) visit(ctx.right);

        if (ctx.joinType().CROSS() != null) {
            return new Join(getLocation(ctx), Join.Type.CROSS, left, right, Optional.empty());
        }

        JoinCriteria criteria;
        if (ctx.NATURAL() != null) {
            criteria = new NaturalJoin();
        }
        else {
            if (ctx.joinCriteria().ON() != null) {
                criteria = new JoinOn((Expression) visit(ctx.joinCriteria().booleanExpression()));
            }
            else if (ctx.joinCriteria().USING() != null) {
                criteria = new JoinUsing(visit(ctx.joinCriteria().identifier(), Identifier.class));
            }
            else {
                throw parseError("Don't support joinCriteria: " + ctx.joinCriteria().getText(), ctx);
            }
        }

        Join.Type joinType;
        if (ctx.joinType().LEFT() != null) {
            joinType = Join.Type.LEFT;
        }
        else if (ctx.joinType().RIGHT() != null) {
            joinType = Join.Type.RIGHT;
        }
        else if (ctx.joinType().FULL() != null) {
            joinType = Join.Type.FULL;
        }
        else {
            joinType = Join.Type.INNER;
        }

        return new Join(getLocation(ctx), joinType, left, right, Optional.of(criteria));
    }

    @Override
    public Node visitIdentifierList(SqlBaseParser.IdentifierListContext ctx) {
        return super.visitIdentifierList(ctx);
    }

    @Override
    public Node visitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx) {
        return super.visitComparisonOperator(ctx);
    }

    @Override
    public Node visitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {
        return super.visitValueExpressionDefault(ctx);
    }

    @Override
    public Node visitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {
        try {
            Number num = NumberFormat.getInstance().parse(ctx.getText());
            // need to make sure it keeps it's original value, e.g. 1.0 is double not integer.
            if (num instanceof Double || !num.toString().equals(ctx.getText())) {
//                switch (parsingOptions.getDecimalLiteralTreatment()) {
//                    case AS_DOUBLE:
                        return new DoubleLiteral(getLocation(ctx), ctx.getText());
//                    case AS_DECIMAL:
//                        return new DecimalLiteral(getLocation(ctx), ctx.getText());
//                    case REJECT:
//                        throw parseError("Unexpected decimal literal: " + ctx.getText(), ctx);
//                }
            } else if (num instanceof Integer || num instanceof Long) {
                return new LongLiteral(getLocation(ctx), ctx.getText());
            }
            else {
                throw parseError("Can't parser number: " + ctx.getText(), ctx);
            }
        } catch (ParseException e) {
            throw parseError("Can't parser number: " + ctx.getText(), ctx);
        }

//        throw parseError("Can't parser number: " + ctx.getText(), ctx);
    }

    @Override
    public Node visitExpression(SqlBaseParser.ExpressionContext ctx) {
        return super.visitExpression(ctx);
    }

    @Override
    public Node visitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx) {
        return super.visitMultiInsertQuery(ctx);
    }

    @Override
    public Node visitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx) {
        return super.visitIdentifierSeq(ctx);
    }

    @Override
    public Node visitFromClause(SqlBaseParser.FromClauseContext ctx) {
        List<Relation> relations = visit(ctx.relation(), Relation.class);

        if (relations.size() > 1) {
            throw parseError("todo", ctx);
        }

        return relations.get(0);
    }

    @Override
    public Node visitLateralView(SqlBaseParser.LateralViewContext ctx) {
        if (ctx.OUTER() != null) {
            throw parseError("Don't support Outer Lateral Views", ctx);
        }

        Identifier qualifiedName = (Identifier) visit(ctx.qualifiedName());
        String udtfName = qualifiedName.getValue().toLowerCase();

        boolean withOrdinality;
        if (udtfName.equals("explode")) {
            withOrdinality = false;
        } else if (udtfName.equals("posexplode")) {
            withOrdinality = true;
        } else {
            throw parseError("Don't support UDTF: " + udtfName, ctx);
        }

        Unnest unnest = new Unnest(getLocation(ctx), visit(ctx.expression(), Expression.class), withOrdinality);

        List<Identifier> columnNames = visit(ctx.colName, Identifier.class);
        if (columnNames.size() > 0) {
            return new AliasedRelation(getLocation(ctx), unnest, (Identifier) visit(ctx.tblName), columnNames);
        } else {
            return new AliasedRelation(getLocation(ctx), unnest, (Identifier) visit(ctx.tblName), null);
        }
    }

    @Override
    public Node visitCreateTable(SqlBaseParser.CreateTableContext ctx) {
        return super.visitCreateTable(ctx);
    }

    @Override
    public Node visitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx) {
        return super.visitConstantDefault(ctx);
    }

    private Node withQueryOrganization(QueryBody term, SqlBaseParser.QueryOrganizationContext ctx) {
        if (ctx.clusterBy != null && !ctx.clusterBy.isEmpty()) {
            throw parseError("Don't support cluster by", ctx);
        }
        if (ctx.distributeBy != null && !ctx.distributeBy.isEmpty()) {
            throw parseError("Don't support distribute by", ctx);
        }
        if (ctx.sort != null && !ctx.sort.isEmpty()) {
            throw parseError("Don't support sort by", ctx);
        }

        Optional<OrderBy> orderBy = Optional.empty();
        if (ctx.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
        }

        Optional<Node> limit = Optional.empty();
        if (ctx.LIMIT() != null) {
            limit = Optional.of(new Limit(Optional.of(getLocation(ctx.LIMIT())), ctx.limit.getText()));
        }

        if (term instanceof QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) term;

            return new Query(
                    getLocation(ctx),
                    Optional.empty(),
                    new QuerySpecification(
                            getLocation(ctx),
                            querySpecification.getSelect(),
                            querySpecification.getFrom(),
                            querySpecification.getWhere(),
                            querySpecification.getGroupBy(),
                            querySpecification.getHaving(),
                            orderBy,
                            Optional.empty(),
                            limit),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        } else {
            return new Query(
                    getLocation(ctx),
                    Optional.empty(),
                    term,
                    orderBy,
                    Optional.empty(),
                    limit);
        }
    }

    @Override
    public Node visitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx) {
        QueryBody term = (QueryBody) visit(ctx.queryTerm());

        return withQueryOrganization(term, ctx.queryOrganization());
    }

    @Override
    public Node visitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx) {
        return super.visitSingleDataType(ctx);
    }

    @Override
    public Node visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        return visit(ctx.statement());
    }

    @Override
    public Node visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        return super.visitQueryTermDefault(ctx);
    }

    @Override
    public Node visitSingleExpression(SqlBaseParser.SingleExpressionContext ctx) {
        return visit(ctx.namedExpression());
    }

    @Override
    public Node visitStar(SqlBaseParser.StarContext ctx) {
        return new StarExpression(getLocation(ctx), visitIfPresent(ctx.qualifiedName(), Identifier.class));
    }

    @Override
    public Node visitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {
        NodeLocation nodeLocation = getLocation(ctx);
        Expression expression = (Expression)visit(ctx.expression());
        Optional<Identifier> identifier = visitIfPresent(ctx.identifier(), Identifier.class);

        if (expression instanceof StarExpression) {
            StarExpression starExpression = (StarExpression) expression;
            // select all
            if (identifier.isPresent()) {
                throw parseError("todo", ctx);
            }

            if (starExpression.getIdentifier().isPresent()) {
                return new AllColumns(nodeLocation, getQualifiedName(starExpression.getIdentifier().get()));
            } else {
                return new AllColumns(nodeLocation);
            }
        } else {
            return new SingleColumn(nodeLocation, expression, identifier);
        }
    }

    @Override
    public Node visitUse(SqlBaseParser.UseContext ctx) {
        Use use;
        if (ctx.catalog != null) {
            use = new Use(
                    getLocation(ctx),
                    Optional.of(new Identifier(getLocation(ctx), ctx.catalog.getText(), false)),
                    new Identifier(getLocation(ctx), ctx.db.getText(), false));
            visit(ctx.catalog);
        } else {
            use = new Use(
                    getLocation(ctx),
                    Optional.ofNullable(null),
                    new Identifier(getLocation(ctx), ctx.db.getText(), false));
        }
        visit(ctx.db);
        return use;
    }

    @Override
    public Node visitSetSession(SqlBaseParser.SetSessionContext ctx) {
        SqlBaseParser.ExpressionContext expression = ctx.expression();
        SetSession setSession = new SetSession(getLocation(ctx),
                getQualifiedName(ctx.qualifiedName()), (Expression) visit(expression));
        return setSession;
    }

    @Override
    public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx) {
        return new LogicalBinaryExpression(
                getLocation(ctx.operator),
                getLogicalBinaryOperator(ctx.operator),
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx) {
        return new Table(getLocation(ctx), getQualifiedName(ctx));
    }

    @Override
    public Node visitTableName(SqlBaseParser.TableNameContext ctx) {
        Table table = (Table) visitTableIdentifier(ctx.tableIdentifier());

        if (ctx.tableAlias() != null && ctx.tableAlias().strictIdentifier() != null) {
            Identifier identifier = (Identifier) visit(ctx.tableAlias().strictIdentifier());

            List<Identifier> aliases = null;
            if (ctx.tableAlias().identifierList() != null) {
                throw parseError("todo", ctx);
                //aliases = visit(ctx.tableAlias().identifierList(), Identifier.class);
            }

            return new AliasedRelation(getLocation(ctx), table, identifier, aliases);
        } else {
            return table;
        }
    }

    @Override public Node visitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx)
    {
        throw parseError("show create table not support yet!", ctx);
    }

    @Override
    public Node visitAggregation(SqlBaseParser.AggregationContext ctx) {
        List<GroupingElement> groupingElements = new ArrayList<>();

        if (ctx.GROUPING() != null) {
            // GROUP BY .... GROUPING SETS (...)
            List<List<Expression>> expresstionLists = ctx.groupingSet().stream().map(groupingSet -> visit(groupingSet.expression(), Expression.class)).collect(toList());

            groupingElements.add(new GroupingSets(getLocation(ctx), expresstionLists));
        } else {
            // GROUP BY .... (WITH CUBE | WITH ROLLUP)?
            if (ctx.CUBE() != null) {
                GroupingElement groupingElement = new Cube(getLocation(ctx), visit(ctx.groupingExpressions, Expression.class));
                groupingElements.add(groupingElement);
            } else if (ctx.ROLLUP() != null) {
                GroupingElement groupingElement = new Rollup(getLocation(ctx), visit(ctx.groupingExpressions, Expression.class));
                groupingElements.add(groupingElement);
            } else {
                // this is a little bit awkward just trying to match whatever presto going to generate.
                for (SqlBaseParser.ExpressionContext groupingExpression : ctx.groupingExpressions) {
                    GroupingElement groupingElement = new SimpleGroupBy(getLocation(ctx), visit(ImmutableList.of(groupingExpression), Expression.class));
                    groupingElements.add(groupingElement);
                }
            }
        }

        return new GroupBy(getLocation(ctx), false, groupingElements);
    }

    @Override
    public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext ctx) {
        if (ctx.kind.getType()  == SqlBaseParser.SELECT) {
            SqlBaseParser.NamedExpressionSeqContext namedExpressionSeqContext = ctx.namedExpressionSeq();
            List<SqlBaseParser.NamedExpressionContext> namedExpressionContexts =
                    namedExpressionSeqContext.namedExpression();

            List<SelectItem> selectItems = new ArrayList<>();
            for (SqlBaseParser.NamedExpressionContext namedExpressionContext: namedExpressionContexts) {
                SelectItem selectItem = (SelectItem)visit(namedExpressionContext);
                selectItems.add(selectItem);
            }

            NodeLocation nodeLocation = getLocation(ctx);
            Select select = new Select(getLocation(ctx.SELECT()), isDistinct(ctx.setQuantifier()), selectItems);

            Optional<Relation> from = visitIfPresent(ctx.fromClause(), Relation.class);

            return new QuerySpecification(
                    nodeLocation,
                    select,
                    from,
                    visitIfPresent(ctx.where, Expression.class),
                    visitIfPresent(ctx.aggregation(), GroupBy.class),
                    visitIfPresent(ctx.having, Expression.class),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        } else {
            throw parseError("Don't support kind: " + ctx.kind.getText(), ctx);
        }
    }

    private static boolean isDistinct(io.hivesql.sql.parser.SqlBaseParser.SetQuantifierContext setQuantifier)
    {
        return setQuantifier != null && setQuantifier.DISTINCT() != null;
    }

    @Override
    public Node visitLogicalNot(SqlBaseParser.LogicalNotContext ctx) {
        return new NotExpression(getLocation(ctx), (Expression) visit(ctx.booleanExpression()));
    }

    @Override
    public Node visitExists(SqlBaseParser.ExistsContext ctx) {
        return new ExistsPredicate(getLocation(ctx), new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));
    }

    @Override
    public Node visitComparison(SqlBaseParser.ComparisonContext ctx) {
        return new ComparisonExpression(
                getLocation(ctx.comparisonOperator()),
                getComparisonOperator(((TerminalNode) ctx.comparisonOperator().getChild(0)).getSymbol()),
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    private Node withPredicate(Expression expression, SqlBaseParser.PredicateContext ctx) {
        switch (ctx.kind.getType()) {
            case SqlBaseParser.NULL:
                if (ctx.NOT() != null) {
                    return new IsNotNullPredicate(getLocation(ctx), expression);
                } else {
                    return new IsNullPredicate(getLocation(ctx), expression);
                }
            case SqlBaseParser.EXISTS:
                return new ExistsPredicate(getLocation(ctx), new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));
            case SqlBaseParser.BETWEEN:
                Expression betweenPredicate = new BetweenPredicate(
                        getLocation(ctx),
                        expression,
                        (Expression) visit(ctx.lower),
                        (Expression) visit(ctx.upper));

                if (ctx.NOT() != null) {
                    return new NotExpression(getLocation(ctx), betweenPredicate);
                } else {
                    return betweenPredicate;
                }
            case SqlBaseParser.IN:
                if (ctx.query() != null) {
                    // In subquery
                    Expression inPredicate = new InPredicate(
                            getLocation(ctx),
                            expression,
                            new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query())));

                    if (ctx.NOT() != null) {
                        return new NotExpression(getLocation(ctx), inPredicate);
                    } else {
                        return inPredicate;
                    }
                } else {
                    // In value list
                    InListExpression inListExpression = new InListExpression(getLocation(ctx), visit(ctx.expression(), Expression.class));
                    Expression inPredicate = new InPredicate(
                            getLocation(ctx),
                            expression,
                            inListExpression);

                    if (ctx.NOT() != null) {
                        return new NotExpression(getLocation(ctx), inPredicate);
                    } else {
                        return inPredicate;
                    }
                }
            case SqlBaseParser.LIKE:
                Expression likePredicate = new LikePredicate(
                        getLocation(ctx),
                        expression,
                        (Expression) visit(ctx.pattern),
                        Optional.empty());

                if (ctx.NOT() != null) {
                    return new NotExpression(getLocation(ctx), likePredicate);
                } else {
                    return likePredicate;
                }
            case SqlBaseParser.DISTINCT:
                Expression comparisonExpression = new ComparisonExpression(
                        getLocation(ctx),
                        ComparisonExpression.Operator.IS_DISTINCT_FROM,
                        expression,
                        (Expression) visit(ctx.right));

                if (ctx.NOT() != null) {
                    return new NotExpression(getLocation(ctx), comparisonExpression);
                } else {
                    return comparisonExpression;
                }
             case SqlBaseParser.RLIKE:
                 Expression rLikePredicate = new RLikePredicate(
                         getLocation(ctx),
                         expression,
                         (Expression) visit(ctx.pattern),
                         Optional.empty());

                 if (ctx.NOT() != null) {
                     return new NotExpression(getLocation(ctx), rLikePredicate);
                 } else {
                     return rLikePredicate;
                 }
            default:
                throw parseError("Not supported type: " + ctx.kind.getText(), ctx);
        }
    }

    @Override
    public Node visitPredicated(SqlBaseParser.PredicatedContext ctx) {
        Expression expression = (Expression) visit(ctx.valueExpression());

        if (ctx.predicate() != null) {
            return withPredicate(expression, ctx.predicate());
        }

        return expression;
    }

    @Override
    public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx) {
        return new ArithmeticBinaryExpression(
                getLocation(ctx.operator),
                getArithmeticBinaryOperator(ctx.operator),
                (Expression) visit(ctx.left),
                (Expression) visit(ctx.right));
    }

    @Override
    public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx) {
        Expression child = (Expression) visit(ctx.valueExpression());

        switch (ctx.operator.getType()) {
            case SqlBaseLexer.MINUS:
                return ArithmeticUnaryExpression.negative(getLocation(ctx), child);
            case SqlBaseLexer.PLUS:
                return ArithmeticUnaryExpression.positive(getLocation(ctx), child);
            default:
                throw new UnsupportedOperationException("Unsupported sign: " + ctx.operator.getText());
        }
    }

    @Override
    public Node visitCast(SqlBaseParser.CastContext ctx) {
        return new Cast(getLocation(ctx), (Expression) visit(ctx.expression()), getType(ctx.dataType()), false);
    }

    // need to be implemented
    @Override
    public Node visitStruct(SqlBaseParser.StructContext ctx) {
        return super.visitStruct(ctx);
    }

    // need to be implemented
    @Override
    public Node visitFirst(SqlBaseParser.FirstContext ctx) {
        return super.visitFirst(ctx);
    }

    // need to be implemented
    @Override
    public Node visitLast(SqlBaseParser.LastContext ctx) {
        return super.visitLast(ctx);
    }

    @Override
    public Node visitPosition(SqlBaseParser.PositionContext ctx) {
        List<Expression> arguments = Lists.reverse(visit(ctx.valueExpression(), Expression.class));
        return new FunctionCall(getLocation(ctx), QualifiedName.of("strpos"), arguments);
    }

    @Override
    public Node visitExtract(SqlBaseParser.ExtractContext ctx) {
        String fieldString = ctx.identifier().getText();
        Extract.Field field;
        try {
            field = Extract.Field.valueOf(fieldString.toUpperCase());
        }
        catch (IllegalArgumentException e) {
            throw parseError("Invalid EXTRACT field: " + fieldString, ctx);
        }
        return new Extract(getLocation(ctx), (Expression) visit(ctx.valueExpression()), field);
    }

    @Override
    public Node visitFunctionCall(SqlBaseParser.FunctionCallContext ctx) {
        QualifiedName name = getQualifiedName(ctx.qualifiedName());
        boolean distinct = isDistinct(ctx.setQuantifier());

        Optional<Window> window = visitIfPresent(ctx.windowSpec(), Window.class);

        if (name.toString().equalsIgnoreCase("if")) {
            check(ctx.expression().size() == 2 || ctx.expression().size() == 3, "Invalid number of arguments for 'if' function", ctx);
            check(!window.isPresent(), "OVER clause not valid for 'if' function", ctx);
            check(!distinct, "DISTINCT not valid for 'if' function", ctx);

            Expression elseExpression = null;
            if (ctx.expression().size() == 3) {
                elseExpression = (Expression) visit(ctx.expression(2));
            }

            return new IfExpression(
                    getLocation(ctx),
                    (Expression) visit(ctx.expression(0)),
                    (Expression) visit(ctx.expression(1)),
                    elseExpression);
        }

        if (name.toString().equalsIgnoreCase("nullif")) {
            check(ctx.expression().size() == 2, "Invalid number of arguments for 'nullif' function", ctx);
            check(!window.isPresent(), "OVER clause not valid for 'nullif' function", ctx);
            check(!distinct, "DISTINCT not valid for 'nullif' function", ctx);

            return new NullIfExpression(
                    getLocation(ctx),
                    (Expression) visit(ctx.expression(0)),
                    (Expression) visit(ctx.expression(1)));
        }

        if (name.toString().equalsIgnoreCase("coalesce")) {
            check(ctx.expression().size() >= 2, "The 'coalesce' function must have at least two arguments", ctx);
            check(!window.isPresent(), "OVER clause not valid for 'coalesce' function", ctx);
            check(!distinct, "DISTINCT not valid for 'coalesce' function", ctx);

            return new CoalesceExpression(getLocation(ctx), visit(ctx.expression(), Expression.class));
        }

        return new FunctionCall(
                Optional.of(getLocation(ctx)),
                name,
                window,
                Optional.empty(),//filter,
                Optional.empty(),//orderBy,
                distinct,
                visit(ctx.expression(), Expression.class));
    }

    @Override
    public Node visitWindowRef(SqlBaseParser.WindowRefContext ctx) {
        throw parseError("Don't support Window Clause", ctx);
    }

    @Override
    public Node visitWindowDef(SqlBaseParser.WindowDefContext ctx) {
        if (ctx.SORT() != null) {
            throw parseError("Don't support SORT", ctx);
        }
        if (ctx.CLUSTER() != null) {
            throw parseError("Don't support CLUSTER", ctx);
        }
        if (ctx.DISTRIBUTE() != null) {
            throw parseError("Don't support DISTRIBUTE", ctx);
        }

        Optional<OrderBy> orderBy = Optional.empty();
        if (ctx.ORDER() != null) {
            orderBy = Optional.of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
        }

        return new Window(
                getLocation(ctx),
                visit(ctx.partition, Expression.class),
                orderBy,
                visitIfPresent(ctx.windowFrame(), WindowFrame.class));
    }

    private static WindowFrame.Type getFrameType(ParserRuleContext ctx, Token type)
    {
        switch (type.getType()) {
            case SqlBaseLexer.RANGE:
                return WindowFrame.Type.RANGE;
            case SqlBaseLexer.ROWS:
                return WindowFrame.Type.ROWS;
        }

        throw parseError("Don't support frame type: " + type.getText(), ctx);
    }

    private static FrameBound.Type getBoundedFrameBoundType(ParserRuleContext ctx, Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.FOLLOWING;
            case SqlBaseLexer.CURRENT:
                return FrameBound.Type.CURRENT_ROW;
        }

        throw parseError("Don't support bound type: " + token.getText(), ctx);
    }

    private static FrameBound.Type getUnboundedFrameBoundType(ParserRuleContext ctx, Token token)
    {
        switch (token.getType()) {
            case SqlBaseLexer.PRECEDING:
                return FrameBound.Type.UNBOUNDED_PRECEDING;
            case SqlBaseLexer.FOLLOWING:
                return FrameBound.Type.UNBOUNDED_FOLLOWING;
        }

        throw parseError("Don't support bound type: " + token.getText(), ctx);
    }

    @Override
    public Node visitWindowFrame(SqlBaseParser.WindowFrameContext ctx) {
        return new WindowFrame(
                getLocation(ctx),
                getFrameType(ctx, ctx.frameType),
                (FrameBound) visit(ctx.start),
                visitIfPresent(ctx.end, FrameBound.class));
    }

    @Override
    public Node visitFrameBound(SqlBaseParser.FrameBoundContext ctx) {
        Expression expression = null;
        if (ctx.expression() != null) {
            expression = (Expression) visit(ctx.expression());
        }

        FrameBound.Type frameBoundType = null;
        if (ctx.UNBOUNDED() == null) {
            frameBoundType = getBoundedFrameBoundType(ctx, ctx.boundType);
        } else {
            frameBoundType = getUnboundedFrameBoundType(ctx, ctx.boundType);
        }

        return new FrameBound(getLocation(ctx), frameBoundType, expression);
    }

    @Override
    public Node visitRowConstructor(SqlBaseParser.RowConstructorContext ctx) {
        return new Row(getLocation(ctx), visit(ctx.namedExpression(), Expression.class));
    }

    @Override
    public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx) {
        return new SubqueryExpression(getLocation(ctx), (Query) visit(ctx.query()));
    }

    @Override
    public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext ctx) {
        return new SimpleCaseExpression(
                getLocation(ctx),
                (Expression) visit(ctx.value),
                visit(ctx.whenClause(), WhenClause.class),
                visitIfPresent(ctx.elseExpression, Expression.class));
    }

    @Override
    public Node visitWhenClause(SqlBaseParser.WhenClauseContext ctx) {
        return new WhenClause(getLocation(ctx), (Expression) visit(ctx.condition), (Expression) visit(ctx.result));
    }

    @Override
    public Node visitSearchedCase(SqlBaseParser.SearchedCaseContext ctx) {
        return new SearchedCaseExpression(
                getLocation(ctx),
                visit(ctx.whenClause(), WhenClause.class),
                visitIfPresent(ctx.elseExpression, Expression.class));
    }

    @Override
    public Node visitDereference(SqlBaseParser.DereferenceContext ctx) {
        return new DereferenceExpression(
                getLocation(ctx),
                (Expression) visit(ctx.base),
                (Identifier) visit(ctx.fieldName));
    }

    @Override
    public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx) {
        return new Identifier(getLocation(ctx), unquote(ctx.getText()), true);
    }

    @Override
    public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx) {
        return new Identifier(getLocation(ctx), ctx.getText(), false);
    }

    @Override
    public Node visitSubscript(SqlBaseParser.SubscriptContext ctx) {
        return new SubscriptExpression(getLocation(ctx),
                (Expression) visit(ctx.value), (Expression) visit(ctx.index));
    }

    @Override
    public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public Node visitSortItem(SqlBaseParser.SortItemContext ctx) {
        return new SortItem(
                getLocation(ctx),
                (Expression) visit(ctx.expression()),
                Optional.ofNullable(ctx.ordering)
                        .map(HiveAstBuilder::getOrderingType)
                        .orElse(SortItem.Ordering.ASCENDING),
                Optional.ofNullable(ctx.nullOrder)
                        .map(HiveAstBuilder::getNullOrderingType)
                        .orElse(SortItem.NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx) {

        String value = ((StringLiteral) visit(ctx.STRING())).getValue();

        String type = ctx.identifier().getText();
        if (type.equalsIgnoreCase("time")) {
            return new TimeLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("timestamp")) {
            return new TimestampLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("decimal")) {
            return new DecimalLiteral(getLocation(ctx), value);
        }
        if (type.equalsIgnoreCase("char")) {
            return new CharLiteral(getLocation(ctx), value);
        }

        return new GenericLiteral(getLocation(ctx), type, value);
    }

    @Override
    public Node visitNullLiteral(SqlBaseParser.NullLiteralContext ctx) {
        return new NullLiteral(getLocation(ctx));
    }

    @Override
    public Node visitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx) {
        return new BooleanLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx) {
        return new LongLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx) {
        return new DoubleLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx) {
        return new DoubleLiteral(getLocation(ctx), ctx.getText());
    }

    @Override
    public Node visitStringLiteral(SqlBaseParser.StringLiteralContext ctx) {
        return new StringLiteral(getLocation(ctx), unquote(ctx.getText()));
    }

    /////////////////////
    // Utility methods //
    /////////////////////
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

    private static ParsingException parseError(String message, ParserRuleContext context)
    {
        return new ParsingException(message, null, context.getStart().getLine(), context.getStart().getCharPositionInLine());
    }

    private String getType(SqlBaseParser.DataTypeContext type)
    {
        if (type instanceof SqlBaseParser.ComplexDataTypeContext) {
            SqlBaseParser.ComplexDataTypeContext complexType = (SqlBaseParser.ComplexDataTypeContext)type;

            if (complexType.ARRAY() != null) {
                return "ARRAY(" + getType((complexType.dataType(0))) + ")";
            }

            if (complexType.MAP() != null) {
                return "MAP(" + getType((complexType.dataType(0))) + "," + getType((complexType.dataType(1))) + ")";
            }

            if (complexType.STRUCT() != null) {
                StringBuilder builder = new StringBuilder("(");

                for (int i = 0; i < complexType.complexColTypeList().complexColType().size(); i++) {
                    SqlBaseParser.ComplexColTypeContext complexColTypeContext = complexType.complexColTypeList().complexColType().get(i);

                    if (i != 0) {
                        builder.append(",");
                    }

                    builder.append(visit(complexColTypeContext.identifier()))
                            .append(" ")
                            .append(getType(complexColTypeContext.dataType()));
                }
                builder.append(")");
                return "ROW" + builder.toString();
            }
        } else {
            return type.getText();
        }

        throw parseError("Don't support type specification: " + type.getText(), type);
    }

    private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator)
    {
        switch (operator.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.PLUS:
                return ArithmeticBinaryExpression.Operator.ADD;
            case io.hivesql.sql.parser.SqlBaseLexer.MINUS:
                return ArithmeticBinaryExpression.Operator.SUBTRACT;
            case io.hivesql.sql.parser.SqlBaseLexer.ASTERISK:
                return ArithmeticBinaryExpression.Operator.MULTIPLY;
            case io.hivesql.sql.parser.SqlBaseLexer.SLASH:
                return ArithmeticBinaryExpression.Operator.DIVIDE;
            case io.hivesql.sql.parser.SqlBaseLexer.PERCENT:
                return ArithmeticBinaryExpression.Operator.MODULUS;
            case io.hivesql.sql.parser.SqlBaseLexer.DIV:
                return ArithmeticBinaryExpression.Operator.DIV;
            case io.hivesql.sql.parser.SqlBaseLexer.PIPE:
                return ArithmeticBinaryExpression.Operator.PIPE;
            case io.hivesql.sql.parser.SqlBaseLexer.TILDE:
                return ArithmeticBinaryExpression.Operator.TILDE;
            case io.hivesql.sql.parser.SqlBaseLexer.AMPERSAND:
                return ArithmeticBinaryExpression.Operator.AMPERSAND;
            case io.hivesql.sql.parser.SqlBaseLexer.CONCAT_PIPE:
                return ArithmeticBinaryExpression.Operator.CONCAT_PIPE;
            case io.hivesql.sql.parser.SqlBaseLexer.HAT:
                return ArithmeticBinaryExpression.Operator.HAT;
        }

        throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
    }

    private static ComparisonExpression.Operator getComparisonOperator(Token symbol)
    {
        switch (symbol.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.EQ:
                return ComparisonExpression.Operator.EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.NSEQ:
                return ComparisonExpression.Operator.EQNSF;
            case io.hivesql.sql.parser.SqlBaseLexer.NEQ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.NEQJ:
                return ComparisonExpression.Operator.NOT_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.LT:
                return ComparisonExpression.Operator.LESS_THAN;
            case io.hivesql.sql.parser.SqlBaseLexer.LTE:
                return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
            case io.hivesql.sql.parser.SqlBaseLexer.GT:
                return ComparisonExpression.Operator.GREATER_THAN;
            case io.hivesql.sql.parser.SqlBaseLexer.GTE:
                return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
        }

        throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
    }

    private static LogicalBinaryExpression.Operator getLogicalBinaryOperator(Token token)
    {
        switch (token.getType()) {
            case io.hivesql.sql.parser.SqlBaseLexer.AND:
                return LogicalBinaryExpression.Operator.AND;
            case io.hivesql.sql.parser.SqlBaseLexer.OR:
                return LogicalBinaryExpression.Operator.OR;
        }

        throw new IllegalArgumentException("Unsupported operator: " + token.getText());
    }

    private QualifiedName getQualifiedName(Identifier identifier)
    {
        return QualifiedName.of(ImmutableList.of(identifier));
    }

    private QualifiedName getQualifiedName(SqlBaseParser.TableIdentifierContext context)
    {
        List<Identifier> identifiers = new ArrayList<>();
        for (SqlBaseParser.IdentifierContext identifierContext: context.identifier()) {
            String qualifiedName = identifierContext.getText();
            String[] tmp = tryUnquote(qualifiedName).split("\\.");

            for (String id: tmp) {
                Identifier identifier =
                        new Identifier(getLocation(identifierContext),
                                id, false);
                identifiers.add(identifier);
            }
        }
        return QualifiedName.of(identifiers);
    }

    private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context)
    {
        List<Identifier> identifiers = new ArrayList<>();
        for (SqlBaseParser.IdentifierContext identifierContext: context.identifier()) {
            Identifier identifier =
                    new Identifier(getLocation(identifierContext),
                            identifierContext.getText(), false);
            identifiers.add(identifier);
            visit(identifierContext);
        }
        return QualifiedName.of(identifiers);
    }

    private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz)
    {
        return contexts.stream()
                .map(this::visit)
                .map(clazz::cast)
                .collect(toList());
    }

    public Node visitUnquotedIdentifier(io.prestosql.sql.parser.SqlBaseParser.UnquotedIdentifierContext context)
    {
        return new Identifier(getLocation(context), context.getText(), false);
    }

    private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz)
    {
        return Optional.ofNullable(context)
                .map(this::visit)
                .map(clazz::cast);
    }

    private static Optional<String> getTextIfPresent(Token token)
    {
        return Optional.ofNullable(token)
                .map(Token::getText);
    }

    public static NodeLocation getLocation(ParserRuleContext parserRuleContext)
    {
        requireNonNull(parserRuleContext, "parserRuleContext is null");
        return getLocation(parserRuleContext.getStart());
    }

    public static NodeLocation getLocation(TerminalNode terminalNode)
    {
        requireNonNull(terminalNode, "terminalNode is null");
        return getLocation(terminalNode.getSymbol());
    }

    public static NodeLocation getLocation(Token token)
    {
        requireNonNull(token, "token is null");
        return new NodeLocation(token.getLine(), token.getCharPositionInLine());
    }

    private static void check(boolean condition, String message, ParserRuleContext context)
    {
        if (!condition) {
            throw parseError(message, context);
        }
    }

    private static QualifiedName getQualifiedName(String qualifiedName)
    {
        if (qualifiedName == null) {
            return null;
        }
        String[] tmp = qualifiedName.replace("`", "").
                replace("\"", "").split("\\.");
        if (tmp.length == 0) {
            return null;
        }
        return QualifiedName.of(tmp[0], Arrays.copyOfRange(tmp, 1, tmp.length));
    }


    /**
     * this will remove single quotation, double quotation and backtick.
     */
    public static String unquote(String value)
    {
        return value.substring(1, value.length() - 1)
                .replace("''", "'");
    }

    public static String tryUnquote(String value)
    {
        if (value == null) {
            return null;
        }
        return value.replace("\'", "").
                replace("\"", "").
                replace("`", "");
    }

    public static String colTypeTransform(String hiveColType) {
        return hiveColType.
                replace("int", "integer").
                replace("biginteger", "bigint").
                replace("string", "varchar");
    }

}
