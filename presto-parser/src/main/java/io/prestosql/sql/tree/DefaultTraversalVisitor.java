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
package io.prestosql.sql.tree;

public abstract class DefaultTraversalVisitor<C>
        extends AstVisitor<Void, C>
{
    @Override
    protected Void visitExtract(Extract node, C context)
    {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected Void visitCast(Cast node, C context)
    {
        process(node.getExpression(), context);
        return null;
    }

    @Override
    protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected Void visitBetweenPredicate(BetweenPredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getMin(), context);
        process(node.getMax(), context);

        return null;
    }

    @Override
    protected Void visitCoalesceExpression(CoalesceExpression node, C context)
    {
        for (Expression operand : node.getOperands()) {
            process(operand, context);
        }

        return null;
    }

    @Override
    protected Void visitAtTimeZone(AtTimeZone node, C context)
    {
        process(node.getValue(), context);
        process(node.getTimeZone(), context);

        return null;
    }

    @Override
    protected Void visitArrayConstructor(ArrayConstructor node, C context)
    {
        for (Expression expression : node.getValues()) {
            process(expression, context);
        }

        return null;
    }

    @Override
    protected Void visitSubscriptExpression(SubscriptExpression node, C context)
    {
        process(node.getBase(), context);
        process(node.getIndex(), context);

        return null;
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected Void visitFormat(Format node, C context)
    {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

        return null;
    }

    @Override
    protected Void visitQuery(Query node, C context)
    {
        if (node.getWith().isPresent()) {
            process(node.getWith().get(), context);
        }
        process(node.getQueryBody(), context);
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        if (node.getOffset().isPresent()) {
            process(node.getOffset().get(), context);
        }
        if (node.getLimit().isPresent()) {
            process(node.getLimit().get(), context);
        }

        return null;
    }

    @Override
    protected Void visitWith(With node, C context)
    {
        for (WithQuery query : node.getQueries()) {
            process(query, context);
        }

        return null;
    }

    @Override
    protected Void visitWithQuery(WithQuery node, C context)
    {
        process(node.getQuery(), context);
        return null;
    }

    @Override
    protected Void visitSelect(Select node, C context)
    {
        for (SelectItem item : node.getSelectItems()) {
            process(item, context);
        }

        return null;
    }

    @Override
    protected Void visitSingleColumn(SingleColumn node, C context)
    {
        process(node.getExpression(), context);

        return null;
    }

    @Override
    protected Void visitAllColumns(AllColumns node, C context)
    {
        node.getTarget().ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected Void visitWhenClause(WhenClause node, C context)
    {
        process(node.getOperand(), context);
        process(node.getResult(), context);

        return null;
    }

    @Override
    protected Void visitInPredicate(InPredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getValueList(), context);

        return null;
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, C context)
    {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        if (node.getWindow().isPresent()) {
            process(node.getWindow().get(), context);
        }

        if (node.getFilter().isPresent()) {
            process(node.getFilter().get(), context);
        }

        return null;
    }

    @Override
    protected Void visitGroupingOperation(GroupingOperation node, C context)
    {
        for (Expression columnArgument : node.getGroupingColumns()) {
            process(columnArgument, context);
        }

        return null;
    }

    @Override
    protected Void visitDereferenceExpression(DereferenceExpression node, C context)
    {
        process(node.getBase(), context);
        return null;
    }

    @Override
    public Void visitWindow(Window node, C context)
    {
        for (Expression expression : node.getPartitionBy()) {
            process(expression, context);
        }

        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }

        if (node.getFrame().isPresent()) {
            process(node.getFrame().get(), context);
        }

        return null;
    }

    @Override
    public Void visitWindowFrame(WindowFrame node, C context)
    {
        process(node.getStart(), context);
        if (node.getEnd().isPresent()) {
            process(node.getEnd().get(), context);
        }

        return null;
    }

    @Override
    public Void visitFrameBound(FrameBound node, C context)
    {
        if (node.getValue().isPresent()) {
            process(node.getValue().get(), context);
        }

        return null;
    }

    @Override
    protected Void visitOffset(Offset node, C context)
    {
        process(node.getRowCount());

        return null;
    }

    @Override
    protected Void visitLimit(Limit node, C context)
    {
        process(node.getRowCount());

        return null;
    }

    @Override
    protected Void visitFetchFirst(FetchFirst node, C context)
    {
        node.getRowCount().ifPresent(this::process);

        return null;
    }

    @Override
    protected Void visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        process(node.getOperand(), context);
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }

        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected Void visitInListExpression(InListExpression node, C context)
    {
        for (Expression value : node.getValues()) {
            process(value, context);
        }

        return null;
    }

    @Override
    protected Void visitNullIfExpression(NullIfExpression node, C context)
    {
        process(node.getFirst(), context);
        process(node.getSecond(), context);

        return null;
    }

    @Override
    protected Void visitIfExpression(IfExpression node, C context)
    {
        process(node.getCondition(), context);
        process(node.getTrueValue(), context);
        if (node.getFalseValue().isPresent()) {
            process(node.getFalseValue().get(), context);
        }

        return null;
    }

    @Override
    protected Void visitTryExpression(TryExpression node, C context)
    {
        process(node.getInnerExpression(), context);
        return null;
    }

    @Override
    protected Void visitBindExpression(BindExpression node, C context)
    {
        for (Expression value : node.getValues()) {
            process(value, context);
        }
        process(node.getFunction(), context);

        return null;
    }

    @Override
    protected Void visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        process(node.getValue(), context);
        return null;
    }

    @Override
    protected Void visitNotExpression(NotExpression node, C context)
    {
        process(node.getValue(), context);
        return null;
    }

    @Override
    protected Void visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        for (WhenClause clause : node.getWhenClauses()) {
            process(clause, context);
        }
        node.getDefaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected Void visitLikePredicate(LikePredicate node, C context)
    {
        process(node.getValue(), context);
        process(node.getPattern(), context);
        node.getEscape().ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected Void visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        process(node.getValue(), context);
        return null;
    }

    @Override
    protected Void visitIsNullPredicate(IsNullPredicate node, C context)
    {
        process(node.getValue(), context);
        return null;
    }

    @Override
    protected Void visitLogicalBinaryExpression(LogicalBinaryExpression node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        return null;
    }

    @Override
    protected Void visitSubqueryExpression(SubqueryExpression node, C context)
    {
        process(node.getQuery(), context);
        return null;
    }

    @Override
    protected Void visitOrderBy(OrderBy node, C context)
    {
        for (SortItem sortItem : node.getSortItems()) {
            process(sortItem, context);
        }
        return null;
    }

    @Override
    protected Void visitSortItem(SortItem node, C context)
    {
        process(node.getSortKey(), context);
        return null;
    }

    @Override
    protected Void visitQuerySpecification(QuerySpecification node, C context)
    {
        process(node.getSelect(), context);
        if (node.getFrom().isPresent()) {
            process(node.getFrom().get(), context);
        }
        if (node.getWhere().isPresent()) {
            process(node.getWhere().get(), context);
        }
        if (node.getGroupBy().isPresent()) {
            process(node.getGroupBy().get(), context);
        }
        if (node.getHaving().isPresent()) {
            process(node.getHaving().get(), context);
        }
        if (node.getOrderBy().isPresent()) {
            process(node.getOrderBy().get(), context);
        }
        if (node.getOffset().isPresent()) {
            process(node.getOffset().get(), context);
        }
        if (node.getLimit().isPresent()) {
            process(node.getLimit().get(), context);
        }
        return null;
    }

    @Override
    protected Void visitSetOperation(SetOperation node, C context)
    {
        for (Relation relation : node.getRelations()) {
            process(relation, context);
        }
        return null;
    }

    @Override
    protected Void visitValues(Values node, C context)
    {
        for (Expression row : node.getRows()) {
            process(row, context);
        }
        return null;
    }

    @Override
    protected Void visitRow(Row node, C context)
    {
        for (Expression expression : node.getItems()) {
            process(expression, context);
        }
        return null;
    }

    @Override
    protected Void visitTableSubquery(TableSubquery node, C context)
    {
        process(node.getQuery(), context);
        return null;
    }

    @Override
    protected Void visitAliasedRelation(AliasedRelation node, C context)
    {
        process(node.getRelation(), context);
        return null;
    }

    @Override
    protected Void visitSampledRelation(SampledRelation node, C context)
    {
        process(node.getRelation(), context);
        process(node.getSamplePercentage(), context);
        return null;
    }

    @Override
    protected Void visitJoin(Join node, C context)
    {
        process(node.getLeft(), context);
        process(node.getRight(), context);

        node.getCriteria()
                .filter(criteria -> criteria instanceof JoinOn)
                .map(criteria -> process(((JoinOn) criteria).getExpression(), context));

        return null;
    }

    @Override
    protected Void visitUnnest(Unnest node, C context)
    {
        for (Expression expression : node.getExpressions()) {
            process(expression, context);
        }

        return null;
    }

    @Override
    protected Void visitGroupBy(GroupBy node, C context)
    {
        for (GroupingElement groupingElement : node.getGroupingElements()) {
            process(groupingElement, context);
        }

        return null;
    }

    @Override
    protected Void visitCube(Cube node, C context)
    {
        return null;
    }

    @Override
    protected Void visitRollup(Rollup node, C context)
    {
        return null;
    }

    @Override
    protected Void visitSimpleGroupBy(SimpleGroupBy node, C context)
    {
        for (Expression expression : node.getExpressions()) {
            process(expression, context);
        }

        return null;
    }

    @Override
    protected Void visitGroupingSets(GroupingSets node, C context)
    {
        return null;
    }

    @Override
    protected Void visitInsert(Insert node, C context)
    {
        process(node.getQuery(), context);

        return null;
    }

    @Override
    protected Void visitRefreshMaterializedView(RefreshMaterializedView node, C context)
    {
        return null;
    }

    @Override
    protected Void visitDelete(Delete node, C context)
    {
        process(node.getTable(), context);
        node.getWhere().ifPresent(where -> process(where, context));

        return null;
    }

    @Override
    protected Void visitCreateTableAsSelect(CreateTableAsSelect node, C context)
    {
        process(node.getQuery(), context);
        for (Property property : node.getProperties()) {
            process(property, context);
        }

        return null;
    }

    @Override
    protected Void visitProperty(Property node, C context)
    {
        process(node.getName(), context);
        process(node.getValue(), context);

        return null;
    }

    @Override
    protected Void visitAnalyze(Analyze node, C context)
    {
        for (Property property : node.getProperties()) {
            process(property, context);
        }
        return null;
    }

    @Override
    protected Void visitCreateView(CreateView node, C context)
    {
        process(node.getQuery(), context);

        return null;
    }

    @Override
    protected Void visitSetSession(SetSession node, C context)
    {
        process(node.getValue(), context);

        return null;
    }

    @Override
    protected Void visitAddColumn(AddColumn node, C context)
    {
        process(node.getColumn(), context);

        return null;
    }

    @Override
    protected Void visitCreateTable(CreateTable node, C context)
    {
        for (TableElement tableElement : node.getElements()) {
            process(tableElement, context);
        }
        for (Property property : node.getProperties()) {
            process(property, context);
        }

        return null;
    }

    @Override
    protected Void visitStartTransaction(StartTransaction node, C context)
    {
        for (TransactionMode transactionMode : node.getTransactionModes()) {
            process(transactionMode, context);
        }

        return null;
    }

    @Override
    protected Void visitExplain(Explain node, C context)
    {
        process(node.getStatement(), context);

        for (ExplainOption option : node.getOptions()) {
            process(option, context);
        }

        return null;
    }

    @Override
    protected Void visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context)
    {
        process(node.getValue(), context);
        process(node.getSubquery(), context);

        return null;
    }

    @Override
    protected Void visitExists(ExistsPredicate node, C context)
    {
        process(node.getSubquery(), context);

        return null;
    }

    @Override
    protected Void visitLateral(Lateral node, C context)
    {
        process(node.getQuery(), context);

        return null;
    }
}
