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
package io.trino.sql.ir;

import javax.annotation.Nullable;

public abstract class IrVisitor<R, C>
{
    public R process(Node node)
    {
        return process(node, null);
    }

    public R process(Node node, @Nullable C context)
    {
        return node.accept(this, context);
    }

    protected R visitNode(Node node, C context)
    {
        return null;
    }

    protected R visitExpression(Expression node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitCurrentTime(CurrentTime node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitExtract(Extract node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitBetweenPredicate(BetweenPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCoalesceExpression(CoalesceExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitComparisonExpression(ComparisonExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLiteral(Literal node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitDoubleLiteral(DoubleLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitDecimalLiteral(DecimalLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitStatement(Statement node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitQuery(Query node, C context)
    {
        return visitStatement(node, context);
    }

    protected R visitGenericLiteral(GenericLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitTimeLiteral(TimeLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitWith(With node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWithQuery(WithQuery node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitSelect(Select node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitRelation(Relation node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitQueryBody(QueryBody node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitOrderBy(OrderBy node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitOffset(Offset node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitFetchFirst(FetchFirst node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitLimit(Limit node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitAllRows(AllRows node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSetOperation(SetOperation node, C context)
    {
        return visitQueryBody(node, context);
    }

    protected R visitIntersect(Intersect node, C context)
    {
        return visitSetOperation(node, context);
    }

    protected R visitTimestampLiteral(TimestampLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitWhenClause(WhenClause node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIntervalLiteral(IntervalLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitInPredicate(InPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitFunctionCall(FunctionCall node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitProcessingMode(ProcessingMode node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWindowOperation(WindowOperation node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLambdaExpression(LambdaExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitStringLiteral(StringLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitCharLiteral(CharLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitBinaryLiteral(BinaryLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitBooleanLiteral(BooleanLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitInListExpression(InListExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIdentifier(Identifier node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitDereferenceExpression(DereferenceExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitTrim(Trim node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNullIfExpression(NullIfExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIfExpression(IfExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNullLiteral(NullLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNotExpression(NotExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSelectItem(SelectItem node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitSingleColumn(SingleColumn node, C context)
    {
        return visitSelectItem(node, context);
    }

    protected R visitAllColumns(AllColumns node, C context)
    {
        return visitSelectItem(node, context);
    }

    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLikePredicate(LikePredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIsNullPredicate(IsNullPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitArrayConstructor(ArrayConstructor node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSubscriptExpression(SubscriptExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLongLiteral(LongLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitParameter(Parameter node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLogicalExpression(LogicalExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSubqueryExpression(SubqueryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSortItem(SortItem node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitLateral(Lateral node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitRow(Row node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitAliasedRelation(AliasedRelation node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitJoin(Join node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitExists(ExistsPredicate node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitTryExpression(TryExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCast(Cast node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitFieldReference(FieldReference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitWindowReference(WindowReference node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWindowSpecification(WindowSpecification node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWindowDefinition(WindowDefinition node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitWindowFrame(WindowFrame node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitFrameBound(FrameBound node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitAtTimeZone(AtTimeZone node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitGroupBy(GroupBy node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitGroupingElement(GroupingElement node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitCube(Cube node, C context)
    {
        return visitGroupingElement(node, context);
    }

    protected R visitGroupingSets(GroupingSets node, C context)
    {
        return visitGroupingElement(node, context);
    }

    protected R visitRollup(Rollup node, C context)
    {
        return visitGroupingElement(node, context);
    }

    protected R visitSimpleGroupBy(SimpleGroupBy node, C context)
    {
        return visitGroupingElement(node, context);
    }

    protected R visitSymbolReference(SymbolReference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLambdaArgumentDeclaration(LambdaArgumentDeclaration node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitBindExpression(BindExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitGroupingOperation(GroupingOperation node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCurrentCatalog(CurrentCatalog node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCurrentSchema(CurrentSchema node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCurrentUser(CurrentUser node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCurrentPath(CurrentPath node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitFormat(Format node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitDataType(DataType node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitRowDataType(RowDataType node, C context)
    {
        return visitDataType(node, context);
    }

    protected R visitGenericDataType(GenericDataType node, C context)
    {
        return visitDataType(node, context);
    }

    protected R visitRowField(RowDataType.Field node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitDataTypeParameter(DataTypeParameter node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitNumericTypeParameter(NumericParameter node, C context)
    {
        return visitDataTypeParameter(node, context);
    }

    protected R visitTypeParameter(TypeParameter node, C context)
    {
        return visitDataTypeParameter(node, context);
    }

    protected R visitIntervalDataType(IntervalDayTimeDataType node, C context)
    {
        return visitDataType(node, context);
    }

    protected R visitDateTimeType(DateTimeDataType node, C context)
    {
        return visitDataType(node, context);
    }

    protected R visitMeasureDefinition(MeasureDefinition node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitSkipTo(SkipTo node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitPatternSearchMode(PatternSearchMode node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitSubsetDefinition(SubsetDefinition node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitVariableDefinition(VariableDefinition node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitPatternRecognitionRelation(PatternRecognitionRelation node, C context)
    {
        return visitRelation(node, context);
    }

    protected R visitLabelDereference(LabelDereference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitRowPattern(RowPattern node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitPatternAlternation(PatternAlternation node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitPatternConcatenation(PatternConcatenation node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitQuantifiedPattern(QuantifiedPattern node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitAnchorPattern(AnchorPattern node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitEmptyPattern(EmptyPattern node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitExcludedPattern(ExcludedPattern node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitPatternPermutation(PatternPermutation node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitPatternVariable(PatternVariable node, C context)
    {
        return visitRowPattern(node, context);
    }

    protected R visitPatternQuantifier(PatternQuantifier node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitZeroOrMoreQuantifier(ZeroOrMoreQuantifier node, C context)
    {
        return visitPatternQuantifier(node, context);
    }

    protected R visitOneOrMoreQuantifier(OneOrMoreQuantifier node, C context)
    {
        return visitPatternQuantifier(node, context);
    }

    protected R visitZeroOrOneQuantifier(ZeroOrOneQuantifier node, C context)
    {
        return visitPatternQuantifier(node, context);
    }

    protected R visitRangeQuantifier(RangeQuantifier node, C context)
    {
        return visitPatternQuantifier(node, context);
    }

    protected R visitJsonExists(JsonExists node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitJsonValue(JsonValue node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitJsonQuery(JsonQuery node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitJsonPathInvocation(JsonPathInvocation node, C context)
    {
        return visitNode(node, context);
    }

    protected R visitJsonObject(JsonObject node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitJsonArray(JsonArray node, C context)
    {
        return visitExpression(node, context);
    }
}
