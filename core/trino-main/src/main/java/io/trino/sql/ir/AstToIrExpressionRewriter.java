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

public class AstToIrExpressionRewriter<C>
{
    protected Expression rewriteExpression(io.trino.sql.tree.Expression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return null;
    }

    public Expression rewriteRow(io.trino.sql.tree.Row node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteArithmeticUnary(io.trino.sql.tree.ArithmeticUnaryExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteArithmeticBinary(io.trino.sql.tree.ArithmeticBinaryExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteComparisonExpression(io.trino.sql.tree.ComparisonExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBetweenPredicate(io.trino.sql.tree.BetweenPredicate node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLogicalExpression(io.trino.sql.tree.LogicalExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNotExpression(io.trino.sql.tree.NotExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIsNullPredicate(io.trino.sql.tree.IsNullPredicate node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIsNotNullPredicate(io.trino.sql.tree.IsNotNullPredicate node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNullIfExpression(io.trino.sql.tree.NullIfExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIfExpression(io.trino.sql.tree.IfExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSearchedCaseExpression(io.trino.sql.tree.SearchedCaseExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSimpleCaseExpression(io.trino.sql.tree.SimpleCaseExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteWhenClause(io.trino.sql.tree.WhenClause node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCoalesceExpression(io.trino.sql.tree.CoalesceExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteInListExpression(io.trino.sql.tree.InListExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteFunctionCall(io.trino.sql.tree.FunctionCall node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteWindowOperation(io.trino.sql.tree.WindowOperation node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLambdaExpression(io.trino.sql.tree.LambdaExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBindExpression(io.trino.sql.tree.BindExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLikePredicate(io.trino.sql.tree.LikePredicate node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteInPredicate(io.trino.sql.tree.InPredicate node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteExists(io.trino.sql.tree.ExistsPredicate node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSubqueryExpression(io.trino.sql.tree.SubqueryExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLiteral(io.trino.sql.tree.Literal node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBinaryLiteral(io.trino.sql.tree.BinaryLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBooleanLiteral(io.trino.sql.tree.BooleanLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCharLiteral(io.trino.sql.tree.CharLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteDecimalLiteral(io.trino.sql.tree.DecimalLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteDoubleLiteral(io.trino.sql.tree.DoubleLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteGenericLiteral(io.trino.sql.tree.GenericLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIntervalLiteral(io.trino.sql.tree.IntervalLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLongLiteral(io.trino.sql.tree.LongLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNullLiteral(io.trino.sql.tree.NullLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteStringLiteral(io.trino.sql.tree.StringLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteTimeLiteral(io.trino.sql.tree.TimeLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteTimestampLiteral(io.trino.sql.tree.TimestampLiteral node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteArrayConstructor(io.trino.sql.tree.ArrayConstructor node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSubscriptExpression(io.trino.sql.tree.SubscriptExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIdentifier(io.trino.sql.tree.Identifier node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteDereferenceExpression(io.trino.sql.tree.DereferenceExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteExtract(io.trino.sql.tree.Extract node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCurrentTime(io.trino.sql.tree.CurrentTime node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCast(io.trino.sql.tree.Cast node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteTryExpression(io.trino.sql.tree.TryExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteAtTimeZone(io.trino.sql.tree.AtTimeZone node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCurrentCatalog(io.trino.sql.tree.CurrentCatalog node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCurrentSchema(io.trino.sql.tree.CurrentSchema node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCurrentUser(io.trino.sql.tree.CurrentUser node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCurrentPath(io.trino.sql.tree.CurrentPath node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteTrim(io.trino.sql.tree.Trim node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteFormat(io.trino.sql.tree.Format node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteFieldReference(io.trino.sql.tree.FieldReference node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSymbolReference(io.trino.sql.tree.SymbolReference node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteParameter(io.trino.sql.tree.Parameter node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteQuantifiedComparison(io.trino.sql.tree.QuantifiedComparisonExpression node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteGroupingOperation(io.trino.sql.tree.GroupingOperation node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteGenericDataType(io.trino.sql.tree.GenericDataType node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteRowDataType(io.trino.sql.tree.RowDataType node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteDateTimeDataType(io.trino.sql.tree.DateTimeDataType node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIntervalDayTimeDataType(io.trino.sql.tree.IntervalDayTimeDataType node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLabelDereference(io.trino.sql.tree.LabelDereference node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteJsonExists(io.trino.sql.tree.JsonExists node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteJsonValue(io.trino.sql.tree.JsonValue node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteJsonQuery(io.trino.sql.tree.JsonQuery node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteJsonObject(io.trino.sql.tree.JsonObject node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteJsonArray(io.trino.sql.tree.JsonArray node, C context, AstToIrExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }
}
