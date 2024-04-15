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

public class ExpressionRewriter<C>
{
    protected Expression rewriteExpression(Expression node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return null;
    }

    public Expression rewriteArray(Array node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteRow(Row node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteComparison(Comparison node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBetween(Between node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLogical(Logical node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNot(Not node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIsNull(IsNull node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteNullIf(NullIf node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCase(Case node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSwitch(Switch node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCoalesce(Coalesce node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCall(Call node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteLambda(Lambda node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteBind(Bind node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteIn(In node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteConstant(Constant node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteSubscript(FieldReference node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteCast(Cast node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }

    public Expression rewriteReference(Reference node, C context, ExpressionTreeRewriter<C> treeRewriter)
    {
        return rewriteExpression(node, context, treeRewriter);
    }
}
