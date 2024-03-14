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

import jakarta.annotation.Nullable;

public abstract class IrVisitor<R, C>
{
    public R process(Expression node)
    {
        return process(node, null);
    }

    public R process(Expression node, @Nullable C context)
    {
        return node.accept(this, context);
    }

    protected R visitExpression(Expression node, C context)
    {
        return null;
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

    protected R visitGenericLiteral(GenericLiteral node, C context)
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

    protected R visitBinaryLiteral(BinaryLiteral node, C context)
    {
        return visitLiteral(node, context);
    }

    protected R visitBooleanLiteral(BooleanLiteral node, C context)
    {
        return visitLiteral(node, context);
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

    protected R visitSearchedCaseExpression(SearchedCaseExpression node, C context)
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

    protected R visitArray(Array node, C context)
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

    protected R visitLogicalExpression(LogicalExpression node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitRow(Row node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCast(Cast node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSymbolReference(SymbolReference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitBindExpression(BindExpression node, C context)
    {
        return visitExpression(node, context);
    }
}
