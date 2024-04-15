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

    protected R visitArray(Array node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitBetween(Between node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCoalesce(Coalesce node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitComparison(Comparison node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitConstant(Constant node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIn(In node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCall(Call node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLambda(Lambda node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitSwitch(Switch node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNullIf(NullIf node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitNot(Not node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitCase(Case node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitIsNull(IsNull node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitFieldReference(FieldReference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitLogical(Logical node, C context)
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

    protected R visitReference(Reference node, C context)
    {
        return visitExpression(node, context);
    }

    protected R visitBind(Bind node, C context)
    {
        return visitExpression(node, context);
    }
}
