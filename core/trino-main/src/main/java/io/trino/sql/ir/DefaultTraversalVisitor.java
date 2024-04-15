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

public abstract class DefaultTraversalVisitor<C>
        extends IrVisitor<Void, C>
{
    @Override
    protected Void visitArray(Array node, C context)
    {
        for (Expression element : node.elements()) {
            process(element, context);
        }

        return null;
    }

    @Override
    protected Void visitCast(Cast node, C context)
    {
        process(node.expression(), context);
        return null;
    }

    @Override
    protected Void visitBetween(Between node, C context)
    {
        process(node.value(), context);
        process(node.min(), context);
        process(node.max(), context);

        return null;
    }

    @Override
    protected Void visitCoalesce(Coalesce node, C context)
    {
        for (Expression operand : node.operands()) {
            process(operand, context);
        }

        return null;
    }

    @Override
    protected Void visitFieldReference(FieldReference node, C context)
    {
        process(node.base(), context);

        return null;
    }

    @Override
    protected Void visitComparison(Comparison node, C context)
    {
        process(node.left(), context);
        process(node.right(), context);

        return null;
    }

    @Override
    protected Void visitIn(In node, C context)
    {
        process(node.value(), context);
        for (Expression argument : node.valueList()) {
            process(argument, context);
        }

        return null;
    }

    @Override
    protected Void visitCall(Call node, C context)
    {
        for (Expression argument : node.arguments()) {
            process(argument, context);
        }

        return null;
    }

    @Override
    protected Void visitSwitch(Switch node, C context)
    {
        process(node.operand(), context);
        for (WhenClause clause : node.whenClauses()) {
            process(clause.getOperand(), context);
            process(clause.getResult(), context);
        }

        process(node.defaultValue(), context);

        return null;
    }

    @Override
    protected Void visitNullIf(NullIf node, C context)
    {
        process(node.first(), context);
        process(node.second(), context);

        return null;
    }

    @Override
    protected Void visitBind(Bind node, C context)
    {
        for (Expression value : node.values()) {
            process(value, context);
        }
        process(node.function(), context);

        return null;
    }

    @Override
    protected Void visitNot(Not node, C context)
    {
        process(node.value(), context);
        return null;
    }

    @Override
    protected Void visitCase(Case node, C context)
    {
        for (WhenClause clause : node.whenClauses()) {
            process(clause.getOperand(), context);
            process(clause.getResult(), context);
        }

        process(node.defaultValue(), context);

        return null;
    }

    @Override
    protected Void visitIsNull(IsNull node, C context)
    {
        process(node.value(), context);
        return null;
    }

    @Override
    protected Void visitLogical(Logical node, C context)
    {
        for (Expression child : node.terms()) {
            process(child, context);
        }

        return null;
    }

    @Override
    protected Void visitRow(Row node, C context)
    {
        for (Expression expression : node.items()) {
            process(expression, context);
        }
        return null;
    }

    @Override
    protected Void visitLambda(Lambda node, C context)
    {
        process(node.body(), context);

        return null;
    }
}
