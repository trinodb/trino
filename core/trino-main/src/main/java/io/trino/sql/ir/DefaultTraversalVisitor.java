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
    protected Void visitCast(Cast node, C context)
    {
        process(node.expression(), context);
        return null;
    }

    @Override
    protected Void visitArithmeticBinary(ArithmeticBinaryExpression node, C context)
    {
        process(node.left(), context);
        process(node.right(), context);

        return null;
    }

    @Override
    protected Void visitBetweenPredicate(BetweenPredicate node, C context)
    {
        process(node.value(), context);
        process(node.min(), context);
        process(node.max(), context);

        return null;
    }

    @Override
    protected Void visitCoalesceExpression(CoalesceExpression node, C context)
    {
        for (Expression operand : node.operands()) {
            process(operand, context);
        }

        return null;
    }

    @Override
    protected Void visitSubscriptExpression(SubscriptExpression node, C context)
    {
        process(node.base(), context);
        process(node.index(), context);

        return null;
    }

    @Override
    protected Void visitComparisonExpression(ComparisonExpression node, C context)
    {
        process(node.left(), context);
        process(node.right(), context);

        return null;
    }

    @Override
    protected Void visitInPredicate(InPredicate node, C context)
    {
        process(node.value(), context);
        for (Expression argument : node.valueList()) {
            process(argument, context);
        }

        return null;
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, C context)
    {
        for (Expression argument : node.arguments()) {
            process(argument, context);
        }

        return null;
    }

    @Override
    protected Void visitSimpleCaseExpression(SimpleCaseExpression node, C context)
    {
        process(node.operand(), context);
        for (WhenClause clause : node.whenClauses()) {
            process(clause.getOperand(), context);
            process(clause.getResult(), context);
        }

        node.defaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected Void visitNullIfExpression(NullIfExpression node, C context)
    {
        process(node.first(), context);
        process(node.second(), context);

        return null;
    }

    @Override
    protected Void visitBindExpression(BindExpression node, C context)
    {
        for (Expression value : node.values()) {
            process(value, context);
        }
        process(node.function(), context);

        return null;
    }

    @Override
    protected Void visitArithmeticNegation(ArithmeticNegation node, C context)
    {
        process(node.value(), context);
        return null;
    }

    @Override
    protected Void visitNotExpression(NotExpression node, C context)
    {
        process(node.value(), context);
        return null;
    }

    @Override
    protected Void visitSearchedCaseExpression(SearchedCaseExpression node, C context)
    {
        for (WhenClause clause : node.whenClauses()) {
            process(clause.getOperand(), context);
            process(clause.getResult(), context);
        }
        node.defaultValue()
                .ifPresent(value -> process(value, context));

        return null;
    }

    @Override
    protected Void visitIsNullPredicate(IsNullPredicate node, C context)
    {
        process(node.value(), context);
        return null;
    }

    @Override
    protected Void visitLogicalExpression(LogicalExpression node, C context)
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
    protected Void visitLambdaExpression(LambdaExpression node, C context)
    {
        process(node.body(), context);

        return null;
    }
}
