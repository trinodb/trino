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
    protected Void visitArray(Array node, C context)
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
        for (Expression argument : node.getValueList()) {
            process(argument, context);
        }

        return null;
    }

    @Override
    protected Void visitFunctionCall(FunctionCall node, C context)
    {
        for (Expression argument : node.getArguments()) {
            process(argument, context);
        }

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
    protected Void visitLogicalExpression(LogicalExpression node, C context)
    {
        for (Expression child : node.getTerms()) {
            process(child, context);
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
    protected Void visitLambdaExpression(LambdaExpression node, C context)
    {
        process(node.getBody(), context);

        return null;
    }
}
