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
package io.trino.sql.routine.ir;

import io.trino.sql.relational.RowExpression;

public class DefaultIrNodeVisitor
        implements IrNodeVisitor<Void, Void>
{
    @Override
    public Void visitRoutine(IrRoutine node, Void context)
    {
        for (IrVariable parameter : node.parameters()) {
            process(parameter, context);
        }
        process(node.body(), context);
        return null;
    }

    @Override
    public Void visitVariable(IrVariable node, Void context)
    {
        visitRowExpression(node.defaultValue());
        return null;
    }

    @Override
    public Void visitBlock(IrBlock node, Void context)
    {
        for (IrVariable variable : node.variables()) {
            process(variable, context);
        }
        for (IrStatement statement : node.statements()) {
            process(statement, context);
        }
        return null;
    }

    @Override
    public Void visitBreak(IrBreak node, Void context)
    {
        return null;
    }

    @Override
    public Void visitContinue(IrContinue node, Void context)
    {
        return null;
    }

    @Override
    public Void visitIf(IrIf node, Void context)
    {
        visitRowExpression(node.condition());
        process(node.ifTrue(), context);
        if (node.ifFalse().isPresent()) {
            process(node.ifFalse().get(), context);
        }
        return null;
    }

    @Override
    public Void visitWhile(IrWhile node, Void context)
    {
        visitRowExpression(node.condition());
        process(node.body(), context);
        return null;
    }

    @Override
    public Void visitRepeat(IrRepeat node, Void context)
    {
        visitRowExpression(node.condition());
        process(node.block(), context);
        return null;
    }

    @Override
    public Void visitLoop(IrLoop node, Void context)
    {
        process(node.block(), context);
        return null;
    }

    @Override
    public Void visitReturn(IrReturn node, Void context)
    {
        visitRowExpression(node.value());
        return null;
    }

    @Override
    public Void visitSet(IrSet node, Void context)
    {
        visitRowExpression(node.value());
        process(node.target(), context);
        return null;
    }

    public void visitRowExpression(RowExpression expression) {}
}
