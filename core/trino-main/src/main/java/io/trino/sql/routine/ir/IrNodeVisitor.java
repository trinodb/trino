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

public interface IrNodeVisitor<C, R>
{
    default R process(IrNode node, C context)
    {
        return node.accept(this, context);
    }

    default R visitNode(IrNode node, C context)
    {
        return null;
    }

    default R visitRoutine(IrRoutine node, C context)
    {
        return visitNode(node, context);
    }

    default R visitVariable(IrVariable node, C context)
    {
        return visitNode(node, context);
    }

    default R visitBlock(IrBlock node, C context)
    {
        return visitNode(node, context);
    }

    default R visitBreak(IrBreak node, C context)
    {
        return visitNode(node, context);
    }

    default R visitContinue(IrContinue node, C context)
    {
        return visitNode(node, context);
    }

    default R visitIf(IrIf node, C context)
    {
        return visitNode(node, context);
    }

    default R visitRepeat(IrRepeat node, C context)
    {
        return visitNode(node, context);
    }

    default R visitLoop(IrLoop node, C context)
    {
        return visitNode(node, context);
    }

    default R visitReturn(IrReturn node, C context)
    {
        return visitNode(node, context);
    }

    default R visitSet(IrSet node, C context)
    {
        return visitNode(node, context);
    }

    default R visitWhile(IrWhile node, C context)
    {
        return visitNode(node, context);
    }
}
