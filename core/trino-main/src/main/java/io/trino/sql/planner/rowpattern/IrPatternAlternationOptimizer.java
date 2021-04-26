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
package io.trino.sql.planner.rowpattern;

import com.google.common.collect.ImmutableList;
import io.trino.sql.planner.rowpattern.ir.IrAlternation;
import io.trino.sql.planner.rowpattern.ir.IrAnchor;
import io.trino.sql.planner.rowpattern.ir.IrConcatenation;
import io.trino.sql.planner.rowpattern.ir.IrEmpty;
import io.trino.sql.planner.rowpattern.ir.IrExclusion;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrPermutation;
import io.trino.sql.planner.rowpattern.ir.IrQuantified;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.planner.rowpattern.ir.IrRowPatternVisitor;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.rowpattern.ir.IrQuantifier.zeroOrOne;

/**
 * Remove empty pattern from pattern alternation and replace it with quantification of a neighbouring term.
 */
public final class IrPatternAlternationOptimizer
{
    private IrPatternAlternationOptimizer() {}

    public static IrRowPattern optimize(IrRowPattern node)
    {
        return new Visitor().process(node);
    }

    private static class Visitor
            extends IrRowPatternVisitor<IrRowPattern, Void>
    {
        @Override
        protected IrRowPattern visitIrRowPattern(IrRowPattern node, Void context)
        {
            throw new UnsupportedOperationException("unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected IrRowPattern visitIrLabel(IrLabel node, Void context)
        {
            return node;
        }

        @Override
        protected IrRowPattern visitIrAnchor(IrAnchor node, Void context)
        {
            return node;
        }

        @Override
        protected IrRowPattern visitIrEmpty(IrEmpty node, Void context)
        {
            return node;
        }

        @Override
        protected IrRowPattern visitIrExclusion(IrExclusion node, Void context)
        {
            IrRowPattern child = process(node.getPattern());

            return new IrExclusion(child);
        }

        @Override
        protected IrRowPattern visitIrAlternation(IrAlternation node, Void context)
        {
            List<IrRowPattern> children = node.getPatterns().stream()
                    .map(this::process)
                    .collect(toImmutableList());

            int emptyChildIndex = -1;
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i) instanceof IrEmpty) {
                    checkState(emptyChildIndex < 0, "run IrRowPatternFlattener first to remove redundant empty pattern");
                    emptyChildIndex = i;
                }
            }

            if (emptyChildIndex < 0) {
                return new IrAlternation(children);
            }

            // remove the empty child:
            // (() | A) -> A??
            // (() | A | B) -> (A?? | B)
            if (emptyChildIndex == 0) {
                IrRowPattern child = new IrQuantified(children.get(1), zeroOrOne(false));
                if (children.size() == 2) {
                    return child;
                }
                ImmutableList.Builder<IrRowPattern> builder = ImmutableList.<IrRowPattern>builder()
                        .add(child)
                        .addAll(children.subList(2, children.size()));
                return new IrAlternation(builder.build());
            }
            // (A | ()) -> A?
            // (A | B | () | C) -> (A | B? | C)
            children = ImmutableList.<IrRowPattern>builder()
                    .addAll(children.subList(0, emptyChildIndex - 1))
                    .add(new IrQuantified(children.get(emptyChildIndex - 1), zeroOrOne(true)))
                    .addAll(children.subList(emptyChildIndex + 1, children.size()))
                    .build();

            if (children.size() == 1) {
                return children.get(0);
            }
            return new IrAlternation(children);
        }

        @Override
        protected IrRowPattern visitIrConcatenation(IrConcatenation node, Void context)
        {
            List<IrRowPattern> children = node.getPatterns().stream()
                    .map(this::process)
                    .collect(toImmutableList());

            return new IrConcatenation(children);
        }

        @Override
        protected IrRowPattern visitIrPermutation(IrPermutation node, Void context)
        {
            List<IrRowPattern> children = node.getPatterns().stream()
                    .map(this::process)
                    .collect(toImmutableList());

            return new IrPermutation(children);
        }

        @Override
        protected IrRowPattern visitIrQuantified(IrQuantified node, Void context)
        {
            IrRowPattern child = process(node.getPattern());

            return new IrQuantified(child, node.getQuantifier());
        }
    }
}
