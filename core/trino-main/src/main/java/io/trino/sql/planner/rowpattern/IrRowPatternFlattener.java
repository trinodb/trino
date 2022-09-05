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
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Optimize row pattern:
 * - remove nested exclusions
 * - flatten alternations and concatenations
 * - remove redundant empty pattern
 */
public final class IrRowPatternFlattener
{
    private IrRowPatternFlattener() {}

    public static IrRowPattern optimize(IrRowPattern node)
    {
        return new Visitor().process(node, false);
    }

    private static class Visitor
            extends IrRowPatternVisitor<IrRowPattern, Boolean>
    {
        @Override
        protected IrRowPattern visitIrRowPattern(IrRowPattern node, Boolean inExclusion)
        {
            throw new UnsupportedOperationException("unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected IrRowPattern visitIrLabel(IrLabel node, Boolean inExclusion)
        {
            return node;
        }

        @Override
        protected IrRowPattern visitIrAnchor(IrAnchor node, Boolean inExclusion)
        {
            return node;
        }

        @Override
        protected IrRowPattern visitIrEmpty(IrEmpty node, Boolean inExclusion)
        {
            return node;
        }

        @Override
        protected IrRowPattern visitIrExclusion(IrExclusion node, Boolean inExclusion)
        {
            IrRowPattern child = process(node.getPattern(), true);
            if (inExclusion) {
                // skip nested exclusion. this is necessary to resolve exclusions correctly during pattern matching
                return child;
            }

            return new IrExclusion(child);
        }

        /**
         * Flatten the alternation and remove redundant empty branches.
         * This method recursively inlines sub-pattern lists from child alternations into the top-level alternation.
         * The branch preference order is reflected in the resulting order of the elements.
         * Examples:
         * A | (B | C) -> A | B | C
         * (A | B) | ((C | D) | E) -> A | B | C | D | E
         * <p>
         * Also, redundant empty branches are removed from the resulting sub-pattern list: all empty branches
         * following the first empty branch are not achievable, as they are less preferred than the first
         * empty branch, so they are considered redundant.
         * Examples:
         * A | (() | B) -> A | () | B
         * (A | ()) | ((() | B) | ()) -> A | () | B
         * (() | ()) | () -> ()
         * <p>
         * Note: The logic of removing redundant empty branches could be extended to remove other duplicate sub-patterns.
         *
         * @return the flattened IrAlternation containing at most one empty branch, or IrEmpty in case when
         * the alternation is reduced to a single empty branch
         */
        @Override
        protected IrRowPattern visitIrAlternation(IrAlternation node, Boolean inExclusion)
        {
            List<IrRowPattern> children = node.getPatterns().stream()
                    .map(pattern -> process(pattern, inExclusion))
                    .collect(toImmutableList());

            // flatten alternation
            children = children.stream()
                    .flatMap(child -> {
                        if (child instanceof IrAlternation) {
                            return ((IrAlternation) child).getPatterns().stream();
                        }
                        return Stream.of(child);
                    })
                    .collect(toImmutableList());

            Optional<IrRowPattern> firstEmptyChild = children.stream()
                    .filter(IrEmpty.class::isInstance)
                    .findFirst();
            if (firstEmptyChild.isEmpty()) {
                return new IrAlternation(children);
            }

            // remove all empty children following the first empty child
            children = children.stream()
                    .filter(child -> !(child instanceof IrEmpty) || child == firstEmptyChild.get())
                    .collect(toImmutableList());

            // if there is only the empty child left, replace alternation with empty pattern
            if (children.size() == 1) {
                return new IrEmpty();
            }

            return new IrAlternation(children);
        }

        /**
         * Flatten the concatenation and remove all empty branches.
         * This method recursively inlines sub-pattern lists from child concatenations into the top-level concatenation.
         * The expected sub-pattern order is reflected in the resulting order of the elements.
         * Also, all empty branches are removed from the resulting sub-pattern list.
         * Examples:
         * A (B C) -> A B C
         * (A B) ((C D) E) -> A B C D E
         * A (() B) -> A B
         * (A ()) ((() B) ()) -> A B
         * () (() A) -> A
         * (() ()) () -> ()
         *
         * @return the flattened IrConcatenation containing no empty branches, or a sub-pattern in case when
         * the concatenation is reduced to a single branch, or IrEmpty in case when all sub-patterns
         * are empty
         */
        @Override
        protected IrRowPattern visitIrConcatenation(IrConcatenation node, Boolean inExclusion)
        {
            List<IrRowPattern> children = node.getPatterns().stream()
                    .map(pattern -> process(pattern, inExclusion))
                    .collect(toImmutableList());

            // flatten concatenation and remove all empty children
            children = children.stream()
                    .flatMap(child -> {
                        if (child instanceof IrConcatenation) {
                            return ((IrConcatenation) child).getPatterns().stream();
                        }
                        return Stream.of(child);
                    })
                    .filter(child -> !(child instanceof IrEmpty))
                    .collect(toImmutableList());

            if (children.isEmpty()) {
                return new IrEmpty();
            }
            if (children.size() == 1) {
                return children.get(0);
            }
            return new IrConcatenation(children);
        }

        /**
         * Remove all empty branches from the permutation.
         * Examples:
         * PERMUTE(A, (), B, ()) -> PERMUTE(A, B)
         * PERMUTE((), A) -> A
         * PERMUTE((), ()) -> ()
         *
         * @return the IrPermutation containing no empty branches, or a sub-pattern in case when
         * the permutation is reduced to a single branch, or IrEmpty in case when all sub-patterns
         * are empty
         */
        @Override
        protected IrRowPattern visitIrPermutation(IrPermutation node, Boolean inExclusion)
        {
            // process children and remove all empty children
            List<IrRowPattern> children = node.getPatterns().stream()
                    .map(pattern -> process(pattern, inExclusion))
                    .filter(child -> !(child instanceof IrEmpty))
                    .collect(toImmutableList());

            if (children.isEmpty()) {
                return new IrEmpty();
            }
            if (children.size() == 1) {
                return children.get(0);
            }
            return new IrPermutation(children);
        }

        @Override
        protected IrRowPattern visitIrQuantified(IrQuantified node, Boolean inExclusion)
        {
            IrRowPattern child = process(node.getPattern(), inExclusion);

            if (child instanceof IrEmpty) {
                return child;
            }
            return new IrQuantified(child, node.getQuantifier());
        }
    }
}
