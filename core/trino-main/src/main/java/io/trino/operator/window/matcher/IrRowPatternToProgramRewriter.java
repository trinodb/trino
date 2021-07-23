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
package io.trino.operator.window.matcher;

import io.trino.sql.planner.rowpattern.ir.IrAlternation;
import io.trino.sql.planner.rowpattern.ir.IrAnchor;
import io.trino.sql.planner.rowpattern.ir.IrConcatenation;
import io.trino.sql.planner.rowpattern.ir.IrEmpty;
import io.trino.sql.planner.rowpattern.ir.IrExclusion;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrPermutation;
import io.trino.sql.planner.rowpattern.ir.IrQuantified;
import io.trino.sql.planner.rowpattern.ir.IrQuantifier;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.planner.rowpattern.ir.IrRowPatternVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Collections2.orderedPermutations;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IrRowPatternToProgramRewriter
{
    private IrRowPatternToProgramRewriter() {}

    public static Program rewrite(IrRowPattern node, Map<IrLabel, Integer> labelMapping)
    {
        List<Instruction> instructions = new ArrayList<>();
        new Rewriter(instructions, labelMapping).process(node);
        instructions.add(new Done());
        return new Program(instructions);
    }

    private static class Rewriter
            extends IrRowPatternVisitor<Void, Void>
    {
        private final List<Instruction> instructions;
        private final Map<IrLabel, Integer> labelMapping;

        public Rewriter(List<Instruction> instructions, Map<IrLabel, Integer> labelMapping)
        {
            this.instructions = requireNonNull(instructions, "instructions is null");
            this.labelMapping = requireNonNull(labelMapping, "labelMapping is null");
        }

        @Override
        protected Void visitIrRowPattern(IrRowPattern node, Void context)
        {
            throw new UnsupportedOperationException("unsupported node type: " + node.getClass().getName());
        }

        @Override
        protected Void visitIrLabel(IrLabel node, Void context)
        {
            instructions.add(new MatchLabel(labelMapping.get(node)));
            return null;
        }

        @Override
        protected Void visitIrEmpty(IrEmpty node, Void context)
        {
            return null;
        }

        @Override
        protected Void visitIrAnchor(IrAnchor node, Void context)
        {
            switch (node.getType()) {
                case PARTITION_START:
                    instructions.add(new MatchStart());
                    return null;
                case PARTITION_END:
                    instructions.add(new MatchEnd());
                    return null;
                default:
                    throw new IllegalStateException("unexpected anchor type: " + node.getType());
            }
        }

        @Override
        protected Void visitIrExclusion(IrExclusion node, Void context)
        {
            instructions.add(new Save());
            process(node.getPattern());
            instructions.add(new Save());
            return null;
        }

        @Override
        protected Void visitIrAlternation(IrAlternation node, Void context)
        {
            List<IrRowPattern> parts = node.getPatterns();
            List<Integer> jumpPositions = new ArrayList<>();

            for (int i = 0; i < parts.size() - 1; i++) {
                int splitPosition = instructions.size();
                instructions.add(null); // placeholder for the Split instruction
                int splitTarget = instructions.size();
                process(parts.get(i));
                jumpPositions.add(instructions.size());
                instructions.add(null); // placeholder for the Jump instruction
                instructions.set(splitPosition, new Split(splitTarget, instructions.size()));
            }

            process(parts.get(parts.size() - 1));

            for (int position : jumpPositions) {
                instructions.set(position, new Jump(instructions.size()));
            }

            return null;
        }

        @Override
        protected Void visitIrConcatenation(IrConcatenation node, Void context)
        {
            concatenation(node.getPatterns());
            return null;
        }

        @Override
        protected Void visitIrPermutation(IrPermutation node, Void context)
        {
            checkArgument(node.getPatterns().size() > 1, "invalid pattern: permutation with single element. run IrRowPatternFlattener first");

            List<Integer> indexes = IntStream.range(0, node.getPatterns().size())
                    .boxed()
                    .collect(toImmutableList());

            List<List<IrRowPattern>> permutations = orderedPermutations(indexes).stream()
                    .map(permutation -> permutation.stream()
                            .map(node.getPatterns()::get)
                            .collect(toImmutableList()))
                    .collect(toImmutableList());

            alternation(permutations);

            return null;
        }

        private void concatenation(List<IrRowPattern> patterns)
        {
            patterns.stream()
                    .forEach(this::process);
        }

        private void alternation(List<List<IrRowPattern>> parts)
        {
            List<Integer> jumpPositions = new ArrayList<>();

            for (int i = 0; i < parts.size() - 1; i++) {
                int splitPosition = instructions.size();
                instructions.add(null); // placeholder for the Split instruction
                int splitTarget = instructions.size();
                concatenation(parts.get(i));
                jumpPositions.add(instructions.size());
                instructions.add(null); // placeholder for the Jump instruction
                instructions.set(splitPosition, new Split(splitTarget, instructions.size()));
            }

            concatenation(parts.get(parts.size() - 1));

            for (int position : jumpPositions) {
                instructions.set(position, new Jump(instructions.size()));
            }
        }

        @Override
        protected Void visitIrQuantified(IrQuantified node, Void context)
        {
            IrRowPattern pattern = node.getPattern();
            IrQuantifier quantifier = node.getQuantifier();
            boolean greedy = quantifier.isGreedy();

            if (quantifier.getAtMost().isPresent()) {
                rangeQuantified(pattern, greedy, quantifier.getAtLeast(), quantifier.getAtMost().get());
            }
            else {
                loopingQuantified(pattern, greedy, quantifier.getAtLeast());
            }

            return null;
        }

        private void loopingQuantified(IrRowPattern pattern, boolean greedy, int min)
        {
            checkArgument(min >= 0, "invalid min value: " + min);

            if (min == 0) {
                int startSplitPosition = instructions.size();
                instructions.add(null); // placeholder for the Split instruction
                int splitTarget = instructions.size();
                int loopingPosition = instructions.size();

                process(pattern);
                loop(loopingPosition, greedy);

                if (greedy) {
                    instructions.set(startSplitPosition, new Split(splitTarget, instructions.size()));
                }
                else {
                    instructions.set(startSplitPosition, new Split(instructions.size(), splitTarget));
                }
                return;
            }

            int loopingPosition = instructions.size();
            for (int i = 0; i < min; i++) {
                loopingPosition = instructions.size();
                process(pattern);
            }

            loop(loopingPosition, greedy);
        }

        private void loop(int loopingPosition, boolean greedy)
        {
            Split loopingSplit;
            if (greedy) {
                loopingSplit = new Split(loopingPosition, instructions.size() + 1);
            }
            else {
                loopingSplit = new Split(instructions.size() + 1, loopingPosition);
            }
            instructions.add(loopingSplit);
        }

        private void rangeQuantified(IrRowPattern pattern, boolean greedy, int min, int max)
        {
            checkArgument(min <= max, format("invalid range: (%s, %s)", min, max));

            for (int i = 0; i < min; i++) {
                process(pattern);
            }

            // handles 0 without adding instructions
            if (min == max) {
                return;
            }

            List<Integer> splitPositions = new ArrayList<>();
            List<Integer> splitTargets = new ArrayList<>();

            for (int i = min; i < max; i++) {
                splitPositions.add(instructions.size());
                instructions.add(null); // placeholder for the Split instruction
                splitTargets.add(instructions.size());
                process(pattern);
            }

            for (int i = 0; i < splitPositions.size(); i++) {
                if (greedy) {
                    instructions.set(splitPositions.get(i), new Split(splitTargets.get(i), instructions.size()));
                }
                else {
                    instructions.set(splitPositions.get(i), new Split(instructions.size(), splitTargets.get(i)));
                }
            }
        }
    }
}
