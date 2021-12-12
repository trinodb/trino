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
package io.trino.operator.window.pattern;

import io.trino.operator.window.matcher.ArrayView;
import io.trino.operator.window.matcher.IntList;
import io.trino.sql.planner.rowpattern.AggregatedSetDescriptor;
import io.trino.sql.planner.rowpattern.ir.IrLabel;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * This class returns a set of positions to aggregate over for an aggregate function in row pattern matching context.
 * Aggregations in row pattern matching context have RUNNING or FINAL semantics, and they apply only to rows
 * matched with certain pattern variables. For example, for a match "A B A A B", the aggregation `sum(B.x)` only
 * applies to the second and the last position.
 * <p>
 * This evaluator is stateful. It requires a reset for every new match.
 * The method `resolveNewPositions()` returns a portion of positions corresponding to the new portion of the match
 * since the last call. It is thus assumed that a sequence of calls since the instance creation or `reset()`
 * applies to the same match (i.e. the `matchedLabels` passed as an argument to one call is prefix of
 * `matchedLabels` passed in the next call).
 * <p>
 * Also, a full list of positions for a current match is kept, and it can be obtained via the `getAllPositions()`
 * method. This is for the purpose comparing pattern matching threads in ThreadEquivalence.
 */
public class SetEvaluator
{
    private static final int DEFAULT_CAPACITY = 10;

    private final Set<Integer> labels;
    private final boolean running;

    // length of the aggregated prefix of the match
    private int aggregated;

    // length of the prefix of the match where all applicable positions were identified,
    // and stored in `allPositions`. During the pattern matching phase it might exceed
    // the `aggregated` prefix due to `getAllPositions()` calls.
    private int evaluated;

    // all identified applicable positions for the current match until the `evaluated` position, starting from 0
    // this list is updated:
    // - by `resolveNewPositions()`
    // - by `getAllPositions()` (only during the pattern matching phase)
    private final IntList allPositions;

    public SetEvaluator(Set<Integer> labels, boolean running)
    {
        this.labels = requireNonNull(labels, "labels is null");
        this.running = running;
        this.allPositions = new IntList(DEFAULT_CAPACITY);
    }

    // for copying
    private SetEvaluator(Set<Integer> labels, boolean running, int aggregated, int evaluated, IntList allPositions)
    {
        this.labels = labels;
        this.running = running;
        this.aggregated = aggregated;
        this.evaluated = evaluated;
        this.allPositions = allPositions;
    }

    public void reset()
    {
        aggregated = 0;
        evaluated = 0;
        allPositions.clear();
    }

    /**
     * This method is used for resolving positions for aggregation:
     * - During pattern matching. In this case, the evaluated label has been appended to `matchedLabels`
     * - When computing row pattern measures after a non-empty match is found.
     * Search is limited up to the current row in case of RUNNING semantics and to the entire match in case of FINAL semantics.
     * <p>
     * TODO If `evaluated` exceeds `aggregated`, we could reuse the pre-evaluated positions. For that,
     * we need to keep count of all previously returned positions from the `aggregated` prefix.
     *
     * @return array of new matching positions since the last call, relative to partition start
     */
    public ArrayView resolveNewPositions(int currentRow, ArrayView matchedLabels, int partitionStart, int patternStart)
    {
        checkArgument(currentRow >= patternStart && currentRow < patternStart + matchedLabels.length(), "current row is out of bounds of the match");
        checkState(aggregated <= evaluated && evaluated <= matchedLabels.length(), "SetEvaluator in inconsistent state");

        IntList positions = new IntList(DEFAULT_CAPACITY);
        int last = running ? currentRow - patternStart : matchedLabels.length() - 1;

        // return positions exceeding the `aggregated` prefix
        for (int position = aggregated; position <= last; position++) {
            if (appliesToLabel(matchedLabels.get(position))) {
                positions.add(position + patternStart - partitionStart);
                if (aggregated >= evaluated) {
                    // after exceeding the `evaluated` prefix, store resolved positions
                    allPositions.add(position);
                }
            }
            aggregated++;
        }
        evaluated = aggregated;

        return positions.toArrayView();
    }

    // for ThreadEquivalence
    // return all positions to aggregate in `labels` starting from 0
    public ArrayView getAllPositions(ArrayView labels)
    {
        checkState(evaluated <= labels.length(), "SetEvaluator in inconsistent state");

        for (int position = evaluated; position < labels.length(); position++) {
            if (appliesToLabel(labels.get(position))) {
                allPositions.add(position);
            }
        }
        evaluated = labels.length();

        return allPositions.toArrayView();
    }

    private boolean appliesToLabel(int label)
    {
        return labels.isEmpty() || labels.contains(label);
    }

    public SetEvaluator copy()
    {
        return new SetEvaluator(labels, running, aggregated, evaluated, allPositions.copy());
    }

    public long getAllPositionsSizeInBytes()
    {
        return allPositions.getSizeInBytes();
    }

    public static class SetEvaluatorSupplier
    {
        private final AggregatedSetDescriptor setDescriptor;
        private final Map<IrLabel, Integer> mapping;

        public SetEvaluatorSupplier(AggregatedSetDescriptor setDescriptor, Map<IrLabel, Integer> mapping)
        {
            this.setDescriptor = requireNonNull(setDescriptor, "setDescriptor is null");
            this.mapping = requireNonNull(mapping, "mapping is null");
        }

        public SetEvaluator get()
        {
            return new SetEvaluator(
                    setDescriptor.getLabels().stream()
                            .map(mapping::get)
                            .collect(toImmutableSet()),
                    setDescriptor.isRunning());
        }
    }
}
