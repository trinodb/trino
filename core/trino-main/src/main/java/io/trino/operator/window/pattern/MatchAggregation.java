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

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.aggregation.Accumulator;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.operator.aggregation.InternalAggregationFunction;
import io.trino.operator.aggregation.LambdaProvider;
import io.trino.operator.window.matcher.ArrayView;
import io.trino.operator.window.pattern.SetEvaluator.SetEvaluatorSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class computes an aggregate function result in row pattern recognition context.
 * <p>
 * Expressions in DEFINE and MEASURES clauses can contain aggregate functions. Each of
 * these aggregate functions is transformed into an instance of `MatchAggregation` class.
 * <p>
 * Whenever the aggregate function needs to be evaluated , the method `aggregate()` is called.
 * The returned value is then used to evaluate the enclosing expression.
 * <p>
 * The aggregate function needs to be evaluated in certain cases:
 * 1. during the pattern matching phase, e.g.
 * with a defining condition: `DEFINE A AS avg(B.x) > 0`,
 * the aggregate function `avg` needs to be evaluated over all rows matched so far to
 * label `B` every time the matching algorithm tries to match label `A`.
 * 2. during row pattern measures computation, e.g.
 * with `MEASURES M1 AS RUNNING sum(A.x)`,
 * the running sum must be evaluated over all rows matched to label `A` up to every row
 * included in the match;
 * with `MEASURES M2 AS FINAL sum(A.x)`,
 * the overall sum must be computed for rows matched to label `A` in the entire match,
 * and the result must be propagated for every output row.
 * <p>
 * To avoid duplicate computations, `MatchAggregation` is stateful. The state consists of:
 * - the accumulator, which holds the partial result
 * - the setEvaluator, which determines the new positions to aggregate over since the
 * previous call
 * If the `MatchAggregation` instance is going to be reused for different matches, it has
 * to be `reset` before a new match.
 */
public class MatchAggregation
{
    private static final int ROWS_UNTIL_MEMORY_REPORT = 1000;

    private final String name;
    private final List<Integer> argumentChannels;
    private final AccumulatorFactory accumulatorFactory;
    private final SetEvaluator setEvaluator;
    private final AggregatedMemoryContext memoryContextSupplier;
    private final LocalMemoryContext memoryContext;

    private Accumulator accumulator;
    private int rowsFromMemoryReport;
    private Block resultOnEmpty;

    private MatchAggregation(InternalAggregationFunction function, List<Integer> argumentChannels, List<LambdaProvider> lambdaProviders, SetEvaluator setEvaluator, AggregatedMemoryContext memoryContextSupplier)
    {
        this.name = function.name();
        this.argumentChannels = ImmutableList.copyOf(argumentChannels);
        this.accumulatorFactory = function.bind(
                argumentChannels,
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                false,
                null,
                null,
                lambdaProviders,
                false,
                null);

        this.setEvaluator = setEvaluator;
        this.memoryContextSupplier = memoryContextSupplier;
        this.memoryContext = memoryContextSupplier.newLocalMemoryContext(MatchAggregation.class.getSimpleName());
        resetAccumulator();
    }

    // for copying when forking threads during pattern matching phase
    private MatchAggregation(String name, List<Integer> argumentChannels, AccumulatorFactory accumulatorFactory, SetEvaluator setEvaluator, Accumulator accumulator, AggregatedMemoryContext memoryContextSupplier)
    {
        this.name = name;
        this.argumentChannels = argumentChannels;
        this.accumulatorFactory = accumulatorFactory;
        this.setEvaluator = setEvaluator;
        this.memoryContextSupplier = memoryContextSupplier;
        this.memoryContext = memoryContextSupplier.newLocalMemoryContext(MatchAggregation.class.getSimpleName());
        this.accumulator = accumulator;
    }

    // reset for a new match during measure computations phase
    public void reset()
    {
        resetAccumulator();
        setEvaluator.reset();
        rowsFromMemoryReport = 0;
    }

    private void resetAccumulator()
    {
        accumulator = accumulatorFactory.createAccumulator();
    }

    /**
     * Identify the new positions for aggregation since the last time this aggregation was run,
     * and add them to `accumulator`. Return the overall aggregation result.
     * This method is used for:
     * - Evaluating labels during pattern matching. In this case, the evaluated label has been appended to `matchedLabels`,
     * - Computing row pattern measures after a non-empty match is found.
     */
    public Block aggregate(int currentRow, ArrayView matchedLabels, long matchNumber, ProjectingPagesWindowIndex windowIndex, int partitionStart, int patternStart)
    {
        // new positions to aggregate since the last time this aggregation was run
        ArrayView positions = setEvaluator.resolveNewPositions(currentRow, matchedLabels, partitionStart, patternStart);
        for (int i = 0; i < positions.length(); i++) {
            int position = positions.get(i); // position from partition start
            windowIndex.setLabelAndMatchNumber(position, matchedLabels.get(position + partitionStart - patternStart), matchNumber);
            accumulator.addInput(windowIndex, argumentChannels, position, position);
        }

        // report accumulator and SetEvaluator memory usage every time a new portion of `ROWS_UNTIL_MEMORY_REPORT` rows was aggregated
        rowsFromMemoryReport += positions.length();
        if (rowsFromMemoryReport >= ROWS_UNTIL_MEMORY_REPORT) {
            rowsFromMemoryReport = 0;
            memoryContext.setBytes(accumulator.getEstimatedSize() + setEvaluator.getAllPositionsSizeInBytes());
        }

        BlockBuilder blockBuilder = accumulator.getFinalType().createBlockBuilder(null, 1);
        accumulator.evaluateFinal(blockBuilder);
        return blockBuilder.build();
    }

    /**
     * Aggregate over empty input. This method is used for computing row pattern measures for empty matches.
     * According to the SQL specification, in such case:
     * - count() aggregation should return 0,
     * - all other aggregations should return null.
     * In Trino, certain aggregations do not follow this pattern (e.g. count_if).
     * This implementation is consistent with aggregations behavior in Trino.
     */
    public Block aggregateEmpty()
    {
        if (resultOnEmpty != null) {
            return resultOnEmpty;
        }
        BlockBuilder blockBuilder = accumulator.getFinalType().createBlockBuilder(null, 1);
        accumulatorFactory.createAccumulator().evaluateFinal(blockBuilder);
        resultOnEmpty = blockBuilder.build();
        return resultOnEmpty;
    }

    // for ThreadEquivalence
    public ArrayView getAllPositions(ArrayView labels)
    {
        return setEvaluator.getAllPositions(labels);
    }

    public MatchAggregation copy()
    {
        Accumulator accumulatorCopy;
        try {
            accumulatorCopy = accumulator.copy();
        }
        catch (UnsupportedOperationException e) {
            throw new TrinoException(NOT_SUPPORTED, format("aggregate function %s does not support copying", name), e);
        }

        return new MatchAggregation(name, argumentChannels, accumulatorFactory, setEvaluator.copy(), accumulatorCopy, memoryContextSupplier);
    }

    public static class MatchAggregationInstantiator
    {
        private final InternalAggregationFunction function;
        private final List<Integer> argumentChannels;
        private final List<LambdaProvider> lambdaProviders;
        private final SetEvaluatorSupplier setEvaluatorSupplier;

        public MatchAggregationInstantiator(InternalAggregationFunction function, List<Integer> argumentChannels, List<LambdaProvider> lambdaProviders, SetEvaluatorSupplier setEvaluatorSupplier)
        {
            this.function = requireNonNull(function, "function is null");
            this.argumentChannels = requireNonNull(argumentChannels, "argumentChannels is null");
            this.lambdaProviders = requireNonNull(lambdaProviders, "lambdaProviders is null");
            this.setEvaluatorSupplier = requireNonNull(setEvaluatorSupplier, "setEvaluatorSupplier is null");
        }

        public MatchAggregation get(AggregatedMemoryContext memoryContextSupplier)
        {
            requireNonNull(memoryContextSupplier, "memoryContextSupplier is null");
            return new MatchAggregation(function, argumentChannels, lambdaProviders, setEvaluatorSupplier.get(), memoryContextSupplier);
        }
    }
}
