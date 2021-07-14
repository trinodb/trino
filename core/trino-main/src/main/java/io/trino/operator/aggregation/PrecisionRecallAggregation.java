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
package io.trino.operator.aggregation;

import com.google.common.collect.Streams;
import io.trino.operator.aggregation.histogram.FixedDoubleHistogram;
import io.trino.operator.aggregation.state.PrecisionRecallState;
import io.trino.spi.TrinoException;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public abstract class PrecisionRecallAggregation
{
    private static final double DEFAULT_WEIGHT = 1.0;
    private static final double MIN_PREDICTION_VALUE = 0.0;
    private static final double MAX_PREDICTION_VALUE = 1.0;
    private static final String ILLEGAL_PREDICTION_VALUE_MESSAGE = String.format(
            "Prediction value must be between %s and %s",
            PrecisionRecallAggregation.MIN_PREDICTION_VALUE,
            PrecisionRecallAggregation.MAX_PREDICTION_VALUE);
    private static final String NEGATIVE_WEIGHT_MESSAGE = "Weights must be non-negative";
    private static final String INCONSISTENT_BUCKET_COUNT_MESSAGE = "Bucket count must be constant";

    protected PrecisionRecallAggregation() {}

    @InputFunction
    public static void input(
            @AggregationState PrecisionRecallState state,
            @SqlType(StandardTypes.BIGINT) long bucketCount,
            @SqlType(StandardTypes.BOOLEAN) boolean outcome,
            @SqlType(StandardTypes.DOUBLE) double pred,
            @SqlType(StandardTypes.DOUBLE) double weight)
    {
        if (state.getTrueWeights() == null) {
            state.setTrueWeights(new FixedDoubleHistogram(
                    (int) (bucketCount),
                    PrecisionRecallAggregation.MIN_PREDICTION_VALUE,
                    PrecisionRecallAggregation.MAX_PREDICTION_VALUE));
            state.setFalseWeights(new FixedDoubleHistogram(
                    (int) (bucketCount),
                    PrecisionRecallAggregation.MIN_PREDICTION_VALUE,
                    PrecisionRecallAggregation.MAX_PREDICTION_VALUE));
        }

        if (pred < MIN_PREDICTION_VALUE || pred > PrecisionRecallAggregation.MAX_PREDICTION_VALUE) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    PrecisionRecallAggregation.ILLEGAL_PREDICTION_VALUE_MESSAGE);
        }
        if (weight < 0) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    PrecisionRecallAggregation.NEGATIVE_WEIGHT_MESSAGE);
        }
        if (bucketCount != state.getTrueWeights().getBucketCount()) {
            throw new TrinoException(
                    INVALID_FUNCTION_ARGUMENT,
                    PrecisionRecallAggregation.INCONSISTENT_BUCKET_COUNT_MESSAGE);
        }

        if (outcome) {
            state.getTrueWeights().add(pred, weight);
        }
        else {
            state.getFalseWeights().add(pred, weight);
        }
    }

    @InputFunction
    public static void input(
            @AggregationState PrecisionRecallState state,
            @SqlType(StandardTypes.BIGINT) long bucketCount,
            @SqlType(StandardTypes.BOOLEAN) boolean outcome,
            @SqlType(StandardTypes.DOUBLE) double pred)
    {
        PrecisionRecallAggregation.input(state, bucketCount, outcome, pred, PrecisionRecallAggregation.DEFAULT_WEIGHT);
    }

    @CombineFunction
    public static void combine(
            @AggregationState PrecisionRecallState state,
            @AggregationState PrecisionRecallState otherState)
    {
        if (state.getTrueWeights() == null && otherState.getTrueWeights() != null) {
            state.setTrueWeights(new FixedDoubleHistogram(otherState.getTrueWeights().clone()));
            state.setFalseWeights(new FixedDoubleHistogram(otherState.getFalseWeights().clone()));
            return;
        }
        if (state.getTrueWeights() != null && otherState.getTrueWeights() != null) {
            state.getTrueWeights().mergeWith(otherState.getTrueWeights());
            state.getFalseWeights().mergeWith(otherState.getFalseWeights());
        }
    }

    protected static class BucketResult
    {
        public final double totalTrueWeight;
        public final double totalFalseWeight;
        public final double runningTrueWeight;
        public final double runningFalseWeight;
        public final double left;
        public final double right;

        public BucketResult(
                double left,
                double right,
                double totalTrueWeight,
                double totalFalseWeight,
                double runningTrueWeight,
                double runningFalseWeight)
        {
            this.left = left;
            this.right = right;
            this.totalTrueWeight = totalTrueWeight;
            this.totalFalseWeight = totalFalseWeight;
            this.runningTrueWeight = runningTrueWeight;
            this.runningFalseWeight = runningFalseWeight;
        }
    }

    protected static Iterator<BucketResult> getResultsIterator(@AggregationState PrecisionRecallState state)
    {
        if (state.getTrueWeights() == null) {
            return Collections.<BucketResult>emptyList().iterator();
        }

        final double totalTrueWeight = Streams.stream(state.getTrueWeights().iterator())
                .mapToDouble(c -> c.weight)
                .sum();
        final double totalFalseWeight = Streams.stream(state.getFalseWeights().iterator())
                .mapToDouble(c -> c.weight)
                .sum();

        return new Iterator<BucketResult>()
        {
            Iterator<FixedDoubleHistogram.Bin> trueIt = state.getTrueWeights().iterator();
            Iterator<FixedDoubleHistogram.Bin> falseIt = state.getFalseWeights().iterator();
            double runningFalseWeight;
            double runningTrueWeight;

            @Override
            public boolean hasNext()
            {
                return trueIt.hasNext() && totalTrueWeight > runningTrueWeight;
            }

            @Override
            public BucketResult next()
            {
                if (!trueIt.hasNext() || !falseIt.hasNext()) {
                    throw new NoSuchElementException();
                }
                final FixedDoubleHistogram.Bin trueResult = trueIt.next();
                final FixedDoubleHistogram.Bin falseResult = falseIt.next();

                final BucketResult result = new BucketResult(
                        trueResult.left,
                        trueResult.right,
                        totalTrueWeight,
                        totalFalseWeight,
                        runningTrueWeight,
                        runningFalseWeight);

                runningTrueWeight += trueResult.weight;
                runningFalseWeight += falseResult.weight;

                return result;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
