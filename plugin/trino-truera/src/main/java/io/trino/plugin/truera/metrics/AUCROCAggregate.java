package io.trino.plugin.truera.metrics;

import static io.trino.plugin.truera.AreaUnderRocCurveAlgorithm.computeRocAuc;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AggregationFunction;
import io.trino.spi.function.AggregationState;
import io.trino.spi.function.CombineFunction;
import io.trino.spi.function.Description;
import io.trino.spi.function.InputFunction;
import io.trino.spi.function.OutputFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;

import java.util.Collections;
import java.util.List;

@AggregationFunction("auc_roc")
@Description("Calculates the Area Under the Curve (AUC)")
public class AUCROCAggregate {

    @InputFunction
    public static void input(@AggregationState AUCAccumulatorState state, @SqlType(StandardTypes.DOUBLE) double actual, @SqlType(StandardTypes.DOUBLE) double prediction) {
        state.getActuals().add(actual);
        state.getPredictions().add(prediction);
    }

    @CombineFunction
    public static void combine(@AggregationState AUCAccumulatorState state, @AggregationState AUCAccumulatorState otherState) {
        state.getActuals().addAll(otherState.getActuals());
        state.getPredictions().addAll(otherState.getPredictions());
    }

    @OutputFunction(StandardTypes.DOUBLE)
    public static void output(@AggregationState AUCAccumulatorState state, BlockBuilder out) {
        List<Double> actuals = state.getActuals();
        List<Double> predictions = state.getPredictions();

        // Validate inputs
        if (actuals == null || predictions == null || actuals.size() != predictions.size()) {
            throw new IllegalArgumentException("Invalid input");
        }

        double auc = computeRocAuc(actuals, predictions);

        DoubleType.DOUBLE.writeDouble(out, auc);
    }
}
