package io.trino.plugin.truera.metrics;

import io.trino.spi.function.AccumulatorState;
import io.trino.spi.function.AccumulatorStateMetadata;
import java.util.ArrayList;
import java.util.List;

@AccumulatorStateMetadata(stateFactoryClass = AUCAccumulatorStateFactory.class, stateSerializerClass = AUCAccumulatorStateSerializer.class)
public interface AUCAccumulatorState extends AccumulatorState {
    List<Double> getActuals();

    List<Double> getPredictions();

    void setActuals(List<Double> actuals);
    void setPredictions(List<Double> predictions);

    long getEstimatedSize();
}

