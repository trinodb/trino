package io.trino.plugin.truera.metrics;

import io.trino.spi.function.AccumulatorStateFactory;

public class AUCAccumulatorStateFactory implements AccumulatorStateFactory<AUCAccumulatorState> {
    @Override
    public AUCAccumulatorState createSingleState() {
        return new AUCState();
    }

    @Override
    public AUCAccumulatorState createGroupedState() {
        return new AUCState();
    }
}
