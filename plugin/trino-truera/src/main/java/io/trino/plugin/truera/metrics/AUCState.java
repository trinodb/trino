package io.trino.plugin.truera.metrics;

import java.util.ArrayList;
import java.util.List;

public class AUCState implements AUCAccumulatorState {

    private List<Double> actuals;
    private List<Double> predictions;

    public AUCState() {
        this.actuals = new ArrayList<>();
        this.predictions = new ArrayList<>();
    }

    public void setActuals(List<Double> actuals) {
        this.actuals = actuals;
    }

    public void setPredictions(List<Double> predictions) {
        this.predictions = predictions;
    }

    public List<Double> getActuals() {
        return actuals;
    }

    public List<Double> getPredictions() {
        return predictions;
    }

    public long getEstimatedSize() {
        return (long) actuals.size() * Double.BYTES + (long) predictions.size() * Double.BYTES;
    }
}
