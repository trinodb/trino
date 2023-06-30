package io.trino.plugin.truera;

import java.util.Comparator;
import io.airlift.log.Logger;
import java.util.List;
import java.util.stream.IntStream;

public class AreaUnderRocCurveAlgorithm {

    public static double computeRocAuc(List<Double> labels, List<Double> scores) {
        int[] sortedIndices = IntStream.range(0, scores.size()).boxed().sorted(
                Comparator.comparing(i -> scores.get(i), Comparator.reverseOrder())
        ).mapToInt(i->i).toArray();

        int currTruePositives = 0, currFalsePositives = 0;
        double auc = 0.;

        int i = 0;
        while (i < sortedIndices.length) {
            int prevTruePositives = currTruePositives;
            int prevFalsePositives = currFalsePositives;
            double currentScore = scores.get(sortedIndices[i]);
            while (i < sortedIndices.length && currentScore == scores.get(sortedIndices[i])) {
                if (labels.get(sortedIndices[i]) == 1.0) {
                    currTruePositives++;
                } else {
                    currFalsePositives++;
                }
                ++i;
            }
            auc += trapezoidIntegrate(prevFalsePositives, currFalsePositives, prevTruePositives, currTruePositives);
        }

        // If labels only contain one class, AUC is undefined
        if (currTruePositives == 0 || currFalsePositives == 0) {
            return Double.NaN;
        }

        return auc / (currTruePositives * currFalsePositives);
    }

    private static double trapezoidIntegrate(double x1, double x2, double y1, double y2) {
        return (y1 + y2) * Math.abs(x2 - x1) / 2; // (base1 + base2) * height / 2
    }
}
