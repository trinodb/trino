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
package io.trino.plugin.base.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

/**
 * This class is NOT thread safe.
 */
public class TDigest
{
    public static final double DEFAULT_COMPRESSION = 100;

    private static final int FORMAT_TAG = 0;
    private static final int T_DIGEST_SIZE = instanceSize(TDigest.class);
    private static final int INITIAL_CAPACITY = 1;
    private static final int FUDGE_FACTOR = 10;

    private final int maxSize;
    private final double compression;

    double[] means;
    double[] weights;
    int centroidCount;
    double totalWeight;

    double min;
    double max;

    private boolean backwards;
    private boolean needsMerge;

    private int[] indexes;
    private double[] tempMeans;
    private double[] tempWeights;

    public TDigest()
    {
        this(DEFAULT_COMPRESSION);
    }

    public TDigest(double compression)
    {
        this(
                compression,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                0,
                0,
                new double[INITIAL_CAPACITY],
                new double[INITIAL_CAPACITY],
                false,
                false);
    }

    private TDigest(
            double compression,
            double min,
            double max,
            double totalWeight,
            int centroidCount,
            double[] means,
            double[] weights,
            boolean needsMerge,
            boolean backwards)
    {
        checkArgument(compression >= 10, "compression factor too small (< 10)");

        this.compression = compression;
        this.maxSize = (int) (6 * (internalCompressionFactor(compression) + FUDGE_FACTOR)); // 5 * size + size (for centroids + new values)
        this.totalWeight = totalWeight;
        this.min = min;
        this.max = max;
        this.centroidCount = centroidCount;
        this.means = requireNonNull(means, "means is null");
        this.weights = requireNonNull(weights, "weights is null");
        this.needsMerge = needsMerge;
        this.backwards = backwards;
    }

    public static TDigest copyOf(TDigest other)
    {
        return new TDigest(
                other.compression,
                other.min,
                other.max,
                other.totalWeight,
                other.centroidCount,
                Arrays.copyOf(other.means, other.centroidCount),
                Arrays.copyOf(other.weights, other.centroidCount),
                other.needsMerge,
                other.backwards);
    }

    public static TDigest deserialize(Slice serialized)
    {
        SliceInput input = serialized.getInput();

        byte format = input.readByte();
        checkArgument(format == FORMAT_TAG, "Invalid format");

        double min = input.readDouble();
        double max = input.readDouble();
        double compression = input.readDouble();
        double totalWeight = input.readDouble();
        int centroidCount = input.readInt();

        double[] means = new double[centroidCount];
        for (int i = 0; i < centroidCount; i++) {
            means[i] = input.readDouble();
        }

        double[] weights = new double[centroidCount];
        for (int i = 0; i < centroidCount; i++) {
            weights[i] = input.readDouble();
        }

        return new TDigest(
                compression,
                min,
                max,
                totalWeight,
                centroidCount,
                means,
                weights,
                false,
                false);
    }

    public double getMin()
    {
        if (totalWeight == 0) {
            return Double.NaN;
        }
        return min;
    }

    public double getMax()
    {
        if (totalWeight == 0) {
            return Double.NaN;
        }
        return max;
    }

    public double getCount()
    {
        return totalWeight;
    }

    public void add(double value)
    {
        add(value, 1);
    }

    public void add(double value, double weight)
    {
        checkArgument(!isNaN(value), "value is NaN");
        checkArgument(!isNaN(weight), "weight is NaN");
        checkArgument(!isInfinite(value), "value must be finite");
        checkArgument(!isInfinite(weight), "weight must be finite");

        if (centroidCount == means.length) {
            if (means.length < maxSize) {
                ensureCapacity(Math.min(Math.max(means.length * 2, INITIAL_CAPACITY), maxSize));
            }
            else {
                merge(internalCompressionFactor(compression));
                if (centroidCount >= means.length) {
                    throw new AssertionError("Invalid size estimation for T-Digest: " + Base64.getEncoder().encodeToString(serializeInternal().getBytes()));
                }
            }
        }

        means[centroidCount] = value;
        weights[centroidCount] = weight;
        centroidCount++;

        totalWeight += weight;
        min = Math.min(value, min);
        max = Math.max(value, max);

        needsMerge = true;
    }

    public void mergeWith(TDigest other)
    {
        if (centroidCount + other.centroidCount > means.length) {
            // first, try to compact the digests to make room
            merge(internalCompressionFactor(compression));
            other.merge(internalCompressionFactor(compression));

            // but if that's not sufficient to fit all clusters, grow the arrays
            ensureCapacity(centroidCount + other.centroidCount);
        }

        System.arraycopy(other.means, 0, means, centroidCount, other.centroidCount);
        System.arraycopy(other.weights, 0, weights, centroidCount, other.centroidCount);

        centroidCount += other.centroidCount;
        totalWeight += other.totalWeight;

        min = Math.min(min, other.min);
        max = Math.max(max, other.max);

        needsMerge = true;
    }

    public double valueAt(double quantile)
    {
        return valuesAt(quantile)[0];
    }

    public List<Double> valuesAt(List<Double> quantiles)
    {
        return Doubles.asList(valuesAt(Doubles.toArray(quantiles)));
    }

    public double[] valuesAt(double... quantiles)
    {
        if (quantiles.length == 0) {
            return new double[0];
        }

        validateQuantilesArgument(quantiles);

        double[] result = new double[quantiles.length];

        if (centroidCount == 0) {
            Arrays.fill(result, Double.NaN);
            return result;
        }

        mergeIfNeeded(internalCompressionFactor(compression));

        if (centroidCount == 1) {
            Arrays.fill(result, means[0]);
            return result;
        }

        // offsets into the theoretical sequence of all values
        for (int i = 0; i < result.length; i++) {
            result[i] = quantiles[i] * totalWeight;
        }

        int index = 0;
        // lowest value
        while (index < result.length && result[index] < 1) {
            result[index] = min;
            index++;
        }
        // between bottom and first centroid
        while (index < result.length && result[index] < weights[0] / 2) {
            result[index] = (min + interpolate(result[index], 1, min, weights[0] / 2, means[0]));
            index++;
        }
        // between last centroid and top, but not the greatest value
        while (index < result.length && result[index] <= totalWeight - 1 && totalWeight - result[index] <= weights[centroidCount - 1] / 2 && weights[centroidCount - 1] / 2 > 1) {
            // we interpolate back from the end, so the value is negative
            result[index] = (max + interpolate(totalWeight - result[index], 1, max, weights[centroidCount - 1] / 2, means[centroidCount - 1]));
            index++;
        }
        // greatest value
        if (index < result.length && result[index] >= totalWeight - 1) {
            Arrays.fill(result, index, result.length, max);
            return result;
        }

        double weightSoFar = weights[0] / 2;
        int currentCentroid = 0;
        while (index < result.length) {
            double delta = (weights[currentCentroid] + weights[currentCentroid + 1]) / 2;
            while (currentCentroid < centroidCount - 1 && weightSoFar + delta <= result[index]) {
                weightSoFar += delta;
                currentCentroid++;
                if (currentCentroid < centroidCount - 1) {
                    delta = (weights[currentCentroid] + weights[currentCentroid + 1]) / 2;
                }
            }
            // past the last centroid
            if (currentCentroid == centroidCount - 1) {
                // between last centroid and top, but not the greatest value
                while (index < result.length && result[index] <= totalWeight - 1 && weights[centroidCount - 1] / 2 > 1) {
                    // we interpolate back from the end, so the value is negative
                    result[index] = (max + interpolate(totalWeight - result[index], 1, max, weights[centroidCount - 1] / 2, means[centroidCount - 1]));
                    index++;
                }
                // greatest value
                if (index < result.length) {
                    Arrays.fill(result, index, result.length, max);
                }
                return result;
            }
            else {
                // single-sample cluster on the left (current centroid) and the quantile falls within that cluster
                if (weights[currentCentroid] == 1 && result[index] - weightSoFar < weights[currentCentroid] / 2) {
                    result[index] = means[currentCentroid];
                }
                // single-sample cluster on the right (next centroid) and the quantile falls within that cluster
                else if (weights[currentCentroid + 1] == 1 && result[index] - weightSoFar >= weights[currentCentroid] / 2) {
                    result[index] = means[currentCentroid + 1];
                }
                // the quantile falls within a multi-sample cluster. If the other cluster is single-sample, we can exclude it from interpolation
                else {
                    double interpolationOffset = result[index] - weightSoFar;
                    double interpolationSectionLength = delta;
                    if (weights[currentCentroid] == 1) {
                        interpolationOffset -= weights[currentCentroid] / 2;
                        interpolationSectionLength = weights[currentCentroid + 1] / 2;
                    }
                    else if (weights[currentCentroid + 1] == 1) {
                        interpolationSectionLength = weights[currentCentroid] / 2;
                    }
                    result[index] = (means[currentCentroid] + interpolate(interpolationOffset, 0, means[currentCentroid], interpolationSectionLength, means[currentCentroid + 1]));
                }
                index++;
            }
        }

        return result;
    }

    private static void validateQuantilesArgument(double[] quantiles)
    {
        for (int i = 0; i < quantiles.length; i++) {
            double quantile = quantiles[i];
            if (i > 0 && quantile < quantiles[i - 1]) {
                throw new IllegalArgumentException("quantiles must be sorted in increasing order");
            }
            else if (quantile < 0 || quantile > 1) {
                throw new IllegalArgumentException("quantiles should be in [0, 1] range");
            }
        }
    }

    public Slice serialize()
    {
        merge(compression);
        return serializeInternal();
    }

    private Slice serializeInternal()
    {
        Slice result = Slices.allocate(serializedSizeInBytes());
        SliceOutput output = result.getOutput();

        output.writeByte(TDigest.FORMAT_TAG);
        output.writeDouble(min);
        output.writeDouble(max);
        output.writeDouble(compression);
        output.writeDouble(totalWeight);
        output.writeInt(centroidCount);
        for (int i = 0; i < centroidCount; i++) {
            output.writeDouble(means[i]);
        }
        for (int i = 0; i < centroidCount; i++) {
            output.writeDouble(weights[i]);
        }

        checkState(!output.isWritable(), "Expected serialized size doesn't match actual written size");

        return result;
    }

    public int serializedSizeInBytes()
    {
        return SizeOf.SIZE_OF_BYTE + // format
                SizeOf.SIZE_OF_DOUBLE + // min
                SizeOf.SIZE_OF_DOUBLE + // max
                SizeOf.SIZE_OF_DOUBLE + // compression
                SizeOf.SIZE_OF_DOUBLE + // totalWeight
                SizeOf.SIZE_OF_INT + // centroid count
                SizeOf.SIZE_OF_DOUBLE * centroidCount + // means
                SizeOf.SIZE_OF_DOUBLE * centroidCount; // weights
    }

    public int estimatedInMemorySizeInBytes()
    {
        return (int) (T_DIGEST_SIZE +
                SizeOf.sizeOf(means) +
                SizeOf.sizeOf(weights) +
                SizeOf.sizeOf(tempMeans) +
                SizeOf.sizeOf(tempWeights) +
                SizeOf.sizeOf(indexes));
    }

    private void merge(double compression)
    {
        if (centroidCount == 0) {
            return;
        }

        initializeIndexes();

        DoubleArrays.quickSortIndirect(indexes, means, 0, centroidCount);
        if (backwards) {
            Ints.reverse(indexes, 0, centroidCount);
        }

        double centroidMean = means[indexes[0]];
        double centroidWeight = weights[indexes[0]];

        if (tempMeans == null) {
            tempMeans = new double[INITIAL_CAPACITY];
            tempWeights = new double[INITIAL_CAPACITY];
        }

        int lastCentroid = 0;
        tempMeans[lastCentroid] = centroidMean;
        tempWeights[lastCentroid] = centroidWeight;

        double weightSoFar = 0;
        double normalizer = normalizer(compression, totalWeight);
        double currentQuantile = 0;
        double currentQuantileMaxClusterSize = maxRelativeClusterSize(currentQuantile, normalizer);

        for (int i = 1; i < centroidCount; i++) {
            int index = indexes[i];
            double entryWeight = weights[index];
            double entryMean = means[index];

            double tentativeWeight = centroidWeight + entryWeight;
            double tentativeQuantile = Math.min((weightSoFar + tentativeWeight) / totalWeight, 1);

            double maxClusterWeight = totalWeight * Math.min(currentQuantileMaxClusterSize, maxRelativeClusterSize(tentativeQuantile, normalizer));
            if (tentativeWeight <= maxClusterWeight) {
                // weighted average of the two centroids
                centroidMean = centroidMean + (entryMean - centroidMean) * entryWeight / tentativeWeight;
                centroidWeight = tentativeWeight;
            }
            else {
                lastCentroid++;

                weightSoFar += centroidWeight;
                currentQuantile = weightSoFar / totalWeight;
                currentQuantileMaxClusterSize = maxRelativeClusterSize(currentQuantile, normalizer);

                centroidWeight = entryWeight;
                centroidMean = entryMean;
            }

            ensureTempCapacity(lastCentroid);
            tempMeans[lastCentroid] = centroidMean;
            tempWeights[lastCentroid] = centroidWeight;
        }

        centroidCount = lastCentroid + 1;

        if (backwards) {
            Doubles.reverse(tempMeans, 0, centroidCount);
            Doubles.reverse(tempWeights, 0, centroidCount);
        }
        backwards = !backwards;

        System.arraycopy(tempMeans, 0, means, 0, centroidCount);
        System.arraycopy(tempWeights, 0, weights, 0, centroidCount);
    }

    @VisibleForTesting
    void forceMerge()
    {
        merge(internalCompressionFactor(compression));
    }

    @VisibleForTesting
    int getCentroidCount()
    {
        return centroidCount;
    }

    private void mergeIfNeeded(double compression)
    {
        if (needsMerge) {
            merge(compression);
        }
    }

    private void ensureCapacity(int newSize)
    {
        if (means.length < newSize) {
            means = Arrays.copyOf(means, newSize);
            weights = Arrays.copyOf(weights, newSize);
        }
    }

    private void ensureTempCapacity(int capacity)
    {
        if (tempMeans.length <= capacity) {
            int newSize = capacity + (int) Math.ceil(capacity * 0.5);
            tempMeans = Arrays.copyOf(tempMeans, newSize);
            tempWeights = Arrays.copyOf(tempWeights, newSize);
        }
    }

    private void initializeIndexes()
    {
        if (indexes == null || indexes.length != means.length) {
            indexes = new int[means.length];
        }
        for (int i = 0; i < centroidCount; i++) {
            indexes[i] = i;
        }
    }

    private static double interpolate(double x, double x0, double y0, double x1, double y1)
    {
        return (x - x0) / (x1 - x0) * (y1 - y0);
    }

    private static double maxRelativeClusterSize(double quantile, double normalizer)
    {
        return quantile * (1 - quantile) / normalizer;
    }

    private static double normalizer(double compression, double weight)
    {
        return compression / (4 * Math.log(weight / compression) + 24);
    }

    private static double internalCompressionFactor(double compression)
    {
        return 2 * compression;
    }
}
