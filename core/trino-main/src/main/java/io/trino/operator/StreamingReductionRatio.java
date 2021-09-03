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
package io.trino.operator;

import static java.lang.String.format;

/**
 * Not thread safe, needs to be held in AtomicReference
 */
public class StreamingReductionRatio
{
    // TODO: Make this a system session property
    private static final long STATS_SAMPLING_MIN_ROW_COUNT = 3000;
    private static final double DATA_REDUCTION_THRESHOLD = 0.85;

    private long sampledRowCountSoFar;
    private double reductionRatioAvg;

    public StreamingReductionRatio()
    {
        this.sampledRowCountSoFar = 0;
        this.reductionRatioAvg = 0;
    }

    public void update(double reductionRatioSample)
    {
        if (sampledRowCountSoFar >= STATS_SAMPLING_MIN_ROW_COUNT) {
            return;
        }

        ++sampledRowCountSoFar;
        double difference = (reductionRatioSample - reductionRatioAvg) / sampledRowCountSoFar;
        reductionRatioAvg += difference;
    }

    public boolean isSampleSizeTooSmall()
    {
        return sampledRowCountSoFar < STATS_SAMPLING_MIN_ROW_COUNT;
    }

    public boolean isInsufficient()
    {
        // resulting group count is larger than threshold to apply PA dynamically.
        return reductionRatioAvg > DATA_REDUCTION_THRESHOLD;
    }

    public double getAverage()
    {
        return reductionRatioAvg;
    }

    public String toString()
    {
        return format(
                "[row count sample: %d, reduction ratio (avg): %f]",
                sampledRowCountSoFar,
                reductionRatioAvg);
    }
}
