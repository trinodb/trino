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
package io.trino.operator.aggregation.partial;

import io.trino.operator.HashAggregationOperator;

/**
 * Controls whenever partial aggregation is enabled across all {@link HashAggregationOperator}s
 * for a particular plan node on a single node.
 * Partial aggregation is disabled once enough rows has been processed ({@link #minNumberOfRowsProcessed})
 * and the ratio between output(unique) and input rows is too high (> {@link #uniqueRowsRatioThreshold}).
 * TODO https://github.com/trinodb/trino/issues/11361 add support to adaptively re-enable partial aggregation.
 * <p>
 * The class is thread safe and objects of this class are used potentially by multiple threads/drivers simultaneously.
 * Different threads either:
 * - modify fields via synchronized {@link #onFlush}.
 * - read volatile {@link #partialAggregationDisabled} (volatile here gives visibility).
 */
public class PartialAggregationController
{
    private final long minNumberOfRowsProcessed;
    private final double uniqueRowsRatioThreshold;

    private volatile boolean partialAggregationDisabled;
    private long totalRowProcessed;
    private long totalUniqueRowsProduced;

    public PartialAggregationController(long minNumberOfRowsProcessedToDisable, double uniqueRowsRatioThreshold)
    {
        this.minNumberOfRowsProcessed = minNumberOfRowsProcessedToDisable;
        this.uniqueRowsRatioThreshold = uniqueRowsRatioThreshold;
    }

    public boolean isPartialAggregationDisabled()
    {
        return partialAggregationDisabled;
    }

    public synchronized void onFlush(long rowsProcessed, long uniqueRowsProduced)
    {
        if (partialAggregationDisabled) {
            return;
        }

        totalRowProcessed += rowsProcessed;
        totalUniqueRowsProduced += uniqueRowsProduced;
        if (shouldDisablePartialAggregation()) {
            partialAggregationDisabled = true;
        }
    }

    private boolean shouldDisablePartialAggregation()
    {
        return totalRowProcessed >= minNumberOfRowsProcessed
                && ((double) totalUniqueRowsProduced / totalRowProcessed) > uniqueRowsRatioThreshold;
    }

    public PartialAggregationController duplicate()
    {
        return new PartialAggregationController(minNumberOfRowsProcessed, uniqueRowsRatioThreshold);
    }
}
