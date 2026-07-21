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
package io.trino.spi.statistics;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Statistics for a group of columns, capturing correlations between them.
 * The primary metric is the joint number of distinct value combinations,
 * which corrects the optimizer's independence assumption when estimating
 * GROUP BY cardinality and multi-predicate selectivity.
 */
public final class ColumnGroupStatistics
{
    private static final ColumnGroupStatistics EMPTY = new ColumnGroupStatistics(Estimate.unknown());

    private final Estimate distinctValuesCount;

    public static ColumnGroupStatistics empty()
    {
        return EMPTY;
    }

    public ColumnGroupStatistics(Estimate distinctValuesCount)
    {
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
        if (!distinctValuesCount.isUnknown() && distinctValuesCount.getValue() < 0) {
            throw new IllegalArgumentException(format("distinctValuesCount must be greater than or equal to 0: %s", distinctValuesCount.getValue()));
        }
    }

    /**
     * Returns the estimated number of distinct value combinations for this column group.
     * This is the joint NDV — it accounts for correlations between columns rather than
     * assuming statistical independence.
     */
    public Estimate getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnGroupStatistics that = (ColumnGroupStatistics) o;
        return Objects.equals(distinctValuesCount, that.distinctValuesCount);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(distinctValuesCount);
    }

    @Override
    public String toString()
    {
        return "ColumnGroupStatistics{" +
                "distinctValuesCount=" + distinctValuesCount +
                '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private Estimate distinctValuesCount = Estimate.unknown();

        public Builder setDistinctValuesCount(Estimate distinctValuesCount)
        {
            this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
            return this;
        }

        public ColumnGroupStatistics build()
        {
            return new ColumnGroupStatistics(distinctValuesCount);
        }
    }
}
