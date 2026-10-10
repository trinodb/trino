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
package io.trino.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.FixedWidthType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.util.MoreMath.firstNonNaN;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PlanNodeStatsEstimate
{
    private static final double DEFAULT_DATA_SIZE_PER_COLUMN = 50;
    private static final PlanNodeStatsEstimate UNKNOWN = new PlanNodeStatsEstimate(NaN, ImmutableMap.of());

    private final double outputRowCount;
    private final Map<Symbol, SymbolStatsEstimate> symbolStatistics;

    public static PlanNodeStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    @JsonCreator
    public PlanNodeStatsEstimate(
            @JsonProperty("outputRowCount") double outputRowCount,
            @JsonProperty("symbolStatistics") Map<Symbol, SymbolStatsEstimate> symbolStatistics)
    {
        checkArgument(isNaN(outputRowCount) || outputRowCount >= 0, "outputRowCount cannot be negative");
        this.outputRowCount = outputRowCount;
        // copyOf returns the same instance when it is already an ImmutableMap, so build() pays no copy
        this.symbolStatistics = ImmutableMap.copyOf(requireNonNull(symbolStatistics, "symbolStatistics is null"));
    }

    /**
     * Returns estimated number of rows.
     * Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getOutputRowCount()
    {
        return outputRowCount;
    }

    /**
     * Returns estimated data size.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputSizeInBytes(Collection<Symbol> outputSymbols)
    {
        requireNonNull(outputSymbols, "outputSymbols is null");

        return outputSymbols.stream()
                .mapToDouble(symbol -> getOutputSizeForSymbol(getSymbolStatistics(symbol), symbol.type()))
                .sum();
    }

    private double getOutputSizeForSymbol(SymbolStatsEstimate symbolStatistics, Type type)
    {
        checkArgument(type != null, "type is null");

        double nullsFraction = firstNonNaN(symbolStatistics.getNullsFraction(), 0d);
        double numberOfNonNullRows = outputRowCount * (1.0 - nullsFraction);

        double outputSize = 0;

        // account for "is null" boolean array
        outputSize += outputRowCount;

        if (type instanceof FixedWidthType fixedType) {
            outputSize += numberOfNonNullRows * fixedType.getFixedSize();
        }
        else {
            double averageRowSize = firstNonNaN(symbolStatistics.getAverageRowSize(), DEFAULT_DATA_SIZE_PER_COLUMN);
            outputSize += numberOfNonNullRows * averageRowSize;

            // account for offsets array
            outputSize += outputRowCount * Integer.BYTES;
            // TODO some types may have more overhead than just offsets array
        }

        return outputSize;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCount(mappingFunction.apply(outputRowCount)).build();
    }

    public PlanNodeStatsEstimate mapSymbolColumnStatistics(Symbol symbol, Function<SymbolStatsEstimate, SymbolStatsEstimate> mappingFunction)
    {
        return buildFrom(this)
                .addSymbolStatistics(symbol, mappingFunction.apply(getSymbolStatistics(symbol)))
                .build();
    }

    public SymbolStatsEstimate getSymbolStatistics(Symbol symbol)
    {
        return symbolStatistics.getOrDefault(symbol, SymbolStatsEstimate.unknown());
    }

    @JsonProperty
    public Map<Symbol, SymbolStatsEstimate> getSymbolStatistics()
    {
        return symbolStatistics;
    }

    public Set<Symbol> getSymbolsWithKnownStatistics()
    {
        return symbolStatistics.keySet();
    }

    public boolean isOutputRowCountUnknown()
    {
        return isNaN(outputRowCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputRowCount", outputRowCount)
                .add("symbolStatistics", symbolStatistics)
                .toString();
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
        PlanNodeStatsEstimate that = (PlanNodeStatsEstimate) o;
        return Double.compare(outputRowCount, that.outputRowCount) == 0 &&
                Objects.equals(symbolStatistics, that.symbolStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount, symbolStatistics);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return new Builder(other.getOutputRowCount(), other.symbolStatistics);
    }

    public static final class Builder
    {
        private double outputRowCount;
        // Shared with the source estimate until the first mutation, then a private copy
        private Map<Symbol, SymbolStatsEstimate> symbolStatistics;
        private boolean ownStatistics;

        public Builder()
        {
            this(NaN, ImmutableMap.of());
        }

        private Builder(double outputRowCount, Map<Symbol, SymbolStatsEstimate> symbolStatistics)
        {
            this.outputRowCount = outputRowCount;
            this.symbolStatistics = symbolStatistics;
            this.ownStatistics = false;
        }

        public Builder setOutputRowCount(double outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder addSymbolStatistics(Symbol symbol, SymbolStatsEstimate statistics)
        {
            mutableStatistics().put(symbol, statistics);
            return this;
        }

        public Builder addSymbolStatistics(Map<Symbol, SymbolStatsEstimate> symbolStatistics)
        {
            mutableStatistics().putAll(symbolStatistics);
            return this;
        }

        public Builder removeSymbolStatistics(Symbol symbol)
        {
            mutableStatistics().remove(symbol);
            return this;
        }

        private Map<Symbol, SymbolStatsEstimate> mutableStatistics()
        {
            if (!ownStatistics) {
                symbolStatistics = new HashMap<>(symbolStatistics);
                ownStatistics = true;
            }
            return symbolStatistics;
        }

        public PlanNodeStatsEstimate build()
        {
            return new PlanNodeStatsEstimate(outputRowCount, symbolStatistics);
        }
    }
}
