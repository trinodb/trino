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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.preferStreamingOperators;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.FIXED;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.MULTIPLE;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static java.util.Objects.requireNonNull;

public class StreamPreferredProperties
{
    private final Optional<StreamDistribution> distribution;

    private final boolean exactColumnOrder;
    private final Optional<List<Symbol>> partitioningColumns; // if missing => any partitioning scheme is acceptable

    private final boolean orderSensitive;

    private StreamPreferredProperties(Optional<StreamDistribution> distribution, Optional<? extends Iterable<Symbol>> partitioningColumns, boolean orderSensitive)
    {
        this(distribution, false, partitioningColumns, orderSensitive);
    }

    private StreamPreferredProperties(
            Optional<StreamDistribution> distribution,
            boolean exactColumnOrder,
            Optional<? extends Iterable<Symbol>> partitioningColumns,
            boolean orderSensitive)
    {
        this.distribution = requireNonNull(distribution, "distribution is null");
        this.partitioningColumns = partitioningColumns.map(ImmutableList::copyOf);
        this.exactColumnOrder = exactColumnOrder;
        this.orderSensitive = orderSensitive;

        checkArgument(!orderSensitive || partitioningColumns.isEmpty(), "An order sensitive context cannot prefer partitioning");
    }

    public static StreamPreferredProperties any()
    {
        return new StreamPreferredProperties(Optional.empty(), Optional.empty(), false);
    }

    public static StreamPreferredProperties singleStream()
    {
        return new StreamPreferredProperties(Optional.of(SINGLE), Optional.empty(), false);
    }

    public static StreamPreferredProperties fixedParallelism()
    {
        return new StreamPreferredProperties(Optional.of(FIXED), Optional.empty(), false);
    }

    public static StreamPreferredProperties defaultParallelism(Session session)
    {
        if (getTaskConcurrency(session) > 1 && !preferStreamingOperators(session)) {
            return new StreamPreferredProperties(Optional.of(MULTIPLE), Optional.empty(), false);
        }
        return any();
    }

    public StreamPreferredProperties withParallelism()
    {
        // do not override an existing parallel preference
        if (isParallelPreferred()) {
            return this;
        }
        return new StreamPreferredProperties(Optional.of(MULTIPLE), Optional.empty(), orderSensitive);
    }

    public StreamPreferredProperties withFixedParallelism()
    {
        if (distribution.isPresent() && distribution.get() == FIXED) {
            return this;
        }
        return fixedParallelism();
    }

    public static StreamPreferredProperties partitionedOn(Collection<Symbol> partitionSymbols)
    {
        if (partitionSymbols.isEmpty()) {
            return singleStream();
        }

        // Prefer partitioning on given partitioning symbols. Partition hash can be evaluated in any order.
        return new StreamPreferredProperties(Optional.of(FIXED), false, Optional.of(ImmutableSet.copyOf(partitionSymbols)), false);
    }

    public static StreamPreferredProperties exactlyPartitionedOn(Collection<Symbol> partitionSymbols)
    {
        if (partitionSymbols.isEmpty()) {
            return singleStream();
        }

        // this must be the exact partitioning symbols, in the exact order
        return new StreamPreferredProperties(Optional.of(FIXED), true, Optional.of(ImmutableList.copyOf(partitionSymbols)), false);
    }

    public StreamPreferredProperties withoutPreference()
    {
        return new StreamPreferredProperties(Optional.empty(), Optional.empty(), orderSensitive);
    }

    public StreamPreferredProperties withPartitioning(Collection<Symbol> partitionSymbols)
    {
        if (partitionSymbols.isEmpty()) {
            return singleStream();
        }

        Iterable<Symbol> desiredPartitioning = partitionSymbols;
        if (partitioningColumns.isPresent()) {
            if (exactColumnOrder) {
                if (partitioningColumns.get().equals(desiredPartitioning)) {
                    return this;
                }
            }
            else {
                // If there are common columns between our requirements and the desired partitionSymbols, both can be satisfied in one shot
                Set<Symbol> common = Sets.intersection(ImmutableSet.copyOf(desiredPartitioning), ImmutableSet.copyOf(partitioningColumns.get()));

                // If we find common partitioning columns, use them, else use child's partitioning columns
                if (!common.isEmpty()) {
                    desiredPartitioning = common;
                }
            }
        }

        return new StreamPreferredProperties(distribution, Optional.of(desiredPartitioning), false);
    }

    public StreamPreferredProperties withDefaultParallelism(Session session)
    {
        if (getTaskConcurrency(session) > 1 && !preferStreamingOperators(session)) {
            return withParallelism();
        }
        return this;
    }

    public boolean isSatisfiedBy(StreamProperties actualProperties)
    {
        // is there a specific preference
        if (distribution.isEmpty() && partitioningColumns.isEmpty()) {
            return true;
        }

        if (isOrderSensitive() && actualProperties.isOrdered()) {
            // ordered is required to be a single stream, so in this ordered case SINGLE is
            // considered satisfactory for MULTIPLE and FIXED
            return true;
        }

        if (distribution.isPresent()) {
            StreamDistribution actualDistribution = actualProperties.getDistribution();
            if (distribution.get() == SINGLE && actualDistribution != SINGLE) {
                return false;
            }
            if (distribution.get() == FIXED && actualDistribution != FIXED) {
                return false;
            }
            if (distribution.get() == MULTIPLE && actualDistribution != FIXED && actualDistribution != MULTIPLE) {
                return false;
            }
        }
        else if (actualProperties.getDistribution() == SINGLE) {
            // when there is no explicit distribution preference, SINGLE satisfies everything
            return true;
        }

        // is there a preference for a specific partitioning scheme?
        if (partitioningColumns.isPresent()) {
            if (exactColumnOrder) {
                return actualProperties.isExactlyPartitionedOn(partitioningColumns.get());
            }
            return actualProperties.isPartitionedOn(partitioningColumns.get());
        }

        return true;
    }

    public boolean isSingleStreamPreferred()
    {
        return distribution.isPresent() && distribution.get() == SINGLE;
    }

    public boolean isParallelPreferred()
    {
        return distribution.isPresent() && distribution.get() != SINGLE;
    }

    public Optional<List<Symbol>> getPartitioningColumns()
    {
        return partitioningColumns;
    }

    public boolean isOrderSensitive()
    {
        return orderSensitive;
    }

    public StreamPreferredProperties translate(Function<Symbol, Optional<Symbol>> translator)
    {
        return new StreamPreferredProperties(
                distribution,
                partitioningColumns.flatMap(partitioning -> translateSymbols(partitioning, translator)),
                orderSensitive);
    }

    private static Optional<List<Symbol>> translateSymbols(Iterable<Symbol> partitioning, Function<Symbol, Optional<Symbol>> translator)
    {
        ImmutableList.Builder<Symbol> newPartitioningColumns = ImmutableList.builder();
        for (Symbol partitioningColumn : partitioning) {
            Optional<Symbol> translated = translator.apply(partitioningColumn);
            if (translated.isEmpty()) {
                return Optional.empty();
            }
            newPartitioningColumns.add(translated.get());
        }
        return Optional.of(newPartitioningColumns.build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("distribution", distribution.orElse(null))
                .add("partitioningColumns", partitioningColumns.orElse(null))
                .omitNullValues()
                .toString();
    }

    public StreamPreferredProperties withOrderSensitivity()
    {
        return new StreamPreferredProperties(distribution, false, Optional.empty(), true);
    }

    public StreamPreferredProperties constrainTo(Iterable<Symbol> symbols)
    {
        if (partitioningColumns.isEmpty()) {
            return this;
        }

        Set<Symbol> availableSymbols = ImmutableSet.copyOf(symbols);
        if (exactColumnOrder) {
            if (availableSymbols.containsAll(partitioningColumns.get())) {
                return this;
            }
            return any();
        }

        List<Symbol> common = partitioningColumns.get().stream()
                .filter(availableSymbols::contains)
                .collect(toImmutableList());
        if (common.isEmpty()) {
            return any();
        }
        return new StreamPreferredProperties(distribution, Optional.of(common), false);
    }
}
