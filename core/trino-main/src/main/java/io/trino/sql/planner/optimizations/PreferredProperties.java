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
import io.trino.spi.connector.LocalProperty;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

class PreferredProperties
{
    private final Optional<PartitioningProperties> nodePartitioning;
    private final List<LocalProperty<Symbol>> localProperties;

    private PreferredProperties(
            Optional<PartitioningProperties> nodePartitioning,
            List<? extends LocalProperty<Symbol>> localProperties)
    {
        requireNonNull(nodePartitioning, "nodePartitioning is null");
        requireNonNull(localProperties, "localProperties is null");

        this.nodePartitioning = nodePartitioning;
        this.localProperties = ImmutableList.copyOf(localProperties);
    }

    public static PreferredProperties any()
    {
        return builder().build();
    }

    public static PreferredProperties undistributed()
    {
        return builder()
                .nodePartitioning(PartitioningProperties.singlePartition())
                .build();
    }

    public static PreferredProperties partitioned(Set<Symbol> columns)
    {
        return builder()
                .nodePartitioning(PartitioningProperties.partitioned(columns))
                .build();
    }

    public static PreferredProperties partitionedWithNullsAndAnyReplicated(Set<Symbol> columns)
    {
        return builder()
                .nodePartitioning(PartitioningProperties.partitioned(columns, true))
                .build();
    }

    public static PreferredProperties partitioned(Partitioning partitioning)
    {
        return builder()
                .nodePartitioning(PartitioningProperties.partitioned(partitioning))
                .build();
    }

    public static PreferredProperties partitionedWithLocal(Set<Symbol> columns, List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .nodePartitioning(PartitioningProperties.partitioned(columns))
                .localProperties(localProperties)
                .build();
    }

    public static PreferredProperties local(List<? extends LocalProperty<Symbol>> localProperties)
    {
        return builder()
                .localProperties(localProperties)
                .build();
    }

    public Optional<PartitioningProperties> getNodePartitioning()
    {
        return nodePartitioning;
    }

    public List<LocalProperty<Symbol>> getLocalProperties()
    {
        return localProperties;
    }

    public PreferredProperties mergeWithParent(PreferredProperties parent)
    {
        List<LocalProperty<Symbol>> newLocal = ImmutableList.<LocalProperty<Symbol>>builder()
                .addAll(localProperties)
                .addAll(parent.getLocalProperties())
                .build();

        Builder builder = builder()
                .localProperties(newLocal);

        if (nodePartitioning.isPresent()) {
            PartitioningProperties current = nodePartitioning.get();
            PartitioningProperties merged = parent.getNodePartitioning()
                    .map(current::mergeWithParent)
                    .orElse(current);
            builder.nodePartitioning(merged);
        }
        else {
            parent.getNodePartitioning().ifPresent(builder::nodePartitioning);
        }

        return builder.build();
    }

    public PreferredProperties translate(Function<Symbol, Optional<Symbol>> translator)
    {
        Optional<PartitioningProperties> newNodePartitioning = nodePartitioning.flatMap(partitioning -> partitioning.translate(translator));
        List<LocalProperty<Symbol>> newLocalProperties = LocalProperties.translate(localProperties, translator);
        return new PreferredProperties(newNodePartitioning, newLocalProperties);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Optional<PartitioningProperties> nodePartitioning = Optional.empty();
        private List<LocalProperty<Symbol>> localProperties = ImmutableList.of();

        public Builder nodePartitioning(PartitioningProperties nodePartitioning)
        {
            this.nodePartitioning = Optional.of(nodePartitioning);
            return this;
        }

        public Builder localProperties(List<? extends LocalProperty<Symbol>> localProperties)
        {
            this.localProperties = ImmutableList.copyOf(localProperties);
            return this;
        }

        public PreferredProperties build()
        {
            return new PreferredProperties(nodePartitioning, localProperties);
        }
    }

    @Immutable
    public static final class PartitioningProperties
    {
        private final Optional<PartitioningHandle> handle;
        private final List<PartitioningArgument> arguments;
        private final boolean nullsAndAnyReplicated;

        private PartitioningProperties(Optional<PartitioningHandle> handle, List<PartitioningArgument> arguments, boolean nullsAndAnyReplicated)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
            this.nullsAndAnyReplicated = nullsAndAnyReplicated;
        }

        public static PartitioningProperties partitioned(Partitioning partitioning)
        {
            return new PartitioningProperties(Optional.of(partitioning.getHandle()), partitioning.getArguments(), partitioning.isNullsAndAnyReplicated());
        }

        public static PartitioningProperties partitioned(Set<Symbol> columns)
        {
            return partitioned(columns, false);
        }

        public static PartitioningProperties partitioned(Set<Symbol> columns, boolean nullsAndAnyReplicated)
        {
            return new PartitioningProperties(
                    Optional.empty(),
                    columns.stream()
                            .map(Symbol::toSymbolReference)
                            .map(PartitioningArgument::expressionArgument)
                            .collect(toImmutableList()),
                    nullsAndAnyReplicated);
        }

        public static PartitioningProperties singlePartition()
        {
            return partitioned(ImmutableSet.of());
        }

        public Set<Symbol> getPartitioningColumns()
        {
            return arguments.stream()
                    .filter(PartitioningArgument::isVariable)
                    .map(PartitioningArgument::getColumn)
                    .collect(toImmutableSet());
        }

        public Optional<PartitioningHandle> getHandle()
        {
            return handle;
        }

        public List<PartitioningArgument> getArguments()
        {
            return arguments;
        }

        public boolean isNullsAndAnyReplicated()
        {
            return nullsAndAnyReplicated;
        }

        public boolean isDistributed()
        {
            return arguments.stream().anyMatch(PartitioningArgument::isVariable);
        }

        public PartitioningProperties mergeWithParent(PartitioningProperties parent)
        {
            // Non-negotiable if we require a specific partitioning
            if (handle.isPresent()) {
                return this;
            }

            // Partitioning with different replication cannot be compared
            if (nullsAndAnyReplicated != parent.nullsAndAnyReplicated) {
                return this;
            }

            if (parent.getHandle().isPresent()) {
                // If the parent has a partitioning preference, propagate parent only if the parent's partitioning columns satisfies our preference.
                // Otherwise, ignore the parent since the parent will have to repartition anyways.
                return getPartitioningColumns().containsAll(parent.getPartitioningColumns()) ? parent : this;
            }

            // Otherwise partition on any common columns if available
            Set<Symbol> common = Sets.intersection(getPartitioningColumns(), parent.getPartitioningColumns());
            return common.isEmpty() ? this : partitioned(common, nullsAndAnyReplicated);
        }

        public Optional<PartitioningProperties> translate(Function<Symbol, Optional<Symbol>> translator)
        {
            PartitioningArgument.Translator argumentTranslator = new PartitioningArgument.Translator(translator, symbol -> Optional.empty(), coalesceSymbols -> Optional.empty());

            List<PartitioningArgument> translatedArguments = arguments.stream()
                    .map(argument -> argument.translate(argumentTranslator))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());

            // Translation fails if we have prior partitioning columns and none could be translated
            if (!arguments.isEmpty() && translatedArguments.isEmpty()) {
                return Optional.empty();
            }

            if (handle.isPresent() && translatedArguments.size() != arguments.size()) {
                // specific partitioning is requested, arguments cannot be lost
                return Optional.empty();
            }

            return Optional.of(new PartitioningProperties(handle, translatedArguments, nullsAndAnyReplicated));
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(handle, arguments, nullsAndAnyReplicated);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            PartitioningProperties other = (PartitioningProperties) obj;
            return Objects.equals(this.handle, other.handle)
                    && Objects.equals(this.arguments, other.arguments)
                    && this.nullsAndAnyReplicated == other.nullsAndAnyReplicated;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("handle", handle)
                    .add("arguments", arguments)
                    .add("nullsAndAnyReplicated", nullsAndAnyReplicated)
                    .toString();
        }
    }
}
