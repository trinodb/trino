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
package io.trino.sql.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.planner.optimizations.PartitioningArgument;

import javax.annotation.concurrent.Immutable;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public final class Partitioning
{
    private final PartitioningHandle handle;
    private final List<PartitioningArgument> arguments;
    // Description of whether rows with nulls in partitioning columns or some arbitrary rows have been replicated to all *nodes*
    private final boolean nullsAndAnyReplicated;

    private Partitioning(PartitioningHandle handle, List<PartitioningArgument> arguments, boolean nullsAndAnyReplicated)
    {
        this.handle = requireNonNull(handle, "handle is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        this.nullsAndAnyReplicated = nullsAndAnyReplicated;
    }

    public static Partitioning create(PartitioningHandle handle, List<Symbol> columns)
    {
        return create(handle, columns, false);
    }

    public static Partitioning create(PartitioningHandle handle, List<Symbol> columns, boolean nullsAndAnyReplicated)
    {
        return new Partitioning(
                handle,
                columns.stream()
                        .map(Symbol::toSymbolReference)
                        .map(PartitioningArgument::expressionArgument)
                        .collect(toImmutableList()),
                nullsAndAnyReplicated);
    }

    // Factory method for JSON serde only!
    @JsonCreator
    public static Partitioning jsonCreate(
            @JsonProperty("handle") PartitioningHandle handle,
            @JsonProperty("arguments") List<PartitioningArgument> arguments,
            @JsonProperty("nullsAndAnyReplicated") boolean nullsAndAnyReplicated)
    {
        return new Partitioning(handle, arguments, nullsAndAnyReplicated);
    }

    @JsonProperty
    public PartitioningHandle getHandle()
    {
        return handle;
    }

    @JsonProperty
    public List<PartitioningArgument> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public boolean isNullsAndAnyReplicated()
    {
        return nullsAndAnyReplicated;
    }

    public Set<Symbol> getColumns()
    {
        return arguments.stream()
                .filter(PartitioningArgument::isVariable)
                .map(PartitioningArgument::getColumn)
                .collect(toImmutableSet());
    }

    public boolean isCompatibleWith(
            Partitioning right,
            Metadata metadata,
            Session session)
    {
        if (nullsAndAnyReplicated != right.nullsAndAnyReplicated) {
            return false;
        }

        if (!handle.equals(right.handle) && metadata.getCommonPartitioning(session, handle, right.handle).isEmpty()) {
            return false;
        }

        return arguments.equals(right.arguments);
    }

    public boolean isCompatibleWith(
            Partitioning right,
            Function<Symbol, Set<Symbol>> leftToRightMappings,
            Function<Symbol, Optional<NullableValue>> leftConstantMapping,
            Function<Symbol, Optional<NullableValue>> rightConstantMapping,
            Metadata metadata,
            Session session)
    {
        if (nullsAndAnyReplicated != right.nullsAndAnyReplicated) {
            return false;
        }

        if (!handle.equals(right.handle) && metadata.getCommonPartitioning(session, handle, right.handle).isEmpty()) {
            return false;
        }

        if (arguments.size() != right.arguments.size()) {
            return false;
        }

        for (int i = 0; i < arguments.size(); i++) {
            PartitioningArgument leftArgument = arguments.get(i);
            PartitioningArgument rightArgument = right.arguments.get(i);

            if (!isPartitionedWith(leftArgument, leftConstantMapping, rightArgument, rightConstantMapping, leftToRightMappings)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPartitionedWith(
            PartitioningArgument leftArgument,
            Function<Symbol, Optional<NullableValue>> leftConstantMapping,
            PartitioningArgument rightArgument,
            Function<Symbol, Optional<NullableValue>> rightConstantMapping,
            Function<Symbol, Set<Symbol>> leftToRightMappings)
    {
        if (leftArgument.isVariable()) {
            if (rightArgument.isVariable()) {
                // variable == variable
                Set<Symbol> mappedColumns = leftToRightMappings.apply(leftArgument.getColumn());
                return mappedColumns.contains(rightArgument.getColumn());
            }
            // variable == constant
            // Normally, this would be a false condition, but if we happen to have an external
            // mapping from the symbol to a constant value and that constant value matches the
            // right value, then we are co-partitioned.
            Optional<NullableValue> leftConstant = leftConstantMapping.apply(leftArgument.getColumn());
            return leftConstant.isPresent() && leftConstant.get().equals(rightArgument.getConstant());
        }
        if (rightArgument.isConstant()) {
            // constant == constant
            return leftArgument.getConstant().equals(rightArgument.getConstant());
        }
        // constant == variable
        Optional<NullableValue> rightConstant = rightConstantMapping.apply(rightArgument.getColumn());
        return rightConstant.isPresent() && rightConstant.get().equals(leftArgument.getConstant());
    }

    public boolean isPartitionedOn(Collection<Symbol> columns, Set<Symbol> knownConstants, boolean nullsAndAnyReplicated)
    {
        if (this.nullsAndAnyReplicated != nullsAndAnyReplicated) {
            return false;
        }
        for (PartitioningArgument argument : arguments) {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            if (argument.isConstant()) {
                continue;
            }
            if (!argument.isVariable()) {
                return false;
            }
            if (!knownConstants.contains(argument.getColumn()) && !columns.contains(argument.getColumn())) {
                return false;
            }
        }
        return true;
    }

    public boolean isPartitionedOnExactly(Collection<Symbol> columns, Set<Symbol> knownConstants, boolean nullsAndAnyReplicated)
    {
        if (this.nullsAndAnyReplicated != nullsAndAnyReplicated) {
            return false;
        }
        Set<Symbol> toCheck = new HashSet<>();
        for (PartitioningArgument argument : arguments) {
            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            if (argument.isConstant()) {
                continue;
            }
            if (!argument.isVariable()) {
                return false;
            }
            if (knownConstants.contains(argument.getColumn())) {
                continue;
            }
            toCheck.add(argument.getColumn());
        }
        return ImmutableSet.copyOf(columns).equals(toCheck);
    }

    public boolean isEffectivelySinglePartition(Set<Symbol> knownConstants)
    {
        return isPartitionedOn(ImmutableSet.of(), knownConstants, false);
    }

    public Partitioning translate(Function<Symbol, Symbol> translator)
    {
        return new Partitioning(
                handle,
                arguments.stream()
                        .map(argument -> argument.translate(translator))
                        .collect(toImmutableList()),
                nullsAndAnyReplicated);
    }

    public Optional<Partitioning> translate(PartitioningArgument.Translator translator)
    {
        ImmutableList.Builder<PartitioningArgument> newArguments = ImmutableList.builder();
        for (PartitioningArgument argument : arguments) {
            Optional<PartitioningArgument> newArgument = argument.translate(translator);
            if (newArgument.isEmpty()) {
                return Optional.empty();
            }
            newArguments.add(newArgument.get());
        }

        return Optional.of(new Partitioning(handle, newArguments.build(), nullsAndAnyReplicated));
    }

    public Partitioning withAlternativePartitioningHandle(PartitioningHandle partitioningHandle)
    {
        return new Partitioning(partitioningHandle, arguments, nullsAndAnyReplicated);
    }

    public Partitioning withNullsAndAnyReplicated()
    {
        return new Partitioning(handle, arguments, true);
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
        Partitioning other = (Partitioning) obj;
        return Objects.equals(this.handle, other.handle) &&
                Objects.equals(this.arguments, other.arguments) &&
                this.nullsAndAnyReplicated == other.nullsAndAnyReplicated;
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
