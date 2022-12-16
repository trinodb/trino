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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ConstantProperty;
import io.trino.spi.connector.LocalProperty;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.tree.Expression;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.util.MoreLists.filteredCopy;
import static java.util.Objects.requireNonNull;

public class ActualProperties
{
    // Description of the partitioning of the data across nodes
    // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
    // the rows will be partitioned into a single node or stream. However, this can still be a partitioned plan in that the plan
    // will be executed on multiple servers, but only one server will get all the data.
    private final Optional<Partitioning> nodePartitioning; // if missing => partitioned with some unknown scheme
    private final List<LocalProperty<Symbol>> localProperties;
    private final Map<Symbol, NullableValue> constants;

    private ActualProperties(
            Optional<Partitioning> nodePartitioning,
            List<? extends LocalProperty<Symbol>> localProperties,
            Map<Symbol, NullableValue> constants)
    {
        requireNonNull(nodePartitioning, "nodePartitioning is null");
        requireNonNull(localProperties, "localProperties is null");
        requireNonNull(constants, "constants is null");

        this.nodePartitioning = nodePartitioning;

        // The constants field implies a ConstantProperty in localProperties (but not vice versa).
        // Let's make sure to include the constants into the local constant properties.
        Set<Symbol> localConstants = LocalProperties.extractLeadingConstants(localProperties);
        localProperties = LocalProperties.stripLeadingConstants(localProperties);

        Set<Symbol> updatedLocalConstants = ImmutableSet.<Symbol>builder()
                .addAll(localConstants)
                .addAll(constants.keySet())
                .build();

        List<LocalProperty<Symbol>> updatedLocalProperties = LocalProperties.normalizeAndPrune(ImmutableList.<LocalProperty<Symbol>>builder()
                .addAll(updatedLocalConstants.stream().map(ConstantProperty::new).iterator())
                .addAll(localProperties)
                .build());

        this.localProperties = ImmutableList.copyOf(updatedLocalProperties);
        this.constants = ImmutableMap.copyOf(constants);
    }

    public boolean isCoordinatorOnly()
    {
        return nodePartitioning
                .map(Partitioning::getHandle)
                .map(PartitioningHandle::isCoordinatorOnly)
                .orElse(false);
    }

    /**
     * @return true if the plan will only execute on a single node
     */
    public boolean isSingleNode()
    {
        return nodePartitioning
                .map(Partitioning::getHandle)
                .map(PartitioningHandle::isSingleNode)
                .orElse(false);
    }

    public boolean isNullsAndAnyReplicated()
    {
        return nodePartitioning
                .map(Partitioning::isNullsAndAnyReplicated)
                .orElse(false);
    }

    public boolean isNodePartitionedOn(Collection<Symbol> columns, boolean exactly)
    {
        return isNodePartitionedOn(columns, false, exactly);
    }

    public boolean isNodePartitionedOn(Collection<Symbol> columns, boolean nullsAndAnyReplicated, boolean exactly)
    {
        if (nodePartitioning.isEmpty()) {
            return false;
        }

        if (exactly) {
            return nodePartitioning.get().isPartitionedOnExactly(columns, constants.keySet(), nullsAndAnyReplicated);
        }
        return nodePartitioning.get().isPartitionedOn(columns, constants.keySet(), nullsAndAnyReplicated);
    }

    public boolean isCompatibleTablePartitioningWith(Partitioning partitioning, Metadata metadata, Session session)
    {
        return nodePartitioning.isPresent() && nodePartitioning.get().isCompatibleWith(partitioning, metadata, session);
    }

    public boolean isCompatibleTablePartitioningWith(ActualProperties other, Function<Symbol, Set<Symbol>> symbolMappings, Metadata metadata, Session session)
    {
        return nodePartitioning.isPresent() &&
                other.nodePartitioning.isPresent() &&
                nodePartitioning.get().isCompatibleWith(
                        other.nodePartitioning.get(),
                        symbolMappings,
                        symbol -> Optional.ofNullable(constants.get(symbol)),
                        symbol -> Optional.ofNullable(other.constants.get(symbol)),
                        metadata,
                        session);
    }

    public boolean isEffectivelySinglePartition()
    {
        return nodePartitioning.isPresent() && nodePartitioning.get().isEffectivelySinglePartition(constants.keySet());
    }

    public ActualProperties translate(Function<Symbol, Optional<Symbol>> translator)
    {
        return builder()
                .nodePartitioning(nodePartitioning.flatMap(partitioning -> partitioning.translate(new Partitioning.Translator(translator, symbol -> Optional.ofNullable(constants.get(symbol)), expression -> Optional.empty()))))
                .local(LocalProperties.translate(localProperties, translator))
                .constants(translateConstants(translator))
                .build();
    }

    public ActualProperties translate(
            Function<Symbol, Optional<Symbol>> translator,
            Function<Expression, Optional<Symbol>> expressionTranslator)
    {
        return builder()
                .nodePartitioning(nodePartitioning.flatMap(partitioning -> partitioning.translate(new Partitioning.Translator(translator, symbol -> Optional.ofNullable(constants.get(symbol)), expressionTranslator))))
                .local(LocalProperties.translate(localProperties, translator))
                .constants(translateConstants(translator))
                .build();
    }

    public Optional<Partitioning> getNodePartitioning()
    {
        return nodePartitioning;
    }

    public Map<Symbol, NullableValue> getConstants()
    {
        return constants;
    }

    public List<LocalProperty<Symbol>> getLocalProperties()
    {
        return localProperties;
    }

    public ActualProperties withNullsAndAnyReplicated()
    {
        return builderFrom(this)
                .nodePartitioning(nodePartitioning.map(Partitioning::withNullsAndAnyReplicated))
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builderFrom(ActualProperties properties)
    {
        return new Builder(properties.nodePartitioning, properties.localProperties, properties.constants);
    }

    private Map<Symbol, NullableValue> translateConstants(Function<Symbol, Optional<Symbol>> translator)
    {
        Map<Symbol, NullableValue> translatedConstants = new HashMap<>();
        for (Map.Entry<Symbol, NullableValue> entry : constants.entrySet()) {
            Optional<Symbol> translatedKey = translator.apply(entry.getKey());
            translatedKey.ifPresent(symbol -> translatedConstants.put(symbol, entry.getValue()));
        }
        return translatedConstants;
    }

    public static class Builder
    {
        private Optional<Partitioning> nodePartitioning;
        private List<LocalProperty<Symbol>> localProperties;
        private Map<Symbol, NullableValue> constants;
        private boolean unordered;

        public Builder()
        {
            this(Optional.empty(), ImmutableList.of(), ImmutableMap.of());
        }

        public Builder(Optional<Partitioning> nodePartitioning, List<LocalProperty<Symbol>> localProperties, Map<Symbol, NullableValue> constants)
        {
            this.nodePartitioning = requireNonNull(nodePartitioning, "nodePartitioning is null");
            this.localProperties = ImmutableList.copyOf(localProperties);
            this.constants = ImmutableMap.copyOf(constants);
        }

        public Builder nodePartitioning(Partitioning nodePartitioning)
        {
            this.nodePartitioning = Optional.of(requireNonNull(nodePartitioning, "nodePartitioning is null"));
            return this;
        }

        public Builder nodePartitioning(Optional<Partitioning> nodePartitioning)
        {
            this.nodePartitioning = requireNonNull(nodePartitioning, "nodePartitioning is null");
            return this;
        }

        public Builder local(List<? extends LocalProperty<Symbol>> localProperties)
        {
            this.localProperties = ImmutableList.copyOf(localProperties);
            return this;
        }

        public Builder constants(Map<Symbol, NullableValue> constants)
        {
            this.constants = ImmutableMap.copyOf(constants);
            return this;
        }

        public Builder unordered(boolean unordered)
        {
            this.unordered = unordered;
            return this;
        }

        public ActualProperties build()
        {
            List<LocalProperty<Symbol>> localProperties = this.localProperties;
            if (unordered) {
                localProperties = filteredCopy(this.localProperties, property -> !property.isOrderSensitive());
            }
            return new ActualProperties(nodePartitioning, localProperties, constants);
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodePartitioning, localProperties, constants.keySet());
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
        ActualProperties other = (ActualProperties) obj;
        return Objects.equals(this.nodePartitioning, other.nodePartitioning)
                && Objects.equals(this.localProperties, other.localProperties)
                && Objects.equals(this.constants.keySet(), other.constants.keySet());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("nodePartitioning", nodePartitioning)
                .add("localProperties", localProperties)
                .add("constants", constants)
                .toString();
    }
}
