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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.LanguageFunctionProvider.LanguageFunctionData;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.function.FunctionId;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@Immutable
public class PlanFragment
{
    private final PlanFragmentId id;
    private final PlanNode root;
    private final Set<Symbol> symbols;
    private final PartitioningHandle partitioning;
    private final Optional<Integer> partitionCount;
    private final List<PlanNodeId> partitionedSources;
    private final Set<PlanNodeId> partitionedSourcesSet;
    private final List<Type> types;
    private final Set<PlanNode> partitionedSourceNodes;
    private final List<RemoteSourceNode> remoteSourceNodes;
    private final PartitioningScheme outputPartitioningScheme;
    private final StatsAndCosts statsAndCosts;
    private final List<CatalogProperties> activeCatalogs;
    private final Map<FunctionId, LanguageFunctionData> languageFunctions;
    private final Optional<String> jsonRepresentation;
    private final boolean containsTableScanNode;

    // Only for creating instances without the JSON representation embedded
    private PlanFragment(
            PlanFragmentId id,
            PlanNode root,
            Set<Symbol> symbols,
            PartitioningHandle partitioning,
            Optional<Integer> partitionCount,
            List<PlanNodeId> partitionedSources,
            Set<PlanNodeId> partitionedSourcesSet,
            List<Type> types,
            Set<PlanNode> partitionedSourceNodes,
            List<RemoteSourceNode> remoteSourceNodes,
            PartitioningScheme outputPartitioningScheme,
            StatsAndCosts statsAndCosts,
            List<CatalogProperties> activeCatalogs,
            Map<FunctionId, LanguageFunctionData> languageFunctions)
    {
        this.id = requireNonNull(id, "id is null");
        this.root = requireNonNull(root, "root is null");
        this.symbols = requireNonNull(symbols, "symbols is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.partitionCount = requireNonNull(partitionCount, "partitionCount is null");
        this.partitionedSources = requireNonNull(partitionedSources, "partitionedSources is null");
        this.partitionedSourcesSet = requireNonNull(partitionedSourcesSet, "partitionedSourcesSet is null");
        this.types = requireNonNull(types, "types is null");
        this.partitionedSourceNodes = requireNonNull(partitionedSourceNodes, "partitionedSourceNodes is null");
        this.remoteSourceNodes = requireNonNull(remoteSourceNodes, "remoteSourceNodes is null");
        this.outputPartitioningScheme = requireNonNull(outputPartitioningScheme, "outputPartitioningScheme is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        this.activeCatalogs = requireNonNull(activeCatalogs, "activeCatalogs is null");
        this.languageFunctions = ImmutableMap.copyOf(languageFunctions);
        this.jsonRepresentation = Optional.empty();
        this.containsTableScanNode = partitionedSourceNodes.stream().anyMatch(TableScanNode.class::isInstance);
    }

    @JsonCreator
    public PlanFragment(
            @JsonProperty("id") PlanFragmentId id,
            @JsonProperty("root") PlanNode root,
            @JsonProperty("symbols") Set<Symbol> symbols,
            @JsonProperty("partitioning") PartitioningHandle partitioning,
            @JsonProperty("partitionCount") Optional<Integer> partitionCount,
            @JsonProperty("partitionedSources") List<PlanNodeId> partitionedSources,
            @JsonProperty("outputPartitioningScheme") PartitioningScheme outputPartitioningScheme,
            @JsonProperty("statsAndCosts") StatsAndCosts statsAndCosts,
            @JsonProperty("activeCatalogs") List<CatalogProperties> activeCatalogs,
            @JsonProperty("languageFunctions") Map<FunctionId, LanguageFunctionData> languageFunctions,
            @JsonProperty("jsonRepresentation") Optional<String> jsonRepresentation)
    {
        this.id = requireNonNull(id, "id is null");
        this.root = requireNonNull(root, "root is null");
        this.symbols = requireNonNull(symbols, "symbols is null");
        this.partitioning = requireNonNull(partitioning, "partitioning is null");
        this.partitionCount = requireNonNull(partitionCount, "partitionCount is null");
        this.partitionedSources = ImmutableList.copyOf(requireNonNull(partitionedSources, "partitionedSources is null"));
        this.partitionedSourcesSet = ImmutableSet.copyOf(partitionedSources);
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        this.activeCatalogs = requireNonNull(activeCatalogs, "activeCatalogs is null");
        this.languageFunctions = ImmutableMap.copyOf(languageFunctions);
        this.jsonRepresentation = requireNonNull(jsonRepresentation, "jsonRepresentation is null");

        checkArgument(
                partitionCount.isEmpty() || partitioning.getConnectorHandle() instanceof SystemPartitioningHandle,
                "Connector partitioning handle should be of type system partitioning when partitionCount is present");

        checkArgument(partitionedSourcesSet.size() == partitionedSources.size(), "partitionedSources contains duplicates");
        checkArgument(ImmutableSet.copyOf(root.getOutputSymbols()).containsAll(outputPartitioningScheme.getOutputLayout()),
                "Root node outputs (%s) does not include all fragment outputs (%s)", root.getOutputSymbols(), outputPartitioningScheme.getOutputLayout());

        types = outputPartitioningScheme.getOutputLayout().stream()
                .map(Symbol::type)
                .collect(toImmutableList());

        this.partitionedSourceNodes = findSources(root, partitionedSources);

        ImmutableList.Builder<RemoteSourceNode> remoteSourceNodes = ImmutableList.builder();
        findRemoteSourceNodes(root, remoteSourceNodes);
        this.remoteSourceNodes = remoteSourceNodes.build();

        this.outputPartitioningScheme = requireNonNull(outputPartitioningScheme, "partitioningScheme is null");
        this.containsTableScanNode = partitionedSourceNodes.stream().anyMatch(TableScanNode.class::isInstance);
    }

    @JsonProperty
    public PlanFragmentId getId()
    {
        return id;
    }

    @JsonProperty
    public PlanNode getRoot()
    {
        return root;
    }

    @JsonProperty
    public Set<Symbol> getSymbols()
    {
        return symbols;
    }

    @JsonProperty
    public PartitioningHandle getPartitioning()
    {
        return partitioning;
    }

    @JsonProperty
    public Optional<Integer> getPartitionCount()
    {
        return partitionCount;
    }

    @JsonProperty
    public List<PlanNodeId> getPartitionedSources()
    {
        return partitionedSources;
    }

    public boolean isPartitionedSources(PlanNodeId nodeId)
    {
        return partitionedSourcesSet.contains(nodeId);
    }

    @JsonProperty
    public PartitioningScheme getOutputPartitioningScheme()
    {
        return outputPartitioningScheme;
    }

    @JsonProperty
    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }

    @JsonProperty
    public List<CatalogProperties> getActiveCatalogs()
    {
        return activeCatalogs;
    }

    @JsonProperty
    public Map<FunctionId, LanguageFunctionData> getLanguageFunctions()
    {
        return languageFunctions;
    }

    @JsonProperty
    public Optional<String> getJsonRepresentation()
    {
        // @reviewer: I believe this should be a json raw value, but that would make this class have a different deserialization constructor.
        // workers don't need this, so that should be OK, but it's worth thinking about.
        return jsonRepresentation;
    }

    public PlanFragment withoutEmbeddedJsonRepresentation()
    {
        if (jsonRepresentation.isEmpty()) {
            return this;
        }
        return new PlanFragment(
                this.id,
                this.root,
                this.symbols,
                this.partitioning,
                this.partitionCount,
                this.partitionedSources,
                this.partitionedSourcesSet,
                this.types,
                this.partitionedSourceNodes,
                this.remoteSourceNodes,
                this.outputPartitioningScheme,
                this.statsAndCosts,
                this.activeCatalogs,
                this.languageFunctions);
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Set<PlanNode> getPartitionedSourceNodes()
    {
        return partitionedSourceNodes;
    }

    public boolean isLeaf()
    {
        return remoteSourceNodes.isEmpty();
    }

    public List<RemoteSourceNode> getRemoteSourceNodes()
    {
        return remoteSourceNodes;
    }

    private static Set<PlanNode> findSources(PlanNode node, Iterable<PlanNodeId> nodeIds)
    {
        ImmutableSet.Builder<PlanNode> nodes = ImmutableSet.builder();
        findSources(node, ImmutableSet.copyOf(nodeIds), nodes);
        return nodes.build();
    }

    private static void findSources(PlanNode node, Set<PlanNodeId> nodeIds, ImmutableSet.Builder<PlanNode> nodes)
    {
        if (nodeIds.contains(node.getId())) {
            nodes.add(node);
        }

        for (PlanNode source : node.getSources()) {
            nodes.addAll(findSources(source, nodeIds));
        }
    }

    private static void findRemoteSourceNodes(PlanNode node, ImmutableList.Builder<RemoteSourceNode> builder)
    {
        for (PlanNode source : node.getSources()) {
            findRemoteSourceNodes(source, builder);
        }

        if (node instanceof RemoteSourceNode) {
            builder.add((RemoteSourceNode) node);
        }
    }

    public PlanFragment withBucketToPartition(Optional<int[]> bucketToPartition)
    {
        return new PlanFragment(
                id,
                root,
                symbols,
                partitioning,
                partitionCount,
                partitionedSources,
                outputPartitioningScheme.withBucketToPartition(bucketToPartition),
                statsAndCosts,
                activeCatalogs,
                languageFunctions,
                jsonRepresentation);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("partitioning", partitioning)
                .add("partitionCount", partitionCount)
                .add("partitionedSource", partitionedSources)
                .add("outputPartitioningScheme", outputPartitioningScheme)
                .toString();
    }

    public PlanFragment withActiveCatalogs(List<CatalogProperties> activeCatalogs)
    {
        return new PlanFragment(
                this.id,
                this.root,
                this.symbols,
                this.partitioning,
                this.partitionCount,
                this.partitionedSources,
                this.outputPartitioningScheme,
                this.statsAndCosts,
                activeCatalogs,
                this.languageFunctions,
                this.jsonRepresentation);
    }

    public boolean containsTableScanNode()
    {
        return containsTableScanNode;
    }
}
