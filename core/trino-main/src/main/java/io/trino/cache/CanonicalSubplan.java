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
package io.trino.cache;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getLast;
import static io.trino.cache.CanonicalSubplanExtractor.columnIdToSymbol;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a canonical subplan. Canonical subplan nodes can either be
 * leaf nodes ({@link CanonicalSubplan#tableScan} is present) or intermediate nodes
 * ({@link CanonicalSubplan#childSubplan} is present).
 * Canonical subplan nodes map to a plan subgraph for which a common subplan can be extracted.
 * For instance, a canonical subplan can be extracted for "scan -> filter -> project" chain of operators
 * or for "aggregation" operator.
 * Canonical symbol names are derived from {@link CacheColumnId} (if symbol represents
 * {@link ColumnHandle}). {@link CacheColumnId} for complex projections will use canonicalized and formatted
 * version of projection expression.
 */
public class CanonicalSubplan
{
    /**
     * Keychain of a {@link CanonicalSubplan} tree. Plans that can be adapted
     * to produce the same results will have the same keychain.
     */
    private final List<Key> keyChain;
    /**
     * {@link PlanNodeId} of original table scan that can be used to identify subquery.
     */
    private final PlanNodeId tableScanId;
    /**
     * Reference to {@link PlanNode} from which {@link CanonicalSubplan} was derived.
     */
    private final PlanNode originalPlanNode;
    /**
     * Mapping from {@link CacheColumnId} to original {@link Symbol} for entire subplan
     * (including child subplans).
     */
    private final BiMap<CacheColumnId, Symbol> originalSymbolMapping;

    /**
     * Group by columns that are part of {@link CanonicalSubplan#assignments}.
     */
    private final Optional<Set<CacheColumnId>> groupByColumns;
    /**
     * Output projections with iteration order matching list of output columns.
     * Symbol names are canonicalized as {@link CacheColumnId}.
     */
    private final Map<CacheColumnId, Expression> assignments;
    /**
     * Filtering conjuncts. Symbol names are canonicalized as {@link CacheColumnId}.
     */
    private final List<Expression> conjuncts;
    /**
     * Set of conjuncts that can be pulled up though intermediate nodes as a top level filter node.
     */
    private final Set<Expression> pullableConjuncts;
    /**
     * List of dynamic filters.
     */
    private final List<Expression> dynamicConjuncts;
    /**
     * If present then this {@link CanonicalSubplan} was created on top of table scan.
     */
    private final Optional<TableScan> tableScan;
    /**
     * If present then this {@link CanonicalSubplan} has another {@link CanonicalSubplan} as it's direct child.
     */
    private final Optional<CanonicalSubplan> childSubplan;

    private CanonicalSubplan(
            Key key,
            PlanNodeId tableScanId,
            PlanNode originalPlanNode,
            BiMap<CacheColumnId, Symbol> originalSymbolMapping,
            Optional<Set<CacheColumnId>> groupByColumns,
            Map<CacheColumnId, Expression> assignments,
            List<Expression> conjuncts,
            Set<Expression> pullableConjuncts,
            List<Expression> dynamicConjuncts,
            Optional<TableScan> tableScan,
            Optional<CanonicalSubplan> childSubplan)
    {
        this.tableScanId = requireNonNull(tableScanId, "tableScanId is null");
        this.originalPlanNode = requireNonNull(originalPlanNode, "originalPlanNode is null");
        this.originalSymbolMapping = ImmutableBiMap.copyOf(requireNonNull(originalSymbolMapping, "originalSymbolMapping is null"));
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns is null").map(ImmutableSet::copyOf);
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        this.conjuncts = ImmutableList.copyOf(requireNonNull(conjuncts, "conjuncts is null"));
        this.pullableConjuncts = ImmutableSet.copyOf(requireNonNull(pullableConjuncts, "pullableConjuncts is null"));
        this.dynamicConjuncts = ImmutableList.copyOf(requireNonNull(dynamicConjuncts, "dynamicConjuncts is null"));
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.childSubplan = requireNonNull(childSubplan, "childSubplan is null");
        checkArgument(tableScan.isPresent() != childSubplan.isPresent(), "Source must be either table scan or child subplan");
        keyChain = ImmutableList.<Key>builder()
                .addAll(childSubplan
                        .map(CanonicalSubplan::getKeyChain)
                        .orElse(ImmutableList.of()))
                .add(requireNonNull(key, "key is null"))
                .build();
    }

    public SymbolMapper canonicalSymbolMapper()
    {
        return new SymbolMapper(symbol -> {
            CacheColumnId columnId = originalSymbolMapping.inverse().get(symbol);
            requireNonNull(columnId, format("No column id for symbol %s", symbol));
            return columnIdToSymbol(columnId);
        });
    }

    public Key getKey()
    {
        return getLast(keyChain);
    }

    public List<Key> getKeyChain()
    {
        return keyChain;
    }

    public PlanNodeId getTableScanId()
    {
        return tableScanId;
    }

    public PlanNode getOriginalPlanNode()
    {
        return originalPlanNode;
    }

    public BiMap<CacheColumnId, Symbol> getOriginalSymbolMapping()
    {
        return originalSymbolMapping;
    }

    public Optional<Set<CacheColumnId>> getGroupByColumns()
    {
        return groupByColumns;
    }

    public Map<CacheColumnId, Expression> getAssignments()
    {
        return assignments;
    }

    public List<Expression> getConjuncts()
    {
        return conjuncts;
    }

    public Set<Expression> getPullableConjuncts()
    {
        return pullableConjuncts;
    }

    public List<Expression> getDynamicConjuncts()
    {
        return dynamicConjuncts;
    }

    public Optional<TableScan> getTableScan()
    {
        return tableScan;
    }

    public Optional<CanonicalSubplan> getChildSubplan()
    {
        return childSubplan;
    }

    public static class TableScan
    {
        /**
         * Mapping from {@link CacheColumnId} to {@link ColumnHandle}.
         */
        private final Map<CacheColumnId, ColumnHandle> columnHandles;
        /**
         * Original table handle.
         */
        private final TableHandle table;
        /**
         * {@link CacheTableId} of scanned table.
         */
        private final CacheTableId tableId;
        /**
         * Whether to use connector provided node partitioning for table scan.
         */
        private final boolean useConnectorNodePartitioning;

        public TableScan(
                Map<CacheColumnId, ColumnHandle> columnHandles,
                TableHandle table,
                CacheTableId tableId,
                boolean useConnectorNodePartitioning)
        {
            this.columnHandles = ImmutableMap.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
            this.table = requireNonNull(table, "table is null");
            this.tableId = requireNonNull(tableId, "tableId is null");
            this.useConnectorNodePartitioning = useConnectorNodePartitioning;
        }

        public Map<CacheColumnId, ColumnHandle> getColumnHandles()
        {
            return columnHandles;
        }

        public TableHandle getTable()
        {
            return table;
        }

        public CacheTableId getTableId()
        {
            return tableId;
        }

        public boolean isUseConnectorNodePartitioning()
        {
            return useConnectorNodePartitioning;
        }
    }

    public static CanonicalSubplanBuilder builderForTableScan(
            Key key,
            Map<CacheColumnId, ColumnHandle> columnHandles,
            TableHandle table,
            CacheTableId tableId,
            boolean useConnectorNodePartitioning,
            PlanNodeId tableScanId)
    {
        return new CanonicalSubplanBuilder(
                key,
                tableScanId,
                Optional.of(new TableScan(columnHandles, table, tableId, useConnectorNodePartitioning)),
                Optional.empty());
    }

    public static CanonicalSubplanBuilder builderForChildSubplan(Key key, CanonicalSubplan childSubplan)
    {
        requireNonNull(childSubplan, "childSubplan is null");
        return new CanonicalSubplanBuilder(
                key,
                childSubplan.getTableScanId(),
                Optional.empty(),
                Optional.of(childSubplan));
    }

    public static CanonicalSubplanBuilder builderExtending(CanonicalSubplan subplan)
    {
        requireNonNull(subplan, "subplan is null");
        return new CanonicalSubplanBuilder(subplan.getKey(), subplan.getTableScanId(), subplan.getTableScan(), subplan.getChildSubplan())
                .conjuncts(subplan.getConjuncts())
                .dynamicConjuncts(subplan.getDynamicConjuncts());
    }

    public static class CanonicalSubplanBuilder
    {
        private final Key key;
        private final PlanNodeId tableScanId;
        private final Optional<TableScan> tableScan;
        private final Optional<CanonicalSubplan> childSubplan;
        private PlanNode originalPlanNode;
        private BiMap<CacheColumnId, Symbol> originalSymbolMapping;
        private Optional<Set<CacheColumnId>> groupByColumns = Optional.empty();
        private Map<CacheColumnId, Expression> assignments;
        private List<Expression> conjuncts = ImmutableList.of();
        private Set<Expression> pullableConjuncts;
        private List<Expression> dynamicConjuncts = ImmutableList.of();

        private CanonicalSubplanBuilder(Key key, PlanNodeId tableScanId, Optional<TableScan> tableScan, Optional<CanonicalSubplan> childSubplan)
        {
            this.key = requireNonNull(key, "key is null");
            this.tableScanId = requireNonNull(tableScanId, "tableScanId is null");
            this.tableScan = requireNonNull(tableScan, "tableScan is null");
            this.childSubplan = requireNonNull(childSubplan, "childSubplan is null");
        }

        @CanIgnoreReturnValue
        public CanonicalSubplanBuilder originalPlanNode(PlanNode originalPlanNode)
        {
            this.originalPlanNode = requireNonNull(originalPlanNode, "originalPlanNode is null");
            return this;
        }

        @CanIgnoreReturnValue
        public CanonicalSubplanBuilder originalSymbolMapping(BiMap<CacheColumnId, Symbol> originalSymbolMapping)
        {
            this.originalSymbolMapping = requireNonNull(originalSymbolMapping, "originalSymbolMapping is null");
            return this;
        }

        @CanIgnoreReturnValue
        public CanonicalSubplanBuilder groupByColumns(Set<CacheColumnId> groupByColumns)
        {
            this.groupByColumns = Optional.of(requireNonNull(groupByColumns, "groupByColumns is null"));
            return this;
        }

        @CanIgnoreReturnValue
        public CanonicalSubplanBuilder assignments(Map<CacheColumnId, Expression> assignments)
        {
            this.assignments = requireNonNull(assignments, "assignments is null");
            return this;
        }

        @CanIgnoreReturnValue
        public CanonicalSubplanBuilder conjuncts(List<Expression> conjuncts)
        {
            this.conjuncts = requireNonNull(conjuncts, "conjuncts is null");
            return this;
        }

        @CanIgnoreReturnValue
        public CanonicalSubplanBuilder pullableConjuncts(Set<Expression> pullableConjuncts)
        {
            this.pullableConjuncts = requireNonNull(pullableConjuncts, "pullableConjuncts is null");
            return this;
        }

        @CanIgnoreReturnValue
        public CanonicalSubplanBuilder dynamicConjuncts(List<Expression> dynamicConjuncts)
        {
            this.dynamicConjuncts = requireNonNull(dynamicConjuncts, "dynamicConjuncts is null");
            return this;
        }

        public CanonicalSubplan build()
        {
            return new CanonicalSubplan(
                    key,
                    tableScanId,
                    originalPlanNode,
                    originalSymbolMapping,
                    groupByColumns,
                    assignments,
                    conjuncts,
                    pullableConjuncts,
                    dynamicConjuncts,
                    tableScan,
                    childSubplan);
        }
    }

    public record ScanFilterProjectKey(CacheTableId tableId)
            implements CanonicalSubplan.Key {}

    public record FilterProjectKey()
            implements CanonicalSubplan.Key {}

    public record AggregationKey(Set<CacheColumnId> groupByColumns, Set<Expression> nonPullableConjuncts)
            implements CanonicalSubplan.Key {}

    public record TopNKey(List<CacheColumnId> orderBy, Map<CacheColumnId, SortOrder> orderings, long count, Set<Expression> nonPullableConjuncts)
            implements CanonicalSubplan.Key {}

    public record TopNRankingKey(
            List<CacheColumnId> partitionBy,
            List<CacheColumnId> orderBy,
            Map<CacheColumnId, SortOrder> orderings,
            RankingType rankingType,
            int maxRankingPerPartition,
            Set<Expression> nonPullableConjuncts)
            implements CanonicalSubplan.Key {}
    public interface Key {}
}
