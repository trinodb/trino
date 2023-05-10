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
import io.trino.metadata.TableHandle;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.SymbolMapper;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.cache.CanonicalSubplanExtractor.columnIdToSymbol;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a scan, filter, project and aggregation subplan with canonicalized symbol names.
 * Canonical symbol names are derived from {@link CacheColumnId} (if symbol represents
 * {@link ColumnHandle}) or from canonical version of project or aggregation expressions.
 */
public class CanonicalSubplan
{
    /**
     * Reference to {@link PlanNode} from which {@link CanonicalSubplan} was derived.
     */
    private final PlanNode originalPlanNode;
    /**
     * Mapping from {@link CacheColumnId} to original {@link Symbol}.
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
     * List of dynamic filters.
     */
    private final List<Expression> dynamicConjuncts;
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
    /**
     * {@link PlanNodeId} of original table scan that can be used to identify subquery.
     */
    private final PlanNodeId tableScanId;

    public CanonicalSubplan(
            PlanNode originalPlanNode,
            BiMap<CacheColumnId, Symbol> originalSymbolMapping,
            Optional<Set<CacheColumnId>> groupByColumns,
            Map<CacheColumnId, Expression> assignments,
            List<Expression> conjuncts,
            List<Expression> dynamicConjuncts,
            Map<CacheColumnId, ColumnHandle> columnHandles,
            TableHandle table,
            CacheTableId tableId,
            boolean useConnectorNodePartitioning,
            PlanNodeId tableScanId)
    {
        this.originalPlanNode = requireNonNull(originalPlanNode, "originalPlanNode is null");
        this.originalSymbolMapping = ImmutableBiMap.copyOf(requireNonNull(originalSymbolMapping, "originalSymbolMapping is null"));
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns is null").map(ImmutableSet::copyOf);
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
        this.conjuncts = ImmutableList.copyOf(requireNonNull(conjuncts, "conjuncts is null"));
        this.dynamicConjuncts = ImmutableList.copyOf(requireNonNull(dynamicConjuncts, "dynamicConjuncts is null"));
        this.columnHandles = ImmutableMap.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.table = requireNonNull(table, "table is null");
        this.tableId = requireNonNull(tableId, "tableId is null");
        this.useConnectorNodePartitioning = useConnectorNodePartitioning;
        this.tableScanId = requireNonNull(tableScanId, "tableScanId is null");
    }

    public SymbolMapper canonicalSymbolMapper()
    {
        return new SymbolMapper(symbol -> {
            CacheColumnId columnId = originalSymbolMapping.inverse().get(symbol);
            requireNonNull(columnId, format("No column id for symbol %s", symbol));
            return columnIdToSymbol(columnId);
        });
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

    public List<Expression> getDynamicConjuncts()
    {
        return dynamicConjuncts;
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

    public PlanNodeId getTableScanId()
    {
        return tableScanId;
    }
}
