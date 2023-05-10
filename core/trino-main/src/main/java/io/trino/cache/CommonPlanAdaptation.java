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

import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;
import io.trino.spi.connector.ColumnHandle;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ChooseAlternativeNode.FilteredTableScan;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class provides a common subplan (shared between different subplans in a query) and a way
 * to adapt it to original plan.
 */
public class CommonPlanAdaptation
{
    /**
     * Common subplan (shared between different subplans in a query)
     */
    private final PlanNode commonSubplan;
    /**
     * Common subplan {@link FilteredTableScan}.
     */
    private final FilteredTableScan commonSubplanFilteredTableScan;
    /**
     * Signature of common subplan.
     */
    private final PlanSignature commonSubplanSignature;
    /**
     * Dynamic filter disjuncts from all common subplans.
     */
    private final Expression commonDynamicFilterDisjuncts;
    /**
     * Mapping from {@link CacheColumnId} to relevant (for plan signature) dynamic filtering columns.
     */
    private final Map<CacheColumnId, ColumnHandle> dynamicFilterColumnMapping;
    /**
     * Optional predicate that needs to be applied in order to adapt common subplan to
     * original plan.
     */
    private final Optional<Expression> adaptationPredicate;
    /**
     * Optional projections that need to applied in order to adapt common subplan
     * to original plan.
     */
    private final Optional<Assignments> adaptationAssignments;

    public CommonPlanAdaptation(
            PlanNode commonSubplan,
            FilteredTableScan commonSubplanFilteredTableScan,
            PlanSignature commonSubplanSignature,
            Expression commonDynamicFilterDisjuncts,
            Map<CacheColumnId, ColumnHandle> dynamicFilterColumnMapping,
            Optional<Expression> adaptationPredicate,
            Optional<Assignments> adaptationAssignments)
    {
        this.commonSubplan = requireNonNull(commonSubplan, "commonSubplan is null");
        this.commonSubplanFilteredTableScan = requireNonNull(commonSubplanFilteredTableScan, "commonSubplanFilteredTableScan is null");
        this.commonSubplanSignature = requireNonNull(commonSubplanSignature, "commonSubplanSignature is null");
        this.commonDynamicFilterDisjuncts = requireNonNull(commonDynamicFilterDisjuncts, "commonDynamicFilterDisjuncts is null");
        this.dynamicFilterColumnMapping = requireNonNull(dynamicFilterColumnMapping, "dynamicFilterColumnMapping is null");
        this.adaptationPredicate = requireNonNull(adaptationPredicate, "adaptationPredicate is null");
        this.adaptationAssignments = requireNonNull(adaptationAssignments, "adaptationAssignments is null");
    }

    public PlanNode adaptCommonSubplan(PlanNode commonSubplan, PlanNodeIdAllocator idAllocator)
    {
        checkArgument(this.commonSubplan.getOutputSymbols().equals(commonSubplan.getOutputSymbols()));
        PlanNode adaptedPlan = commonSubplan;
        if (adaptationPredicate.isPresent()) {
            adaptedPlan = new FilterNode(
                    idAllocator.getNextId(),
                    adaptedPlan,
                    adaptationPredicate.get());
        }
        if (adaptationAssignments.isPresent()) {
            adaptedPlan = new ProjectNode(
                    idAllocator.getNextId(),
                    adaptedPlan,
                    adaptationAssignments.get());
        }
        return adaptedPlan;
    }

    public PlanNode getCommonSubplan()
    {
        return commonSubplan;
    }

    public FilteredTableScan getCommonSubplanFilteredTableScan()
    {
        return commonSubplanFilteredTableScan;
    }

    public PlanSignature getCommonSubplanSignature()
    {
        return commonSubplanSignature;
    }

    public Expression getCommonDynamicFilterDisjuncts()
    {
        return commonDynamicFilterDisjuncts;
    }

    public Map<CacheColumnId, ColumnHandle> getDynamicFilterColumnMapping()
    {
        return dynamicFilterColumnMapping;
    }
}
