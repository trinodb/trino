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

import io.trino.Session;
import io.trino.cost.TableStatsProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.TrinoException;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.tree.Node;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;

public class CheckSubqueryNodesAreRewritten
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, TableStatsProvider tableStatsProvider)
    {
        searchFrom(plan).where(ApplyNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    ApplyNode applyNode = (ApplyNode) node;
                    throw error(applyNode.getCorrelation(), applyNode.getOriginSubquery());
                });

        searchFrom(plan).where(CorrelatedJoinNode.class::isInstance)
                .findFirst()
                .ifPresent(node -> {
                    CorrelatedJoinNode correlatedJoinNode = (CorrelatedJoinNode) node;
                    throw error(correlatedJoinNode.getCorrelation(), correlatedJoinNode.getOriginSubquery());
                });

        return plan;
    }

    private TrinoException error(List<Symbol> correlation, Node originSubquery)
    {
        checkState(!correlation.isEmpty(), "All the non correlated subqueries should be rewritten at this point");
        throw semanticException(NOT_SUPPORTED, originSubquery, "Given correlated subquery is not supported");
    }
}
