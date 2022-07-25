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
package io.trino.sql.planner.sanity;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NoDuplicatePlanNodeIdsChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(
            PlanNode planNode,
            Session session,
            PlannerContext plannerContext,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        Map<PlanNodeId, PlanNode> planNodeIds = new HashMap<>();
        searchFrom(planNode)
                .findAll()
                .forEach(node -> planNodeIds.merge(node.getId(), node, this::reportDuplicateId));
    }

    private PlanNode reportDuplicateId(PlanNode first, PlanNode second)
    {
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        checkArgument(first.getId().equals(second.getId()));

        throw new IllegalStateException(format(
                "Generated plan contains nodes with duplicated id %s: %s and %s",
                first.getId(),
                first,
                second));
    }
}
