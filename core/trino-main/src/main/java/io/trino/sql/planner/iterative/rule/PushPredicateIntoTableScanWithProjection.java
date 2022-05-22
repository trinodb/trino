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
package io.trino.sql.planner.iterative.rule;

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.tableScan;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PushPredicateIntoTableScanWithProjection
        extends PushPredicateIntoTableScan<ProjectNode>
{
    private static final Capture<FilterNode> FILTER = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(
                    filter().capturedAs(FILTER)
                            .with(source().matching(
                                    tableScan().capturedAs(TABLE_SCAN)))));

    public PushPredicateIntoTableScanWithProjection(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer)
    {
        super(plannerContext, typeAnalyzer);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        FilterNode filterNode = captures.get(FILTER);
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        Optional<PlanNode> rewritten = pushFilterIntoTableScan(
                Optional.of(projectNode),
                filterNode,
                tableScan,
                false,
                context.getSession(),
                context.getSymbolAllocator(),
                plannerContext,
                typeAnalyzer,
                context.getStatsProvider(),
                new DomainTranslator(plannerContext));

        if (rewritten.isEmpty() || rewritten.get().getSources().size() != 1 || arePlansSame(filterNode, tableScan, rewritten.get().getSources().get(0))) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewritten.get());
    }
}
