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

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.plan.PlanNode;

import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;

/**
 * It is going to be executed to verify logical planner correctness
 */
public final class PlanSanityChecker
{
    public static final PlanSanityChecker DISTRIBUTED_PLAN_SANITY_CHECKER = new PlanSanityChecker(false);

    private final Multimap<Stage, Checker> checkers;

    public PlanSanityChecker(boolean forceSingleNode)
    {
        checkers = ImmutableListMultimap.<Stage, Checker>builder()
                .putAll(
                        Stage.INTERMEDIATE,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyOnlyOneOutputNode())
                .putAll(
                        Stage.FINAL,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyOnlyOneOutputNode(),
                        new VerifyNoFilteredAggregations(),
                        new VerifyUseConnectorNodePartitioningSet(),
                        new ValidateAggregationsWithDefaultValues(forceSingleNode),
                        new ValidateScaledWritersUsage(),
                        new ValidateStreamingAggregations(),
                        new DynamicFiltersChecker(),
                        new TableScanValidator(),
                        new TableExecuteStructureValidator())
                .putAll(
                        Stage.AFTER_ADAPTIVE_PLANNING,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new TypeValidator(),
                        new VerifyOnlyOneOutputNode(),
                        new VerifyNoFilteredAggregations(),
                        new VerifyUseConnectorNodePartitioningSet(),
                        new ValidateScaledWritersUsage(),
                        new TableScanValidator(),
                        new TableExecuteStructureValidator())
                .build();
    }

    public void validateFinalPlan(
            PlanNode planNode,
            Session session,
            PlannerContext plannerContext,
            WarningCollector warningCollector)
    {
        validate(Stage.FINAL, planNode, session, plannerContext, warningCollector);
    }

    public void validateIntermediatePlan(
            PlanNode planNode,
            Session session,
            PlannerContext plannerContext,
            WarningCollector warningCollector)
    {
        validate(Stage.INTERMEDIATE, planNode, session, plannerContext, warningCollector);
    }

    public void validateAdaptivePlan(
            PlanNode planNode,
            Session session,
            PlannerContext plannerContext,
            WarningCollector warningCollector)
    {
        validate(Stage.AFTER_ADAPTIVE_PLANNING, planNode, session, plannerContext, warningCollector);
    }

    private void validate(
            Stage stage,
            PlanNode planNode,
            Session session,
            PlannerContext plannerContext,
            WarningCollector warningCollector)
    {
        try {
            checkers.get(stage).forEach(checker -> checker.validate(planNode, session, plannerContext, warningCollector));
        }
        catch (RuntimeException e) {
            try {
                int nestLevel = 4; // so that it renders reasonably within exception stacktrace
                String explain = textLogicalPlan(
                        planNode,
                        plannerContext.getMetadata(),
                        plannerContext.getFunctionManager(),
                        StatsAndCosts.empty(),
                        session,
                        nestLevel,
                        false);
                e.addSuppressed(new Exception("Current plan:\n" + explain));
            }
            catch (RuntimeException ignore) {
                // ignored
            }
            throw e;
        }
    }

    public interface Checker
    {
        void validate(
                PlanNode planNode,
                Session session,
                PlannerContext plannerContext,
                WarningCollector warningCollector);
    }

    private enum Stage
    {
        INTERMEDIATE, FINAL, AFTER_ADAPTIVE_PLANNING
    }
}
