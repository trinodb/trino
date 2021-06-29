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
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.PlanNode;

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
                        new AllFunctionsResolved(),
                        new TypeValidator(),
                        new NoSubqueryExpressionLeftChecker(),
                        new NoIdentifierLeftChecker(),
                        new VerifyOnlyOneOutputNode())
                .putAll(
                        Stage.FINAL,
                        new ValidateDependenciesChecker(),
                        new NoDuplicatePlanNodeIdsChecker(),
                        new SugarFreeChecker(),
                        new AllFunctionsResolved(),
                        new TypeValidator(),
                        new NoSubqueryExpressionLeftChecker(),
                        new NoIdentifierLeftChecker(),
                        new VerifyOnlyOneOutputNode(),
                        new VerifyNoFilteredAggregations(),
                        new VerifyUseConnectorNodePartitioningSet(),
                        new ValidateAggregationsWithDefaultValues(forceSingleNode),
                        new ValidateStreamingAggregations(),
                        new ValidateLimitWithPresortedInput(),
                        new DynamicFiltersChecker(),
                        new TableScanValidator())
                .build();
    }

    public void validateFinalPlan(PlanNode planNode,
            Session session,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        checkers.get(Stage.FINAL).forEach(checker -> checker.validate(planNode, session, metadata, typeOperators, typeAnalyzer, types, warningCollector));
    }

    public void validateIntermediatePlan(PlanNode planNode,
            Session session,
            Metadata metadata,
            TypeOperators typeOperators,
            TypeAnalyzer typeAnalyzer,
            TypeProvider types,
            WarningCollector warningCollector)
    {
        checkers.get(Stage.INTERMEDIATE).forEach(checker -> checker.validate(planNode, session, metadata, typeOperators, typeAnalyzer, types, warningCollector));
    }

    public interface Checker
    {
        void validate(PlanNode planNode,
                Session session,
                Metadata metadata,
                TypeOperators typeOperators,
                TypeAnalyzer typeAnalyzer,
                TypeProvider types,
                WarningCollector warningCollector);
    }

    private enum Stage
    {
        INTERMEDIATE, FINAL
    }
}
