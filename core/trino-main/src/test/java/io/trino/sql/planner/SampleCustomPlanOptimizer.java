package io.trino.sql.planner;

import io.trino.Session;
import io.trino.cost.*;
import io.trino.execution.TaskManagerConfig;
import io.trino.execution.warnings.WarningCollector;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.optimizations.CustomPlanOptimizer;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.*;
import static java.util.Objects.requireNonNull;

public class SampleCustomPlanOptimizer extends CustomPlanOptimizer {
    /*
    This custom plan optimizer will just alter the limit expression from its original value 9999 to a new value of 7999.
    This will help us test the custom plan optimizer injection feature.
     */
    public SampleCustomPlanOptimizer(){

    }
    @Override
    public PlanOptimizer getPlanOptimizerInstance(PlannerContext plannerContext, TypeAnalyzer typeAnalyzer, TaskManagerConfig taskManagerConfig, boolean forceSingleNode, SplitManager splitManager, PageSourceManager pageSourceManager, StatsCalculator statsCalculator, ScalarStatsCalculator scalarStatsCalculator, CostCalculator costCalculator, CostCalculator estimatedExchangesCostCalculator, CostComparator costComparator, TaskCountEstimator taskCountEstimator, NodePartitioningManager nodePartitioningManager, RuleStatsRecorder ruleStats) {
        return new SampleCustomPlanOptimizer();
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector) {
        return SimplePlanRewriter.rewriteWith(new SampleCustomPlanOptimizer.Rewriter(idAllocator), plan);
    }

    private static class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitLimit(LimitNode node, RewriteContext<Void> context) {
            LimitNode changedLimitNode = new LimitNode(
                    idAllocator.getNextId(),
                    node.getSource(),
                    7999L,
                    node.isPartial());
            return changedLimitNode;
        }
    }
}
