package io.trino.sql.planner;

import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.sql.planner.assertions.PlanMatchPattern.*;

public class TestCustomPlanOptimizer extends BasePlanTest {

    @Test
    public void testCustomPlanOptimizer()
    {
        PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);
        //This checks if the SampleCustomPlanOptimizer is not only is on the chain but is also doing what it's designed to do.
        //Which is update the limit count to 7999 (from original 9999)
        assertPlan(
                "SELECT orderstatus FROM orders limit 9999",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(limit(7999,planMatchPattern)),
                allOptimizers);
        PlanOptimizer firstOptimizer = allOptimizers.get(0);
        //This asserts is the SampleCustomPlanOptimizer is the first optimizer in the chain.
        assert firstOptimizer instanceof SampleCustomPlanOptimizer;
    }

    @Test
    public void testCustomPlanOptimizerNotSet()
    {
        PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
        // use all optimizers, including the custom plan optimizer that we have externally injected.
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);
        assertPlan(
                "SELECT orderstatus FROM orders limit 9999",
                // TODO this could be optimized to VALUES with values from partitions
                anyTree(limit(9999,planMatchPattern)),
                allOptimizers);
    }
}
