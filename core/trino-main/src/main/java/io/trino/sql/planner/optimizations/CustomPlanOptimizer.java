package io.trino.sql.planner.optimizations;

import com.google.inject.Binder;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.cost.*;
import io.trino.execution.TaskManagerConfig;
import io.trino.split.PageSourceManager;
import io.trino.split.SplitManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.NodePartitioningManager;
import io.trino.sql.planner.RuleStatsRecorder;
import io.trino.sql.planner.TypeAnalyzer;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;

public abstract class CustomPlanOptimizer implements PlanOptimizer {

    private static final Logger LOG = Logger.get(CustomPlanOptimizer.class);

    /*
    Reads any injected PlanOptimizer class names from the config and returns a list of their instances
     */
    @Inject
    public static final List<PlanOptimizer> getCustomPlanOptimizers(PlannerContext plannerContext,
                                                                    TypeAnalyzer typeAnalyzer,
                                                                    TaskManagerConfig taskManagerConfig,
                                                                    boolean forceSingleNode,
                                                                    SplitManager splitManager,
                                                                    PageSourceManager pageSourceManager,
                                                                    StatsCalculator statsCalculator,
                                                                    ScalarStatsCalculator scalarStatsCalculator,
                                                                    CostCalculator costCalculator,
                                                                    CostCalculator estimatedExchangesCostCalculator,
                                                                    CostComparator costComparator,
                                                                    TaskCountEstimator taskCountEstimator,
                                                                    NodePartitioningManager nodePartitioningManager,
                                                                    RuleStatsRecorder ruleStats
    ) {

        List<PlanOptimizer> listOfPlanOptimizers = new ArrayList<>();
        CustomOptimizerConfig customOptimizerConfig = getConfig();
        if (!customOptimizerConfig.isAllowCustomPlanOptimizers()) {
            LOG.debug("No registered custom optimizers found");
            return listOfPlanOptimizers;
        }
        try {
            List<String> customPlanOptimizerClassNames = customOptimizerConfig.getAdditionalPlanOptimizerClasses();
            for (String customPlanOptimizerClassName : customPlanOptimizerClassNames) {
                LOG.info("Creating custom optimizer instance %s", customPlanOptimizerClassName);
                CustomPlanOptimizer customPlanOptimizer = (CustomPlanOptimizer) Class.forName(customPlanOptimizerClassName).getDeclaredConstructor().newInstance();
                PlanOptimizer planOptimizer = customPlanOptimizer.getPlanOptimizerInstance(plannerContext,
                        typeAnalyzer,
                        taskManagerConfig,
                        forceSingleNode,
                        splitManager,
                        pageSourceManager,
                        statsCalculator,
                        scalarStatsCalculator,
                        costCalculator,
                        estimatedExchangesCostCalculator,
                        costComparator,
                        taskCountEstimator,
                        nodePartitioningManager,
                        ruleStats);
                listOfPlanOptimizers.add(planOptimizer);
            }
        } catch (Exception e) {
            LOG.error(e);
        }
        return listOfPlanOptimizers;
    }

    private static CustomOptimizerConfig getConfig() {
        Bootstrap app = new Bootstrap(new ConfigAccessModule());
        Injector injector = app.initialize();
        CustomOptimizerConfig customOptimizerConfig = injector.getInstance(CustomOptimizerConfig.class);
        return customOptimizerConfig;
    }

    public abstract PlanOptimizer getPlanOptimizerInstance(PlannerContext plannerContext,
                                                           TypeAnalyzer typeAnalyzer,
                                                           TaskManagerConfig taskManagerConfig,
                                                           boolean forceSingleNode,
                                                           SplitManager splitManager,
                                                           PageSourceManager pageSourceManager,
                                                           StatsCalculator statsCalculator,
                                                           ScalarStatsCalculator scalarStatsCalculator,
                                                           CostCalculator costCalculator,
                                                           CostCalculator estimatedExchangesCostCalculator,
                                                           CostComparator costComparator,
                                                           TaskCountEstimator taskCountEstimator,
                                                           NodePartitioningManager nodePartitioningManager,
                                                           RuleStatsRecorder ruleStats);

    private static class ConfigAccessModule
            extends AbstractConfigurationAwareModule {

        public ConfigAccessModule() {
        }

        @Override
        protected void setup(Binder binder) {
            configBinder(binder).bindConfig(CustomOptimizerConfig.class);
        }
    }

}
