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

import com.google.inject.Binder;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.trino.cost.CostCalculator;
import io.trino.cost.CostComparator;
import io.trino.cost.ScalarStatsCalculator;
import io.trino.cost.StatsCalculator;
import io.trino.cost.TaskCountEstimator;
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

/**
 * This abstract class must be implemented by any custom optimizer,
 * implemented custom optimizer class need to have a default public constructor.
 */
public abstract class CustomPlanOptimizer
        implements PlanOptimizer
{
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
                                                                    RuleStatsRecorder ruleStats)
    {
        List<PlanOptimizer> listOfPlanOptimizers = new ArrayList<>();
        CustomOptimizerConfig customOptimizerConfig = getConfig();
        if (!customOptimizerConfig.isAllowCustomPlanOptimizers()) {
            LOG.debug("Custom optimizers are disabled");
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
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return listOfPlanOptimizers;
    }

    private static CustomOptimizerConfig getConfig()
    {
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
            extends AbstractConfigurationAwareModule
    {
        public ConfigAccessModule()
        {
        }

        @Override
        protected void setup(Binder binder)
        {
            configBinder(binder).bindConfig(CustomOptimizerConfig.class);
        }
    }
}
