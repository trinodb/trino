package io.trino;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;

import javax.annotation.Nullable;

@DefunctConfig({

})
public class CustomOptimizerConfig
{
    @VisibleForTesting
    private String additionalPlanOptimizerClasses;
    private boolean allowCustomPlanOptimizers;

    @Nullable
    public String getAdditionalPlanOptimizerClasses()
    {
        return additionalPlanOptimizerClasses;
    }

    @ConfigDescription("List of extra CustomPlanOptimizer classes")
    @Config("optimizer.custom-optimizer.list")
    public CustomOptimizerConfig setAdditionalPlanOptimizerClasses(String classNames)
    {
        this.additionalPlanOptimizerClasses = classNames;
        return this;
    }
    public boolean isAllowCustomPlanOptimizers()
    {
        return allowCustomPlanOptimizers;
    }

    @ConfigDescription("Whether custom plan optimizer support is enabled")
    @Config("optimizer.custom-optimizer.allow")
    public CustomOptimizerConfig setAllowCustomPlanOptimizers(boolean allowCustomPlanOptimizers)
    {
        this.allowCustomPlanOptimizers = allowCustomPlanOptimizers;
        return this;
    }
}
