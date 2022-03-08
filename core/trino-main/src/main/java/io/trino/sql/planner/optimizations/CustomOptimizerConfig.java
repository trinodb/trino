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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CustomOptimizerConfig
{
    @VisibleForTesting
    private String additionalPlanOptimizerClasses;
    private boolean allowCustomPlanOptimizers;

    public CustomOptimizerConfig()
    {
    }

    @Nullable
    public List<String> getAdditionalPlanOptimizerClasses()
    {
        if (additionalPlanOptimizerClasses == null || additionalPlanOptimizerClasses.isEmpty()) {
            return new ArrayList<>();
        }
        List<String> listOfClasses = Arrays.asList(additionalPlanOptimizerClasses);
        listOfClasses.replaceAll(String::trim);
        return listOfClasses;
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
