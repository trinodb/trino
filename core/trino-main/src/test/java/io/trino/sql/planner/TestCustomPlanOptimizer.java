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
package io.trino.sql.planner;

import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static io.trino.sql.planner.assertions.PlanMatchPattern.*;

public class TestCustomPlanOptimizer extends BasePlanTest {

    @Test
    public void testSampleCustomPlanOptimizer() throws IOException {
            List<String> customOptimizerProperties = Arrays.asList("optimizer.custom-optimizer.allow=true",
                    "optimizer.custom-optimizer.list=io.trino.sql.planner.SampleCustomPlanOptimizer");
            setConfig(customOptimizerProperties);
            PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
            List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);
            //This checks if the SampleCustomPlanOptimizer is not only is on the chain but is also doing what it's designed to do.
            //Which is update the limit count to 7999 (from original 9999)
            assertPlan(
                    "SELECT orderstatus FROM orders limit 9999",

                    anyTree(limit(7999, planMatchPattern)),
                    allOptimizers);
            PlanOptimizer firstOptimizer = allOptimizers.get(0);
            //This asserts is the SampleCustomPlanOptimizer is the first optimizer in the chain.
            assert firstOptimizer instanceof SampleCustomPlanOptimizer;
    }

    @Test
    public void testCustomPlanOptimizerNotAllowed() throws IOException {
        List<String> customOptimizerProperties = Arrays.asList("optimizer.custom-optimizer.allow=false");
        setConfig(customOptimizerProperties);
        PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
        // Limit count will not change as optimizer.custom-optimizer.allow=false
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);
        assertPlan(
                "SELECT orderstatus FROM orders limit 9999",
                anyTree(limit(9999, planMatchPattern)),
                allOptimizers);
    }

    @Test
    public void testCustomPlanOptimizerNotSet() throws IOException {
        List<String> customOptimizerProperties = Arrays.asList("optimizer.custom-optimizer.allow=true");
        setConfig(customOptimizerProperties);
        PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
        // Limit count will not change as no custom optimizer is registered
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);
        assertPlan(
                "SELECT orderstatus FROM orders limit 9999",
                anyTree(limit(9999, planMatchPattern)),
                allOptimizers);
    }

    @Test
    public void testCustomPlanOptimizerAllowNotSet() throws IOException {
        List<String> customOptimizerProperties = Arrays.asList("optimizer.custom-optimizer.list=io.trino.sql.planner.SampleCustomPlanOptimizer");
        setConfig(customOptimizerProperties);
        PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
        // Limit count will not change as optimizer.custom-optimizer.allow is not set
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);
        assertPlan(
                "SELECT orderstatus FROM orders limit 9999",
                anyTree(limit(9999, planMatchPattern)),
                allOptimizers);
    }

    @Test
    public void testCustomPlanOptimizerCustomPropertiesNotSet() throws IOException {
        List<String> customOptimizerProperties = Arrays.asList();
        setConfig(customOptimizerProperties);
        PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
        // Limit count will not change as no custom optimizer properties set
        List<PlanOptimizer> allOptimizers = getQueryRunner().getPlanOptimizers(false);
        assertPlan(
                "SELECT orderstatus FROM orders limit 9999",
                anyTree(limit(9999, planMatchPattern)),
                allOptimizers);
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*ClassNotFoundException.*")
    public void testCustomPlanOptimizerClassNotExist() throws IOException {
        // Throw ClassNotFoundException as the registered custom optimizer class does not exist
        List<String> customOptimizerProperties = Arrays.asList("optimizer.custom-optimizer.allow=true",
                "optimizer.custom-optimizer.list=io.trino.sql.planner.NotExistCustomPlanOptimizer");
        setConfig(customOptimizerProperties);
        PlanMatchPattern planMatchPattern = anyTree(tableScan("orders"));
        getQueryRunner().getPlanOptimizers(false);
    }

    // Creates and sets trino config file with custom optimizer related properties
    private void setConfig(List<String> propertiesList) throws IOException {
        Path customOptimizerConfig = Files.createTempFile("custom-optimizer-config", ".conf");
        customOptimizerConfig.toFile().deleteOnExit();
        Files.write(customOptimizerConfig, propertiesList);
        System.setProperty("config", customOptimizerConfig.toString());
    }
}
