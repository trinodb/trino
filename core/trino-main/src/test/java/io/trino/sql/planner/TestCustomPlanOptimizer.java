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

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.trino.CustomOptimizerConfig;
import io.trino.FeaturesConfig;
import io.trino.eventlistener.EventListenerConfig;
import io.trino.execution.TaskManager;
import io.trino.execution.TaskManagerConfig;
import io.trino.server.CustomServerMainModule;
import io.trino.server.ServerMainModule;
import io.trino.server.security.ServerSecurityModule;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.sql.planner.assertions.PlanMatchPattern.*;
import static java.lang.Integer.parseInt;

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
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new CustomServerMainModule("testing"));

        Bootstrap app = new Bootstrap(modules.build());

        app.initialize();

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
