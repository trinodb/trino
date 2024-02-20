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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public abstract class BaseRuleTest
{
    private RuleTester tester;
    private final List<Plugin> plugins;

    public BaseRuleTest(Plugin... plugins)
    {
        this.plugins = ImmutableList.copyOf(plugins);
    }

    @BeforeAll
    public final void setUp()
    {
        Optional<PlanTester> planTester = createPlanTester();

        if (planTester.isPresent()) {
            plugins.forEach(plugin -> planTester.get().installPlugin(plugin));
            tester = new RuleTester(planTester.get());
        }
        else {
            tester = RuleTester.builder()
                    .addPlugins(plugins)
                    .build();
        }
    }

    protected Optional<PlanTester> createPlanTester()
    {
        return Optional.empty();
    }

    @AfterAll
    public final void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    protected RuleTester tester()
    {
        return tester;
    }
}
