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
package io.trino.sql.planner.iterative.rule;

import io.trino.Session;
import io.trino.metadata.TestingFunctionResolution;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.SystemSessionProperties.FUNCTION_PREIMAGES_ENABLED;
import static io.trino.SystemSessionProperties.isFunctionPreimagesEnabled;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFunctionPreimageSelection
{
    @Test
    public void testExclusiveRuleSelection()
    {
        var plannerContext = new TestingFunctionResolution().getPlannerContext();
        var preimages = new UnwrapFunctionInComparison(plannerContext);
        List<ExpressionRewriteRuleSet> legacy = List.of(
                new UnwrapCastInComparison(plannerContext),
                new UnwrapYearInComparison(plannerContext),
                new UnwrapDateTruncInComparison(plannerContext),
                new UnwrapAtTimeZoneInComparison(plannerContext));
        Session defaultSession = testSessionBuilder().build();
        assertThat(isFunctionPreimagesEnabled(defaultSession)).isTrue();
        for (boolean enabled : List.of(false, true)) {
            Session session = Session.builder(defaultSession)
                    .setSystemProperty(FUNCTION_PREIMAGES_ENABLED, Boolean.toString(enabled))
                    .build();
            assertThat(preimages.rules()).allSatisfy(rule -> assertThat(rule.isEnabled(session)).isEqualTo(enabled));
            for (ExpressionRewriteRuleSet rules : legacy) {
                assertThat(rules.rules()).allSatisfy(rule -> assertThat(rule.isEnabled(session)).isEqualTo(!enabled));
            }
        }
    }
}
