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
package io.trino.tests;

import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingSession.testSession;
import static io.trino.testing.ThreadAssertions.assertNoThreadLeakedInPlanTester;

public class TestPlanTester
{
    @Test
    public void testNoThreadLeaked()
            throws Exception
    {
        assertNoThreadLeakedInPlanTester(
                () -> PlanTester.create(testSession()),
                planTester -> planTester.inTransaction(transactionSession -> {
                    planTester.createPlan(transactionSession, "SELECT 1");
                    return null;
                }));
    }
}
