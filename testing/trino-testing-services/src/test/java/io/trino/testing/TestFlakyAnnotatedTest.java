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
package io.trino.testing;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static io.trino.testing.FlakyTestRetryExtension.ALLOWED_RETRIES_COUNT;
import static io.trino.testing.FlakyTestRetryExtension.CONTINUOUS_INTEGRATION_ENVIRONMENT;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@EnabledIfEnvironmentVariable(named = CONTINUOUS_INTEGRATION_ENVIRONMENT, matches = ".*")
@TestInstance(PER_CLASS)
public class TestFlakyAnnotatedTest
{
    private int barCount;

    @FlakyTest(issue = "https://github.com/org/repo/issues/1", match = "Irrelevant in this test")
    public void testFoo()
    {
        // success on first run
    }

    @FlakyTest(issue = "https://github.com/org/repo/issues/1", match = "Intentional flaky fail")
    public void testBar()
    {
        if (barCount++ < ALLOWED_RETRIES_COUNT) {
            fail("Intentional flaky fail on execution #" + barCount);
        }
    }
}
