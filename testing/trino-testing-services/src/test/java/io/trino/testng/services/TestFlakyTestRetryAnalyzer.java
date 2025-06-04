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
package io.trino.testng.services;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.testng.services.FlakyTestRetryAnalyzer.ALLOWED_RETRIES_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

@Test(singleThreaded = true)
public class TestFlakyTestRetryAnalyzer
        extends TestingOverridesTest
{
    private int testRetryingCount;
    private int testNoRetryingCount;
    private int[] testRetryingParametricTestCount = new int[2];

    @Flaky(issue = "intentionally flaky for @Flaky test purposes", match = "I am trying hard to fail!")
    @Test
    public void testRetrying()
    {
        testRetryingCount++;
        if (testRetryingCount <= ALLOWED_RETRIES_COUNT) {
            fail("I am trying hard to fail!");
        }
        assertThat(testRetryingCount).isEqualTo(3);
    }

    @Test
    public void testNoRetrying()
    {
        testNoRetryingCount++;
        assertThat(testNoRetryingCount).isEqualTo(1);
    }

    @Override
    @Test
    @Flaky(issue = "intentionally flaky for @Flaky test purposes", match = "I am trying hard to fail!")
    public void testRetryingOverriddenTest()
    {
        super.testRetryingOverriddenTest();
    }

    @Flaky(issue = "intentionally flaky for @Flaky test purposes", match = "I am trying hard to fail!")
    @Test(dataProvider = "parameters")
    public void testRetryingParametricTest(int index)
    {
        testRetryingParametricTestCount[index]++;
        if (testRetryingParametricTestCount[index] <= ALLOWED_RETRIES_COUNT) {
            fail("I am trying hard to fail!");
        }
        assertThat(testRetryingParametricTestCount[index]).isEqualTo(3);
    }

    @DataProvider
    public Object[][] parameters()
    {
        return new Object[][] {{0}, {1}};
    }
}
