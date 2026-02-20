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
package io.trino.tests.product.suite;

import io.trino.tests.product.TestGroup;
import io.trino.tests.product.blackhole.BlackHoleEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 5 test suite for BlackHole connector tests.
 * <p>
 * This suite runs all tests tagged with {@code @TestGroup.Blackhole} using
 * the {@link BlackHoleEnvironment}.
 * <p>
 * To run this suite:
 * <pre>
 * mvn exec:java -Dexec.mainClass="io.trino.tests.product.suite.SuiteBlackHole"
 * </pre>
 */
public final class SuiteBlackHole
{
    private SuiteBlackHole() {}

    public static void main(String[] args)
    {
        // Set strict mode to ensure tests are properly isolated
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        // Run BlackHole tests
        results.add(SuiteRunner.forEnvironment(BlackHoleEnvironment.class)
                .includeTag(TestGroup.Blackhole.class)
                .run());

        // Print combined summary
        SuiteRunner.printSummary(results);

        // Exit with appropriate code
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
