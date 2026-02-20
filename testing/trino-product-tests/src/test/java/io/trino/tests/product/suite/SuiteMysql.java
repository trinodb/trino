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
import io.trino.tests.product.mariadb.MariaDbEnvironment;
import io.trino.tests.product.mysql.MySqlEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 5 test suite for MySQL and MariaDB connector tests.
 * <p>
 * This suite runs two sequential test runs:
 * <ol>
 *   <li>MySQL tests: All tests tagged with {@code @TestGroup.Mysql} using {@link MySqlEnvironment}</li>
 *   <li>MariaDB tests: All tests tagged with {@code @TestGroup.Mariadb} using {@link MariaDbEnvironment}</li>
 * </ol>
 * <p>
 * The environments are run sequentially with explicit shutdown between runs to prevent
 * OOM conditions on CI runners.
 * <p>
 * To run this suite:
 * <pre>
 * java -cp ... io.trino.tests.product.suite.SuiteMysql
 * </pre>
 * <p>
 * Or via Maven:
 * <pre>
 * mvn exec:java -Dexec.mainClass="io.trino.tests.product.suite.SuiteMysql"
 * </pre>
 */
public final class SuiteMysql
{
    private SuiteMysql() {}

    public static void main(String[] args)
    {
        // Set strict mode to ensure tests are properly isolated
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        // Run 1: MySQL tests
        results.add(SuiteRunner.forEnvironment(MySqlEnvironment.class)
                .includeTag(TestGroup.Mysql.class)
                .run());

        // Run 2: MariaDB tests
        results.add(SuiteRunner.forEnvironment(MariaDbEnvironment.class)
                .includeTag(TestGroup.Mariadb.class)
                .run());

        // Print combined summary
        SuiteRunner.printSummary(results);

        // Exit with appropriate code
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
