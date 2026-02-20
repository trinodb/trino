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
import io.trino.tests.product.postgresql.PostgresqlBasicEnvironment;
import io.trino.tests.product.postgresql.PostgresqlSpoolingEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 5 test suite for PostgreSQL connector tests.
 * <p>
 * This suite runs two sequential test runs:
 * <ol>
 *   <li>PostgreSQL basic tests: All tests tagged with {@code postgresql} but not {@code postgresql_spooling}</li>
 *   <li>PostgreSQL spooling tests: All tests tagged with {@code postgresql_spooling}</li>
 * </ol>
 * <p>
 * To run this suite:
 * <pre>
 * mvn exec:java -Dexec.mainClass="io.trino.tests.product.suite.SuitePostgresql"
 * </pre>
 */
public final class SuitePostgresql
{
    private SuitePostgresql() {}

    public static void main(String[] args)
    {
        // Set strict mode to ensure tests are properly isolated
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        // Run 1: PostgreSQL basic tests (excluding spooling tests)
        results.add(SuiteRunner.forEnvironment(PostgresqlBasicEnvironment.class)
                .includeTag(TestGroup.Postgresql.class)
                .excludeTag(TestGroup.PostgresqlSpooling.class)
                .run());

        // Run 2: PostgreSQL spooling tests
        results.add(SuiteRunner.forEnvironment(PostgresqlSpoolingEnvironment.class)
                .includeTag(TestGroup.PostgresqlSpooling.class)
                .run());

        // Print combined summary
        SuiteRunner.printSummary(results);

        // Exit with appropriate code
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
