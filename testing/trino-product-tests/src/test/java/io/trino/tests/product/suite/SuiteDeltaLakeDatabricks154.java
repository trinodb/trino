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
import io.trino.tests.product.deltalake.DeltaLakeDatabricks154Environment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

public final class SuiteDeltaLakeDatabricks154
{
    // Parity note: legacy launcher keeps a dedicated 154 lane filtered by DELTA_LAKE_DATABRICKS_154.
    // There are currently no tests tagged for that group, so this lane intentionally discovers 0 tests.
    private SuiteDeltaLakeDatabricks154() {}

    public static void main(String[] args)
    {
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();
        results.add(SuiteRunner.forEnvironment(DeltaLakeDatabricks154Environment.class)
                .includeTag(TestGroup.ConfiguredFeatures.class)
                .includeTag(TestGroup.DeltaLakeDatabricks154.class)
                .run());

        SuiteRunner.printSummary(results);
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
