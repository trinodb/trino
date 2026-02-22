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
import io.trino.tests.product.azure.AzureEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;

public final class SuiteAzure
{
    private SuiteAzure() {}

    public static void main(String[] args)
    {
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        results.add(SuiteRunner.forEnvironment(AzureEnvironment.class)
                .includeTag(TestGroup.ConfiguredFeatures.class)
                .includeTag(TestGroup.Azure.class)
                .run());

        results.add(SuiteRunner.forEnvironment(AzureEnvironment.class)
                .includeTag(TestGroup.ConfiguredFeatures.class)
                .includeTag(TestGroup.DeltaLakeAzure.class)
                .run());

        results.add(SuiteRunner.forEnvironment(AzureEnvironment.class)
                .includeTag(TestGroup.ConfiguredFeatures.class)
                .includeTag(TestGroup.IcebergAzure.class)
                .run());

        SuiteRunner.printSummary(results);
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }
}
