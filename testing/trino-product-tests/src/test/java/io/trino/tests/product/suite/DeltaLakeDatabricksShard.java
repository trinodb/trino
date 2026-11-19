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

import io.trino.tests.product.suite.SuiteRunner.TestRun;

import static org.junit.platform.engine.discovery.ClassNameFilter.excludeClassNamePatterns;
import static org.junit.platform.engine.discovery.ClassNameFilter.includeClassNamePatterns;

final class DeltaLakeDatabricksShard
{
    private static final String DELTA_LAKE_TEST_CLASS = "io[.]trino[.]tests[.]product[.]deltalake[.]";

    // Shard 2 runs the remaining classes, including any new Databricks tests.
    private static final String[] FIRST_SHARD = {
            DELTA_LAKE_TEST_CLASS + "TestDeltaLakeCheckpointsDatabricksCompatibility",
            DELTA_LAKE_TEST_CLASS + "TestDeltaLakeCloneTableCompatibilityDatabricks",
            DELTA_LAKE_TEST_CLASS + "TestDeltaLakeCreateTableAsSelectCompatibilityDatabricks",
            DELTA_LAKE_TEST_CLASS + "TestDeltaLakeDatabricksCreateTableCompatibility",
            DELTA_LAKE_TEST_CLASS + "TestDeltaLakeJmxDatabricks",
            DELTA_LAKE_TEST_CLASS + "TestDeltaLakeUpdateCompatibility"};

    private DeltaLakeDatabricksShard() {}

    public static TestRun selectShard(TestRun run)
    {
        String shard = System.getProperty("trino.product-tests.databricks-shard", "");
        return switch (shard) {
            case "" -> run;
            case "1" -> run.filterClassNames(includeClassNamePatterns(FIRST_SHARD));
            case "2" -> run.filterClassNames(excludeClassNamePatterns(FIRST_SHARD));
            default -> throw new IllegalArgumentException("Unknown Databricks test shard: " + shard);
        };
    }
}
