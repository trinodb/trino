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
package io.trino.tests.product.launcher.suite.suites;

import com.google.common.collect.ImmutableList;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeDeltaLakeDatabricks104;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeDeltaLakeDatabricks73;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeDeltaLakeDatabricks91;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteDeltaLakeDatabricks
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        String[] excludedTests = {
                // AWS Glue does not support table comments
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testDeltaToHiveCommentTable",
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testHiveToDeltaCommentTable",
                "io.trino.tests.product.deltalake.TestDeltaLakeAlterTableCompatibility.testCommentOnTableUnsupportedWriterVersion",
                // AWS Glue does not support column comments
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testDeltaToHiveCommentColumn",
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testHiveToDeltaCommentColumn",
                "io.trino.tests.product.deltalake.TestDeltaLakeAlterTableCompatibility.testCommentOnColumnUnsupportedWriterVersion",
                // AWS Glue does not support table renames
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testDeltaToHiveAlterTable",
                "io.trino.tests.product.deltalake.TestHiveAndDeltaLakeRedirect.testHiveToDeltaAlterTable",
                // TODO https://github.com/trinodb/trino/issues/13017
                "io.trino.tests.product.deltalake.TestDeltaLakeDropTableCompatibility.testCreateManagedTableInDeltaDropTableInTrino"
        };
        return ImmutableList.of(
                testOnEnvironment(EnvSinglenodeDeltaLakeDatabricks73.class)
                        .withGroups("configured_features", "delta-lake-databricks")
                        .withExcludedGroups("delta-lake-exclude-73")
                        .withExcludedTests(excludedTests)
                        .build(),

                testOnEnvironment(EnvSinglenodeDeltaLakeDatabricks91.class)
                        .withGroups("configured_features", "delta-lake-databricks")
                        .withExcludedGroups("delta-lake-exclude-91")
                        .withExcludedTests(excludedTests)
                        .build(),

                testOnEnvironment(EnvSinglenodeDeltaLakeDatabricks104.class)
                        .withGroups("configured_features", "delta-lake-databricks")
                        .withExcludedTests(excludedTests)
                        .build());
    }
}
