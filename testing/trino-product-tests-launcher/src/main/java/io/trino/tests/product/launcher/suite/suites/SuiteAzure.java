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
import io.trino.tests.product.launcher.env.environment.EnvMultinodeAzure;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;

import static io.trino.tests.product.TestGroups.AZURE;
import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_AZURE;
import static io.trino.tests.product.TestGroups.ICEBERG_AZURE;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;

public class SuiteAzure
        extends Suite
{
    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        return ImmutableList.of(
                testOnEnvironment(EnvMultinodeAzure.class)
                        .withGroups(CONFIGURED_FEATURES, AZURE)
                        .build(),
                testOnEnvironment(EnvMultinodeAzure.class)
                        .withGroups(DELTA_LAKE_AZURE, CONFIGURED_FEATURES)
                        .build(),
                testOnEnvironment(EnvMultinodeAzure.class)
                        .withGroups(ICEBERG_AZURE, CONFIGURED_FEATURES)
                        .build());
    }
}
