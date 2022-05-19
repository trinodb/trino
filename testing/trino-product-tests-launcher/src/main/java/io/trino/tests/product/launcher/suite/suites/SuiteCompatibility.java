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
import com.google.common.collect.ImmutableMap;
import io.trino.testing.TestingProperties;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentDefaults;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeCompatibility;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class SuiteCompatibility
        extends Suite
{
    private static final String PRESTOSQL_IMAGE = "ghcr.io/trinodb/presto";
    private static final int LAST_PRESTOSQL_VERSION = 350;

    private static final String TRINO_IMAGE = "trinodb/trino";
    private static final int FIRST_TRINO_VERSION = 351;

    private static final int NUMBER_OF_TESTED_VERSIONS = 5;
    private static final int TESTED_VERSIONS_GRANULARITY = 3;

    @Override
    public List<SuiteTestRun> getTestRuns(EnvironmentConfig config)
    {
        verify(config.getHadoopBaseImage().equals(EnvironmentDefaults.HADOOP_BASE_IMAGE), "The suite should be run with default HADOOP_BASE_IMAGE. Leave HADOOP_BASE_IMAGE unset.");

        ImmutableList<SuiteTestRun> trinoCompatibilityTestRuns = testedTrinoDockerImages().stream()
                .map(image -> testOnEnvironment(EnvSinglenodeCompatibility.class, ImmutableMap.of("compatibility.testDockerImage", image))
                        .withGroups("configured_features", "hive_view_compatibility")
                        .build())
                .collect(toImmutableList());
        ImmutableList<SuiteTestRun> prestoCompatibilityTestRuns = testedPrestoDockerImages().stream()
                .map(image -> testOnEnvironment(EnvSinglenodeCompatibility.class, ImmutableMap.of("compatibility.testDockerImage", image))
                        .withGroups("configured_features", "hive_view_compatibility")
                        .build())
                .collect(toImmutableList());

        return ImmutableList.<SuiteTestRun>builder()
                .addAll(trinoCompatibilityTestRuns)
                .addAll(prestoCompatibilityTestRuns)
                .build();
    }

    private static List<String> testedTrinoDockerImages()
    {
        try {
            String currentVersionString = TestingProperties.getProjectVersion();
            Matcher matcher = Pattern.compile("(\\d+)(?:-SNAPSHOT)?").matcher(currentVersionString);
            checkState(matcher.matches());
            int currentVersion = parseInt(matcher.group(1));
            ImmutableList.Builder<String> testedTrinoVersions = ImmutableList.builder();
            int testVersion = currentVersion - 1; // always test last release version
            for (int i = 0; i < NUMBER_OF_TESTED_VERSIONS; i++) {
                if (testVersion < FIRST_TRINO_VERSION) {
                    break;
                }
                testedTrinoVersions.add(format("%s:%s", TRINO_IMAGE, testVersion));
                testVersion -= TESTED_VERSIONS_GRANULARITY;
            }

            return testedTrinoVersions.build();
        }
        catch (Throwable e) {
            throw new IllegalStateException("Could not determine Trino versions to test; " + e.getMessage(), e);
        }
    }

    private static List<String> testedPrestoDockerImages()
    {
        return ImmutableList.of(format("%s:%s", PRESTOSQL_IMAGE, LAST_PRESTOSQL_VERSION));
    }
}
