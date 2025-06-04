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
import io.trino.testing.TestingProperties;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentDefaults;
import io.trino.tests.product.launcher.env.environment.EnvSinglenodeCompatibility;
import io.trino.tests.product.launcher.suite.Suite;
import io.trino.tests.product.launcher.suite.SuiteTestRun;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tests.product.TestGroups.CONFIGURED_FEATURES;
import static io.trino.tests.product.TestGroups.HIVE_VIEW_COMPATIBILITY;
import static io.trino.tests.product.TestGroups.ICEBERG_FORMAT_VERSION_COMPATIBILITY;
import static io.trino.tests.product.launcher.suite.SuiteTestRun.testOnEnvironment;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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

        List<SuiteTestRun> trinoCompatibilityTestRuns = testedTrinoDockerImages().stream()
                .map(testedImage -> testOnEnvironment(EnvSinglenodeCompatibility.class, Map.of("compatibility.testVersion", Integer.toString(testedImage.version), "compatibility.testDockerImage", testedImage.image))
                        .withGroups(CONFIGURED_FEATURES, HIVE_VIEW_COMPATIBILITY, ICEBERG_FORMAT_VERSION_COMPATIBILITY)
                        .build())
                .collect(toImmutableList());
        List<SuiteTestRun> prestoCompatibilityTestRuns = testedPrestoDockerImages().stream()
                .map(testedImage -> testOnEnvironment(EnvSinglenodeCompatibility.class, Map.of("compatibility.testVersion", Integer.toString(testedImage.version), "compatibility.testDockerImage", testedImage.image))
                        .withGroups(CONFIGURED_FEATURES, HIVE_VIEW_COMPATIBILITY)
                        .build())
                .collect(toImmutableList());

        return ImmutableList.<SuiteTestRun>builder()
                .addAll(trinoCompatibilityTestRuns)
                .addAll(prestoCompatibilityTestRuns)
                .build();
    }

    private static List<TestedImage> testedTrinoDockerImages()
    {
        try {
            String currentVersionString = TestingProperties.getProjectVersion();
            Matcher matcher = Pattern.compile("(\\d+)(?:-SNAPSHOT)?").matcher(currentVersionString);
            checkState(matcher.matches());
            int currentVersion = parseInt(matcher.group(1));
            ImmutableList.Builder<TestedImage> testedTrinoVersions = ImmutableList.builder();
            int testVersion = currentVersion - 1; // always test last release version
            for (int i = 0; i < NUMBER_OF_TESTED_VERSIONS; i++) {
                if (testVersion == 456) {
                    // 456 release was skipped.
                    testVersion--;
                }
                if (testVersion < FIRST_TRINO_VERSION) {
                    break;
                }
                testedTrinoVersions.add(new TestedImage(testVersion, format("%s:%s", TRINO_IMAGE, testVersion)));
                testVersion -= TESTED_VERSIONS_GRANULARITY;
            }

            return testedTrinoVersions.build();
        }
        catch (Throwable e) {
            throw new IllegalStateException("Could not determine Trino versions to test; " + e.getMessage(), e);
        }
    }

    private static List<TestedImage> testedPrestoDockerImages()
    {
        return ImmutableList.of(new TestedImage(LAST_PRESTOSQL_VERSION, format("%s:%s", PRESTOSQL_IMAGE, LAST_PRESTOSQL_VERSION)));
    }

    record TestedImage(int version, String image)
    {
        TestedImage
        {
            requireNonNull(image, "image is null");
        }
    }
}
