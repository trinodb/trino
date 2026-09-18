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

import com.google.common.collect.ImmutableList;
import io.trino.testing.TestingProperties;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.compatibility.CompatibilityEnvironment;
import io.trino.tests.product.suite.SuiteRunner.TestRunResult;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.tests.product.compatibility.CompatibilityEnvironment.COMPATIBILITY_IMAGE_PROPERTY;
import static io.trino.tests.product.compatibility.CompatibilityEnvironment.COMPATIBILITY_VERSION_PROPERTY;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public final class SuiteCompatibility
{
    private static final String PRESTOSQL_IMAGE = "ghcr.io/trinodb/presto";
    private static final String TRINO_IMAGE = "trinodb/trino";
    private static final int FIRST_TRINO_VERSION = 351;
    private static final int NUMBER_OF_TESTED_VERSIONS = 5;
    private static final int TESTED_VERSIONS_GRANULARITY = 3;

    private SuiteCompatibility() {}

    public static void main(String[] args)
            throws Exception
    {
        System.setProperty("trino.product-test.environment-mode", "STRICT");

        List<TestRunResult> results = new ArrayList<>();

        for (TestedImage image : testedTrinoImages()) {
            results.add(runTrinoCompatibilityTests(image));
        }
        results.add(runPrestoCompatibilityTests(new TestedImage(350, PRESTOSQL_IMAGE + ":350")));

        SuiteRunner.printSummary(results);
        System.exit(SuiteRunner.hasFailures(results) ? 1 : 0);
    }

    private static TestRunResult runTrinoCompatibilityTests(TestedImage image)
            throws Exception
    {
        setCompatibilityImage(image);
        try {
            return SuiteRunner.forEnvironment(CompatibilityEnvironment.class)
                    .includeTag(TestGroup.Compatibility.class)
                    .run();
        }
        finally {
            clearCompatibilityImage();
        }
    }

    private static TestRunResult runPrestoCompatibilityTests(TestedImage image)
            throws Exception
    {
        setCompatibilityImage(image);
        try {
            return SuiteRunner.forEnvironment(CompatibilityEnvironment.class)
                    .includeTag(TestGroup.HiveViewCompatibility.class)
                    .run();
        }
        finally {
            clearCompatibilityImage();
        }
    }

    private static List<TestedImage> testedTrinoImages()
    {
        Matcher matcher = Pattern.compile("(\\d+)(?:-SNAPSHOT)?").matcher(TestingProperties.getProjectVersion());
        if (!matcher.matches()) {
            throw new IllegalStateException("Invalid current version: " + TestingProperties.getProjectVersion());
        }

        ImmutableList.Builder<TestedImage> images = ImmutableList.builder();
        int version = parseInt(matcher.group(1)) - 1;
        for (int index = 0; index < NUMBER_OF_TESTED_VERSIONS && version >= FIRST_TRINO_VERSION; index++) {
            if (version == 456) {
                version--;
            }
            images.add(new TestedImage(version, format("%s:%s", TRINO_IMAGE, version)));
            version -= TESTED_VERSIONS_GRANULARITY;
        }
        return images.build();
    }

    private static void setCompatibilityImage(TestedImage image)
    {
        System.setProperty(COMPATIBILITY_IMAGE_PROPERTY, image.image());
        System.setProperty(COMPATIBILITY_VERSION_PROPERTY, Integer.toString(image.version()));
    }

    private static void clearCompatibilityImage()
    {
        System.clearProperty(COMPATIBILITY_IMAGE_PROPERTY);
        System.clearProperty(COMPATIBILITY_VERSION_PROPERTY);
    }

    private record TestedImage(int version, String image) {}
}
