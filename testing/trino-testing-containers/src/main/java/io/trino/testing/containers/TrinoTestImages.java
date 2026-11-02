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
package io.trino.testing.containers;

import io.trino.testing.TestingProperties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.parseInt;

public final class TrinoTestImages
{
    private static final String DEFAULT_TRINO_IMAGE = "trinodb/trino:latest";
    private static final Pattern PROJECT_VERSION = Pattern.compile("(\\d+)(?:-SNAPSHOT)?");

    public static final String TRINO_IMAGE_PROPERTY = "trino.product-tests.image";

    private TrinoTestImages() {}

    public static String getDefaultTrinoImage()
    {
        return System.getProperty(TRINO_IMAGE_PROPERTY, DEFAULT_TRINO_IMAGE);
    }

    /**
     * The last released Trino version, for tests exercising container plumbing rather than the code
     * under development. The project version itself is not published as an image yet.
     */
    public static String getLastReleasedTrinoVersion()
    {
        String projectVersion = TestingProperties.getProjectVersion();
        Matcher matcher = PROJECT_VERSION.matcher(projectVersion);
        checkState(matcher.matches(), "Unrecognized project version: %s", projectVersion);
        return String.valueOf(parseInt(matcher.group(1)) - 1);
    }
}
