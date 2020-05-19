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
package io.prestosql.tests.product.launcher.testcontainers;

import io.prestosql.testing.AbstractTestDistributedQueries;
import org.testcontainers.utility.TestcontainersConfiguration;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class TestRyukImage
{
    void fakeDependency()
    {
        // Launcher depends on presto-testing resource classpath. Make maven dependency check happy.
        System.out.println(AbstractTestDistributedQueries.class);
    }

    @Test
    public void testImageVersion()
            throws Exception
    {
        String defaultRyukImage = getDefaultConfiguration().getRyukImage();
        assertThat(defaultRyukImage).doesNotContain("presto"); // sanity check defaults are indeed defaults
        assertThat(defaultRyukImage).startsWith("quay.io/testcontainers/ryuk"); // if the image is moved to e.g. Docker Hub, we may want to remove our override

        Matcher matcher = Pattern.compile(".*:(\\d\\.\\d\\.\\d)$").matcher(defaultRyukImage);
        assertTrue(matcher.matches());
        String version = matcher.group(1);

        String effectiveRyukImage = TestcontainersConfiguration.getInstance().getRyukImage();

        // Verify we are using our image (otherwise this test method is not needed).
        assertThat(effectiveRyukImage).startsWith("prestodev/");

        // Verify we have the same version
        assertThat(effectiveRyukImage).endsWith(":" + version);
    }

    private TestcontainersConfiguration getDefaultConfiguration()
            throws Exception
    {
        Constructor<TestcontainersConfiguration> constructor = TestcontainersConfiguration.class.getDeclaredConstructor(Properties.class, Properties.class);
        constructor.setAccessible(true);
        return constructor.newInstance(new Properties(), new Properties());
    }
}
