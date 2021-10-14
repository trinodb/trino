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
package io.trino.testing;

import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class TestingProperties
{
    public static final TestingProperties INSTANCE = new TestingProperties("trino-testing.properties");

    private Properties properties = new Properties();

    public TestingProperties(String resourceName)
    {
        try {
            try (InputStream stream = Resources.getResource(resourceName).openStream()) {
                properties.load(stream);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String getProjectVersion()
    {
        return requireNonNull(properties.getProperty("project.version"), "project.version is null");
    }

    public String getDockerImagesVersion()
    {
        return requireNonNull(properties.getProperty("docker.images.version"), "docker.images.version is null");
    }
}
