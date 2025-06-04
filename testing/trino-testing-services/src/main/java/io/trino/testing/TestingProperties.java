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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.Resources;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class TestingProperties
{
    private TestingProperties() {}

    private static final Supplier<Properties> properties = Suppliers.memoize(() -> {
        Properties properties = new Properties();
        try {
            try (InputStream stream = Resources.getResource("trino-testing.properties").openStream()) {
                properties.load(stream);
            }

            return properties;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    });

    public static String getProjectVersion()
    {
        return getProjectProperty("project.version");
    }

    public static String getDockerImagesVersion()
    {
        return getProjectProperty("docker.images.version");
    }

    private static String getProjectProperty(String name)
    {
        return requireNonNull(properties.get().getProperty(name), name + " is null");
    }

    public static String requiredNonEmptySystemProperty(String propertyName)
    {
        String value = System.getProperty(propertyName);
        checkArgument(!isNullOrEmpty(value), "System property %s must be non-empty", propertyName);
        return value;
    }
}
