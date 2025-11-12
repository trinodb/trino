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
package io.trino.server;

import com.google.common.io.Resources;

import java.io.InputStream;
import java.util.Properties;

import static com.google.common.base.MoreObjects.firstNonNull;

public final class TrinoServer
{
    private TrinoServer() {}

    public static void main(String[] args)
    {
        Runtime.Version javaVersion = Runtime.version();
        int requiredVersion = requiredJavaVersion();
        if (javaVersion.feature() < requiredVersion) {
            System.err.printf("ERROR: Trino requires Java %d+ (found %s)%n", requiredVersion, javaVersion);
            System.exit(100);
        }

        String trinoVersion = TrinoServer.class.getPackage().getImplementationVersion();
        new Server().start(firstNonNull(trinoVersion, "unknown"));
    }

    private static int requiredJavaVersion()
    {
        Properties properties = new Properties();
        try (InputStream inputStream = Resources.getResource("io/trino/server/build.properties").openStream()) {
            properties.load(inputStream);
            return Integer.parseInt(properties.getProperty("target.jdk"));
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to load required Java version from properties file", e);
        }
    }
}
