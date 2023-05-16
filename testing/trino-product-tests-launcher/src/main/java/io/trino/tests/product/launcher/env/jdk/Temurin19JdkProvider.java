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
package io.trino.tests.product.launcher.env.jdk;

import com.google.inject.Inject;
import io.trino.testing.containers.TestContainers;
import io.trino.tests.product.launcher.env.EnvironmentOptions;

public class Temurin19JdkProvider
        extends TarDownloadingJdkProvider
{
    private static final String VERSION = "19.0.2-7";

    @Inject
    public Temurin19JdkProvider(EnvironmentOptions environmentOptions)
    {
        super(environmentOptions);
    }

    @Override
    protected String getDownloadUri(TestContainers.DockerArchitecture architecture)
    {
        return switch (architecture) {
            case AMD64 -> "https://github.com/adoptium/temurin19-binaries/releases/download/jdk-%s/OpenJDK19U-jdk_x64_linux_hotspot_19.0.2_7.tar.gz".formatted(VERSION.replace("-", "%2B"));
            case ARM64 -> "https://github.com/adoptium/temurin19-binaries/releases/download/jdk-%s/OpenJDK19U-jdk_aarch64_linux_hotspot_19.0.2_7.tar.gz".formatted(VERSION.replace("-", "%2B"));
            default -> throw new IllegalArgumentException("Architecture %s is not supported for Temurin 19 distribution".formatted(architecture));
        };
    }

    @Override
    public String getDescription()
    {
        return "Temurin " + VERSION;
    }
}
