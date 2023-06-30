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
import io.trino.testing.containers.TestContainers.DockerArchitecture;
import io.trino.tests.product.launcher.env.EnvironmentOptions;

public class Temurin20JdkProvider
        extends TarDownloadingJdkProvider
{
    private static final String VERSION = "20.0.1-9";

    @Inject
    public Temurin20JdkProvider(EnvironmentOptions environmentOptions)
    {
        super(environmentOptions);
    }

    @Override
    protected String getDownloadUri(DockerArchitecture architecture)
    {
        return switch (architecture) {
            case AMD64 -> "https://github.com/adoptium/temurin20-binaries/releases/download/jdk-%s/OpenJDK20U-jdk_x64_linux_hotspot_20.0.1_9.tar.gz".formatted(VERSION.replace("-", "%2B"));
            case ARM64 -> "https://github.com/adoptium/temurin20-binaries/releases/download/jdk-%s/OpenJDK20U-jdk_aarch64_linux_hotspot_20.0.1_9.tar.gz".formatted(VERSION.replace("-", "%2B"));
            default -> throw new IllegalArgumentException("Architecture %s is not supported for Temurin 20 distribution".formatted(architecture));
        };
    }

    @Override
    public String getDescription()
    {
        return "Temurin " + VERSION;
    }
}
