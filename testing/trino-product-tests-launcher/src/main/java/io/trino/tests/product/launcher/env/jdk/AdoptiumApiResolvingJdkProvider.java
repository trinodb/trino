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

import io.trino.testing.containers.TestContainers;
import io.trino.tests.product.launcher.env.EnvironmentOptions;

public abstract class AdoptiumApiResolvingJdkProvider
        extends TarDownloadingJdkProvider
{
    public AdoptiumApiResolvingJdkProvider(EnvironmentOptions environmentOptions)
    {
        super(environmentOptions);
    }

    protected abstract String getReleaseName();

    @Override
    public String getDescription()
    {
        return "Temurin " + getReleaseName();
    }

    @Override
    protected String getDownloadUri(TestContainers.DockerArchitecture architecture)
    {
        return switch (architecture) {
            case AMD64 -> "https://api.adoptium.net/v3/binary/version/%s/linux/%s/jdk/hotspot/normal/eclipse?project=jdk".formatted(getReleaseName(), "x64");
            case ARM64 -> "https://api.adoptium.net/v3/binary/version/%s/linux/%s/jdk/hotspot/normal/eclipse?project=jdk".formatted(getReleaseName(), "aarch64");
            default -> throw new UnsupportedOperationException("Fetching Temurin JDK for arch " + architecture + " is not supported");
        };
    }
}
