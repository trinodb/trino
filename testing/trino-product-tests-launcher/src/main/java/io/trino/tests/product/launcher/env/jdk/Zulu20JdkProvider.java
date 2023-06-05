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

public class Zulu20JdkProvider
        extends TarDownloadingJdkProvider
{
    private static final String VERSION = "20.0.1";

    @Inject
    public Zulu20JdkProvider(EnvironmentOptions environmentOptions)
    {
        super(environmentOptions);
    }

    @Override
    protected String getDownloadUri(TestContainers.DockerArchitecture architecture)
    {
        return switch (architecture) {
            case AMD64 -> "https://cdn.azul.com/zulu/bin/zulu20.30.11-ca-jdk%s-linux_x64.tar.gz".formatted(VERSION);
            case ARM64 -> "https://cdn.azul.com/zulu/bin/zulu20.30.11-ca-jdk%s-linux_aarch64.tar.gz".formatted(VERSION);
            default -> throw new IllegalArgumentException("Architecture %s is not supported for Zulu 20 distribution".formatted(architecture));
        };
    }

    @Override
    public String getDescription()
    {
        return "Zulu " + VERSION;
    }
}
