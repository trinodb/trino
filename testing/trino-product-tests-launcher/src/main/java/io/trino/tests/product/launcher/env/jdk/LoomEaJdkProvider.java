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

import io.trino.testing.containers.TestContainers.DockerArchitecture;

import java.nio.file.Path;

public class LoomEaJdkProvider
        extends TarDownloadingJdkProvider
{
    public static final String LOOM_EA = "loom-ea";

    public LoomEaJdkProvider(Path jdkDownloadPath)
    {
        super(jdkDownloadPath);
    }

    @Override
    protected String getDownloadUri(DockerArchitecture architecture)
    {
        return switch (architecture) {
            case AMD64 -> "https://download.java.net/java/early_access/loom/1/openjdk-24-loom+1-17_linux-x64_bin.tar.gz";
            case ARM64 -> "https://download.java.net/java/early_access/loom/1/openjdk-24-loom+1-17_linux-aarch64_bin.tar.gz";
            default -> throw new UnsupportedOperationException("Fetching Loom EA for arch " + architecture + " is not supported");
        };
    }

    @Override
    protected String getName()
    {
        return "24-loom+1-17";
    }

    @Override
    public String getDescription()
    {
        return "Loom EA build";
    }
}
