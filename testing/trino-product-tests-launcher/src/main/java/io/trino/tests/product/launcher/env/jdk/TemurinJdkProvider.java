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
import jakarta.annotation.Nullable;

import java.nio.file.Path;

import static java.util.Objects.requireNonNull;

public final class TemurinJdkProvider
        extends TarDownloadingJdkProvider
{
    private static final String TEMURIN_DOWNLOAD_URL = "https://api.adoptium.net/v3/binary/version/%s/linux/%s/jdk/hotspot/normal/eclipse?project=jdk";

    private final String releaseName;

    public TemurinJdkProvider(String releaseName, @Nullable Path downloadPath)
    {
        super(downloadPath);
        this.releaseName = requireNonNull(releaseName, "releaseName is null");
    }

    @Override
    public String getDescription()
    {
        return releaseName;
    }

    @Override
    protected String getName()
    {
        return "Temurin " + releaseName.replace("/", "_");
    }

    @Override
    protected String getDownloadUri(TestContainers.DockerArchitecture architecture)
    {
        return TEMURIN_DOWNLOAD_URL.formatted(releaseName, getTemurinArchitectureName(architecture));
    }

    private static String getTemurinArchitectureName(TestContainers.DockerArchitecture architecture)
    {
        return switch (architecture) {
            case AMD64 -> "x64";
            case ARM64 -> "aarch64";
            case PPC64 -> "ppc64le";
        };
    }
}
