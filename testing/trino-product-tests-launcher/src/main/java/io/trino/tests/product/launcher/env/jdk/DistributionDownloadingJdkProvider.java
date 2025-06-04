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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public final class DistributionDownloadingJdkProvider
        extends TarDownloadingJdkProvider
{
    private final String distributionName;
    private final Path distributionPath;

    public DistributionDownloadingJdkProvider(String distributionsPath, String distributionName, @Nullable Path downloadPath)
    {
        super(downloadPath);
        this.distributionName = requireNonNull(distributionName, "distributionName is null");
        this.distributionPath = Paths.get(distributionsPath).resolve(distributionName);
    }

    @Override
    public String getDescription()
    {
        return distributionName;
    }

    @Override
    protected String getName()
    {
        return distributionName.replace("/", "_");
    }

    @Override
    protected String getDownloadUri(TestContainers.DockerArchitecture architecture)
    {
        Properties properties = new Properties();
        try (InputStream inputStream = Files.newInputStream(getDistributionPath(distributionPath, architecture))) {
            properties.load(inputStream);
            return requireNonNull(properties.getProperty("distributionUrl"), "distributionUrl is null").trim();
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Architecture %s not found in distribution %s".formatted(architecture, distributionPath));
        }
    }

    private static Path getDistributionPath(Path distributionPath, TestContainers.DockerArchitecture architecture)
    {
        return switch (architecture) {
            case AMD64 -> distributionPath.resolve("amd64");
            case ARM64 -> distributionPath.resolve("arm64");
            case PPC64 -> distributionPath.resolve("ppc64le");
        };
    }
}
