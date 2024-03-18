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
package io.varada.cloudstorage.azure;

import io.airlift.configuration.ConfigurationFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.azure.AzureAuth;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.spi.connector.ConnectorContext;
import io.varada.annotation.Default;
import io.varada.cloudstorage.CloudStorageAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import java.util.Map;

@Disabled
public class AzureCloudStorageTest
        extends CloudStorageAbstractTest
{
    public static final String ABFS_ACCESS_KEY = "";
    public static final String ABFS_CONTAINER = "poc";
    public static final String ABFS_ACCOUNT = "siactestdata";

    @BeforeEach
    void setUp()
    {
        OpenTelemetry openTelemetry = OpenTelemetry.noop();

        AzureAuth azureAuth = new AzureAuthAccessKey(ABFS_ACCESS_KEY);
        AzureFileSystemConfig azureFileSystemConfig = new AzureFileSystemConfig()
                .setAuthType(AzureFileSystemConfig.AuthType.ACCESS_KEY);

        AzureFileSystemFactory fileSystemFactory = new AzureFileSystemFactory(openTelemetry,
                azureAuth,
                azureFileSystemConfig);

        ConfigurationFactory configurationFactory = new ConfigurationFactory(Map.of("azure.auth-type", "ACCESS_KEY"));
        AzureCloudStorageModule module = new AzureCloudStorageModule(new TestingConnectorContext(), configurationFactory, Default.class);

        cloudStorage = module.provideAzureCloudStorage(fileSystemFactory, openTelemetry, azureAuth);
    }

    @Override
    protected String getBucket()
    {
        return "abfss://%s@%s.dfs.core.windows.net/".formatted(ABFS_CONTAINER, ABFS_ACCOUNT);
    }

    @Override
    protected String getEmptyBucket()
    {
        return "abfss://%s@%s.dfs.core.windows.net/empty".formatted(ABFS_CONTAINER, ABFS_ACCOUNT);
    }

    @Override
    protected String getNotExistBucket()
    {
        return "abfss://%s@%s.dfs.core.windows.net/not-exist".formatted(ABFS_CONTAINER, ABFS_ACCOUNT);
    }

    static class TestingConnectorContext
            implements ConnectorContext
    {
    }
}
