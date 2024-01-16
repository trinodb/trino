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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.IOException;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class TestUnloadAbfs
        extends BaseUnloadFileSystemTest
{
    private final String container;
    private final String account;
    private final String accessKey;
    private final String bucketName;

    public TestUnloadAbfs()
    {
        container = requireEnv("ABFS_CONTAINER");
        account = requireEnv("ABFS_ACCOUNT");
        accessKey = requireEnv("ABFS_ACCESSKEY");
        bucketName = "test-unload-test-" + randomNameSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.azure.abfs-storage-account", account)
                        .put("hive.azure.abfs-access-key", accessKey)
                        .buildOrThrow())
                .build();
    }

    @Override
    protected TrinoFileSystemFactory getFileSystemFactory()
            throws IOException
    {
        HdfsConfig hdfsConfig = new HdfsConfig();
        HiveAzureConfig azureConfig = new HiveAzureConfig()
                .setAbfsStorageAccount(account)
                .setAbfsAccessKey(accessKey);
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(
                new HdfsConfigurationInitializer(hdfsConfig, ImmutableSet.of(new TrinoAzureConfigurationInitializer(azureConfig))),
                ImmutableSet.of());
        return new HdfsFileSystemFactory(new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication()), new TrinoHdfsFileSystemStats());
    }

    @Override
    protected String getLocation(String path)
    {
        return "abfs://%s@%s.dfs.core.windows.net/%s/%s".formatted(container, account, bucketName, path);
    }
}
