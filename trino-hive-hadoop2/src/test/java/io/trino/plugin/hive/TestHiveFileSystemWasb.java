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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.azure.HiveAzureConfig;
import io.prestosql.plugin.hive.azure.PrestoAzureConfigurationInitializer;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.util.Strings.isNullOrEmpty;

public class TestHiveFileSystemWasb
        extends AbstractTestHiveFileSystem
{
    private String container;
    private String account;
    private String accessKey;
    private String testDirectory;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.wasb.container",
            "hive.hadoop2.wasb.account",
            "hive.hadoop2.wasb.accessKey",
            "hive.hadoop2.wasb.testDirectory",
    })
    @BeforeClass
    public void setup(String host, int port, String databaseName, String container, String account, String accessKey, String testDirectory)
    {
        checkArgument(!isNullOrEmpty(host), "expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "expected non empty databaseName");
        checkArgument(!isNullOrEmpty(container), "expected non empty container");
        checkArgument(!isNullOrEmpty(account), "expected non empty account");
        checkArgument(!isNullOrEmpty(accessKey), "expected non empty accessKey");
        checkArgument(!isNullOrEmpty(testDirectory), "expected non empty testDirectory");

        this.container = container;
        this.account = account;
        this.accessKey = accessKey;
        this.testDirectory = testDirectory;

        super.setup(host, port, databaseName, false, createHdfsConfiguration());
    }

    private HdfsConfiguration createHdfsConfiguration()
    {
        ConfigurationInitializer wasbConfig = new PrestoAzureConfigurationInitializer(new HiveAzureConfig()
                .setWasbAccessKey(accessKey)
                .setWasbStorageAccount(account));
        return new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(wasbConfig)), ImmutableSet.of());
    }

    @Override
    protected Path getBasePath()
    {
        return new Path(format("wasb://%s@%s.blob.core.windows.net/%s/", container, account, testDirectory));
    }
}
