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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.util.Strings.isNullOrEmpty;

@TestInstance(PER_CLASS)
public class TestHiveFileSystemWasb
        extends AbstractTestHiveFileSystem
{
    private String container;
    private String account;
    private String accessKey;
    private String testDirectory;

    @BeforeAll
    public void setup()
    {
        String host = System.getProperty("hive.hadoop2.metastoreHost");
        int port = Integer.getInteger("hive.hadoop2.metastorePort");
        String databaseName = System.getProperty("hive.hadoop2.databaseName");
        String container = System.getProperty("hive.hadoop2.wasb.container");
        String account = System.getProperty("hive.hadoop2.wasb.account");
        String accessKey = System.getProperty("hive.hadoop2.wasb.accessKey");
        String testDirectory = System.getProperty("hive.hadoop2.wasb.testDirectory");

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

        super.setup(host, port, databaseName, createHdfsConfiguration());
    }

    private HdfsConfiguration createHdfsConfiguration()
    {
        ConfigurationInitializer wasbConfig = new TrinoAzureConfigurationInitializer(new HiveAzureConfig()
                .setWasbAccessKey(accessKey)
                .setWasbStorageAccount(account));
        return new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(wasbConfig)), ImmutableSet.of());
    }

    @Override
    protected Path getBasePath()
    {
        return new Path(format("wasb://%s@%s.blob.core.windows.net/%s/", container, account, testDirectory));
    }
}
