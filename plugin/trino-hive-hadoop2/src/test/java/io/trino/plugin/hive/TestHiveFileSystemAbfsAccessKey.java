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

import io.trino.hdfs.azure.HiveAzureConfig;
import org.testng.annotations.BeforeClass;

public class TestHiveFileSystemAbfsAccessKey
        extends AbstractTestHiveFileSystemAbfs
{
    private String accessKey;

    @BeforeClass
    public void setup()
    {
        this.accessKey = checkParameter(System.getProperty("hive.hadoop2.abfs.accessKey"), "access key");

        super.setup(
                System.getProperty("hive.hadoop2.metastoreHost"),
                Integer.parseInt(System.getProperty("hive.hadoop2.metastorePort")),
                System.getProperty("hive.hadoop2.databaseName"),
                System.getProperty("hive.hadoop2.abfs.container"),
                System.getProperty("hive.hadoop2.abfs.account"),
                System.getProperty("hive.hadoop2.abfs.testDirectory"));
    }

    @Override
    protected HiveAzureConfig getConfig()
    {
        return new HiveAzureConfig()
                .setAbfsAccessKey(accessKey)
                .setAbfsStorageAccount(account);
    }
}
