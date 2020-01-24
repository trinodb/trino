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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

public class TestHiveAzure
        extends AbstractTestHive
{
    private String account;
    private String accessKey;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.timeZone",
            "hive.hadoop2.wasb-account",
            "hive.hadoop2.wasb-access-key",
    })
    @BeforeClass
    public void initialize(String host, int port, String databaseName, String timeZone, String account, String accessKey)
    {
        checkArgument(!isNullOrEmpty(host), "expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "Expected non empty databaseName");
        checkArgument(!isNullOrEmpty(timeZone), "Expected non empty timeZone");
        checkArgument(!isNullOrEmpty(account), "expected non empty account");
        checkArgument(!isNullOrEmpty(accessKey), "expected non empty accessKey");

        this.account = account;
        this.accessKey = accessKey;

        setup(host, port, databaseName, timeZone);
    }

    @Override
    protected HiveHdfsConfiguration createTestHdfsConfiguration()
    {
        ConfigurationInitializer wasbConfig = new PrestoAzureConfigurationInitializer(new HiveAzureConfig()
                .setWasbAccessKey(accessKey)
                .setWasbStorageAccount(account));
        return new HiveHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(wasbConfig)), ImmutableSet.of());
    }
}
