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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHiveFileSystemAbfsOAuth
        extends AbstractTestHiveFileSystemAbfs
{
    private String endpoint;
    private String clientId;
    private String secret;

    @BeforeAll
    public void setup()
    {
        this.endpoint = checkParameter(System.getProperty("test.hive.azure.abfs.oauth.endpoint"), "endpoint");
        this.clientId = checkParameter(System.getProperty("test.hive.azure.abfs.oauth.client-id"), "client ID");
        this.secret = checkParameter(System.getProperty("test.hive.azure.abfs.oauth.secret"), "secret");
        super.setup(
                System.getProperty("hive.hadoop2.metastoreHost"),
                Integer.getInteger("hive.hadoop2.metastorePort"),
                System.getProperty("hive.hadoop2.databaseName"),
                System.getProperty("test.hive.azure.abfs.container"),
                System.getProperty("test.hive.azure.abfs.storage-account"),
                System.getProperty("test.hive.azure.abfs.test-directory"));
    }

    @Override
    protected HiveAzureConfig getConfig()
    {
        return new HiveAzureConfig()
                .setAbfsOAuthClientEndpoint(endpoint)
                .setAbfsOAuthClientId(clientId)
                .setAbfsOAuthClientSecret(secret);
    }
}
