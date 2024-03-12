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
package io.trino.filesystem.azure;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.io.IOException;

import static io.trino.filesystem.azure.AbstractTestAzureFileSystem.AccountKind.HIERARCHICAL;

@TestInstance(Lifecycle.PER_CLASS)
public class TestAzureFileSystemOAuthGen2Hierarchical
        extends AbstractTestAzureFileSystem
{
    @BeforeAll
    void setup()
            throws IOException
    {
        String account = getRequiredEnvironmentVariable("ABFS_HIERARCHICAL_ACCOUNT");
        String tenantId = getRequiredEnvironmentVariable("ABFS_OAUTH_TENANT_ID");
        String clientId = getRequiredEnvironmentVariable("ABFS_OAUTH_CLIENT_ID");
        String clientSecret = getRequiredEnvironmentVariable("ABFS_OAUTH_CLIENT_SECRET");
        initializeWithOAuth(account, tenantId, clientId, clientSecret, HIERARCHICAL);
    }
}
