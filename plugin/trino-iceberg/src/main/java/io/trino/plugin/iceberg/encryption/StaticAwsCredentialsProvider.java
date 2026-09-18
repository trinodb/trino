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
package io.trino.plugin.iceberg.encryption;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;

// Used by AwsKeyManagementClient via client.credentials-provider property.
// AwsClientProperties loads credentials providers by calling create(Map<String, String>).
public class StaticAwsCredentialsProvider
        implements AwsCredentialsProvider
{
    private final AwsCredentials credentials;

    private StaticAwsCredentialsProvider(String accessKeyId, String secretAccessKey)
    {
        this.credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey);
    }

    public static StaticAwsCredentialsProvider create(Map<String, String> properties)
    {
        String accessKeyId = properties.get("access-key-id");
        String secretAccessKey = properties.get("secret-access-key");
        return new StaticAwsCredentialsProvider(accessKeyId, secretAccessKey);
    }

    @Override
    public AwsCredentials resolveCredentials()
    {
        return credentials;
    }
}
