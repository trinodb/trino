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

import org.apache.iceberg.aws.AwsKeyManagementClient;
import org.apache.iceberg.encryption.KeyManagementClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;

import java.util.Map;

import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;

final class TestIcebergAwsEncryptionFloci
        extends BaseIcebergEncryptionFlociTest
{
    @Override
    protected String kmsKey()
    {
        try (KmsClient client = KmsClient.builder()
                .endpointOverride(hiveFlociDataLake.floci().endpoint())
                .region(Region.of(FLOCI_REGION))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(FLOCI_ACCESS_KEY, FLOCI_SECRET_KEY)))
                .build()) {
            return client.createKey(CreateKeyRequest.builder().build()).keyMetadata().arn();
        }
    }

    @Override
    protected KeyManagementClient kmsClient()
    {
        KeyManagementClient keyManagementClient = new AwsKeyManagementClient();
        keyManagementClient.initialize(Map.of(
                "kms.endpoint", hiveFlociDataLake.floci().endpoint().toString(),
                "client.region", FLOCI_REGION,
                "client.credentials-provider", StaticAwsCredentialsProvider.class.getName(),
                "client.credentials-provider.access-key-id", FLOCI_ACCESS_KEY,
                "client.credentials-provider.secret-access-key", FLOCI_SECRET_KEY));
        return keyManagementClient;
    }
}
