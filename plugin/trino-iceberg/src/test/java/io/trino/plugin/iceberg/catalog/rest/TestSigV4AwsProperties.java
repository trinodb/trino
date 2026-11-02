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
package io.trino.plugin.iceberg.catalog.rest;

import io.trino.filesystem.s3.S3FileSystemConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.iceberg.aws.AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER;
import static org.apache.iceberg.aws.AwsProperties.REST_ACCESS_KEY_ID;
import static org.apache.iceberg.aws.AwsProperties.REST_SECRET_ACCESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSigV4AwsProperties
{
    @Test
    void testStaticCredentials()
    {
        Map<String, String> properties = new SigV4AwsProperties(
                new IcebergRestCatalogSigV4Config(),
                new S3FileSystemConfig()
                        .setRegion("us-east-2")
                        .setAwsAccessKey("access-key")
                        .setAwsSecretKey("secret-key"))
                .get();
        assertThat(properties)
                .containsEntry(REST_ACCESS_KEY_ID, "access-key")
                .containsEntry(REST_SECRET_ACCESS_KEY, "secret-key");
    }

    @Test
    void testDefaultCredentialsProviderChain()
    {
        Map<String, String> properties = new SigV4AwsProperties(
                new IcebergRestCatalogSigV4Config(),
                new S3FileSystemConfig().setRegion("us-east-2"))
                .get();
        assertThat(properties)
                .doesNotContainKeys(REST_ACCESS_KEY_ID, REST_SECRET_ACCESS_KEY, CLIENT_CREDENTIALS_PROVIDER);
    }

    @Test
    void testPartialStaticCredentials()
    {
        assertThatThrownBy(() -> new SigV4AwsProperties(
                new IcebergRestCatalogSigV4Config(),
                new S3FileSystemConfig()
                        .setRegion("us-east-2")
                        .setAwsAccessKey("access-key")))
                .hasMessage("s3.aws-secret-key is null");
        assertThatThrownBy(() -> new SigV4AwsProperties(
                new IcebergRestCatalogSigV4Config(),
                new S3FileSystemConfig()
                        .setRegion("us-east-2")
                        .setAwsSecretKey("secret-key")))
                .hasMessage("s3.aws-access-key is null");
    }

    @Test
    void testIamRole()
    {
        Map<String, String> properties = new SigV4AwsProperties(
                new IcebergRestCatalogSigV4Config(),
                new S3FileSystemConfig()
                        .setRegion("us-east-2")
                        .setIamRole("arn:aws:iam::123456789012:role/example"))
                .get();
        assertThat(properties)
                .containsEntry(CLIENT_CREDENTIALS_PROVIDER, "io.trino.plugin.iceberg.StsAwsCredentialProvider")
                .doesNotContainKeys(REST_ACCESS_KEY_ID, REST_SECRET_ACCESS_KEY);
    }
}
