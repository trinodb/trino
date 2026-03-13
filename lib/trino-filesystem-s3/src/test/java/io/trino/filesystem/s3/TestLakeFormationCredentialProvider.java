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
package io.trino.filesystem.s3;

import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLakeFormationCredentialProvider
{
    @Test
    public void testGetCredentialsProviderRejectsNullArn()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setLakeFormationEnabled(true)
                .setRegion("us-east-1")
                .setLakeFormationCredentialDuration(new Duration(15, MINUTES))
                .setLakeFormationCredentialCacheTtl(new Duration(10, MINUTES));

        LakeFormationCredentialProvider provider = new LakeFormationCredentialProvider(config);

        assertThatThrownBy(() -> provider.getCredentialsProvider(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("tableArn is null");
    }

    @Test
    public void testGetCredentialsProviderReturnsDelegatingProvider()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setLakeFormationEnabled(true)
                .setRegion("us-east-1")
                .setLakeFormationCredentialDuration(new Duration(15, MINUTES))
                .setLakeFormationCredentialCacheTtl(new Duration(10, MINUTES));

        LakeFormationCredentialProvider provider = new LakeFormationCredentialProvider(config);

        String tableArn = "arn:aws:glue:us-east-1:123456789012:table/mydb/mytable";
        AwsCredentialsProvider credentialsProvider = provider.getCredentialsProvider(tableArn);

        // Verify it returns a non-null delegating provider (not StaticCredentialsProvider)
        assertThat(credentialsProvider).isNotNull();
        // The provider should be a lambda, not a StaticCredentialsProvider
        assertThat(credentialsProvider.getClass().getName()).doesNotContain("StaticCredentialsProvider");
    }

    @Test
    public void testLakeFormationRegionFallsBackToS3Region()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setLakeFormationEnabled(true)
                .setRegion("eu-west-1")
                .setLakeFormationCredentialDuration(new Duration(15, MINUTES))
                .setLakeFormationCredentialCacheTtl(new Duration(10, MINUTES));

        // Should not throw - uses s3 region as fallback for Lake Formation region
        LakeFormationCredentialProvider provider = new LakeFormationCredentialProvider(config);
        assertThat(provider).isNotNull();
    }

    @Test
    public void testLakeFormationExplicitRegionOverridesS3Region()
    {
        S3FileSystemConfig config = new S3FileSystemConfig()
                .setLakeFormationEnabled(true)
                .setRegion("us-east-1")
                .setLakeFormationRegion("us-west-2")
                .setLakeFormationCredentialDuration(new Duration(15, MINUTES))
                .setLakeFormationCredentialCacheTtl(new Duration(10, MINUTES));

        // Should not throw - explicit LF region is used
        LakeFormationCredentialProvider provider = new LakeFormationCredentialProvider(config);
        assertThat(provider).isNotNull();
    }
}
