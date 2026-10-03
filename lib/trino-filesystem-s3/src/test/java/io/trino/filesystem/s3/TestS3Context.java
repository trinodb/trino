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

import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.signer.Signer;

import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType.STANDARD;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("deprecation")
final class TestS3Context
{
    @Test
    void testRemoteSignerOverridesRequestSigning()
    {
        Signer signer = (request, _) -> request;
        S3Context context = new S3Context(
                5 * 1024 * 1024,
                false,
                S3Context.S3SseContext.of(S3FileSystemConfig.S3SseType.NONE, null, null),
                Optional.empty(),
                STANDARD,
                S3FileSystemConfig.ObjectCannedAcl.NONE)
                .withCredentials(ConnectorIdentity.ofUser("test"), Optional.of(signer));

        AwsRequestOverrideConfiguration.Builder builder = AwsRequestOverrideConfiguration.builder();
        context.applyRequestOverrides(builder);
        AwsRequestOverrideConfiguration configuration = builder.build();

        assertThat(configuration.signer()).contains(signer);
        assertThat(configuration.credentialsProvider()).containsInstanceOf(AnonymousCredentialsProvider.class);
    }
}
