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

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class S3PresignedUriGenerator
        implements S3GetObjectPresigner
{
    private final S3Presigner presigner;
    private final S3PresigningCredentials clientCredentials;

    public S3PresignedUriGenerator(S3Presigner presigner, AwsCredentialsProvider credentialsProvider)
    {
        this.presigner = requireNonNull(presigner, "presigner is null");
        this.clientCredentials = new S3PresigningCredentials(requireNonNull(credentialsProvider, "credentialsProvider is null"));
    }

    @Override
    public PresignedGetObjectRequest presignGetObject(GetObjectPresignRequest request)
            throws IOException
    {
        checkArgument(request.signatureDuration().isPositive() && request.signatureDuration().compareTo(Duration.ofDays(7)) <= 0,
                "S3 presigned URI lifetime must be positive and no greater than seven days");
        Optional<AwsCredentialsProvider> override = request.getObjectRequest().overrideConfiguration()
                .flatMap(AwsRequestOverrideConfiguration::credentialsProvider);
        // Override providers are created per file system, so caching their credentials would never hit
        S3PresigningCredentials signingCredentials = override.map(S3PresigningCredentials::new).orElse(clientCredentials);
        AwsCredentials snapshot = signingCredentials.resolve(request.signatureDuration());
        GetObjectRequest getObject = request.getObjectRequest().toBuilder()
                .overrideConfiguration(request.getObjectRequest().overrideConfiguration().orElseGet(() -> AwsRequestOverrideConfiguration.builder().build())
                        .toBuilder()
                        .credentialsProvider(StaticCredentialsProvider.create(snapshot))
                        .build())
                .build();
        PresignedGetObjectRequest signed = presigner.presignGetObject(request.toBuilder().getObjectRequest(getObject).build());
        // Check the SDK's actual expiration too, accounting for time spent resolving and signing.
        S3PresigningCredentials.validate(snapshot, signed.expiration());
        return signed;
    }

    @Override
    public void close()
    {
        presigner.close();
    }
}
