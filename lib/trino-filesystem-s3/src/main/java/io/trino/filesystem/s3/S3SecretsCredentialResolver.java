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

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.secrets.RuntimeSecretResolver;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

final class S3SecretsCredentialResolver
{
    private final RuntimeSecretResolver secretResolver;
    private final String secretProvider;
    private final String bucketKeyPrefix;

    @Inject
    S3SecretsCredentialResolver(RuntimeSecretResolver secretResolver, S3FileSystemConfig config)
    {
        this.secretResolver = requireNonNull(secretResolver, "secretResolver is null");
        requireNonNull(config, "config is null");
        this.secretProvider = requireNonNull(config.getSecretsProvider(), "secretsProvider is null");
        this.bucketKeyPrefix = requireNonNull(config.getSecretsBucketKeyPrefix(), "secretsBucketKeyPrefix is null");
    }

    BucketCredentials resolveBucketCredentials(String bucket)
    {
        requireNonNull(bucket, "bucket is null");
        try {
            String accessKeyAlias = bucketKeyPrefix + bucket + ".access.key";
            String secretKeyAlias = bucketKeyPrefix + bucket + ".secret.key";
            return new BucketCredentials(
                    secretResolver.resolveSecret(secretProvider, accessKeyAlias),
                    secretResolver.resolveSecret(secretProvider, secretKeyAlias));
        }
        catch (RuntimeException e) {
            throw new TrinoException(CONFIGURATION_INVALID, "Failed to resolve S3 credentials for bucket: " + bucket, e);
        }
    }

    record BucketCredentials(String accessKey, String secretKey) {}
}
