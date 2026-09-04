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
package io.trino.filesystem.s3.keystore;

import io.trino.spi.TrinoException;

import java.security.KeyStore;
import java.util.Locale;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.util.Objects.requireNonNull;

public final class KeyStoreCredentialAliasResolver
{
    private final KeyStore keyStore;
    private final String keystorePassword;
    private final String entryPassword;

    public KeyStoreCredentialAliasResolver(String keystorePath, String keystoreType, String keystorePassword, String entryPassword)
    {
        requireNonNull(keystorePath, "keystorePath is null");
        requireNonNull(keystoreType, "keystoreType is null");
        requireNonNull(keystorePassword, "keystorePassword is null");
        requireNonNull(entryPassword, "entryPassword is null");

        try {
            this.keyStore = KeyStoreUtils.loadKeyStore(keystoreType, keystorePath, keystorePassword);
        }
        catch (Exception e) {
            throw new TrinoException(CONFIGURATION_INVALID, "Failed to load keystore from " + keystorePath, e);
        }
        this.keystorePassword = keystorePassword;
        this.entryPassword = entryPassword;
    }

    public String resolveAlias(String alias)
    {
        alias = alias.toLowerCase(Locale.US);
        try {
            if (!keyStore.containsAlias(alias)) {
                throw new TrinoException(CONFIGURATION_INVALID, "Unknown credential alias: " + alias);
            }
            return KeyStoreUtils.readEntity(keyStore, alias, keystorePassword, entryPassword);
        }
        catch (Exception e) {
            if (e instanceof TrinoException trinoException) {
                throw trinoException;
            }
            throw new TrinoException(CONFIGURATION_INVALID, "Failed to resolve credential alias: " + alias, e);
        }
    }

    public BucketCredentials resolveBucketCredentials(String bucketKeyPrefix, String bucket)
    {
        requireNonNull(bucketKeyPrefix, "bucketKeyPrefix is null");
        requireNonNull(bucket, "bucket is null");
        String accessKeyAlias = bucketKeyPrefix + bucket + ".access.key";
        String secretKeyAlias = bucketKeyPrefix + bucket + ".secret.key";
        return new BucketCredentials(resolveAlias(accessKeyAlias), resolveAlias(secretKeyAlias));
    }

    public record BucketCredentials(String accessKey, String secretKey) {}
}
