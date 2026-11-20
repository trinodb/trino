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

import com.google.common.annotations.VisibleForTesting;
import io.trino.filesystem.TrinoFileSystemException;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Reuses credentials only while they cover the requested presigned URI lifetime.
 * Refreshes are serialized so concurrent signing requests can share one STS call.
 */
final class S3PresigningCredentials
{
    private final AwsCredentialsProvider provider;
    private final Clock clock;
    private Optional<AwsCredentials> cached = Optional.empty();

    public S3PresigningCredentials(AwsCredentialsProvider provider)
    {
        this(provider, Clock.systemUTC());
    }

    @VisibleForTesting
    S3PresigningCredentials(AwsCredentialsProvider provider, Clock clock)
    {
        this.provider = requireNonNull(provider, "provider is null");
        this.clock = requireNonNull(clock, "clock is null");
    }

    public synchronized AwsCredentials resolve(Duration requiredTtl)
            throws IOException
    {
        if (cached.isPresent() && isValidAtLeastUntil(cached.orElseThrow(), clock.instant().plus(requiredTtl))) {
            return cached.orElseThrow();
        }

        AwsCredentials credentials = provider.resolveCredentials();
        if (!isValidAtLeastUntil(credentials, clock.instant().plus(requiredTtl))) {
            // Temporary credentials without an expiration cannot provide a lifetime guarantee.
            requireKnownExpiration(credentials);
            credentials = refresh();
        }
        validate(credentials, clock.instant().plus(requiredTtl));
        // Credentials without expiration, such as static keys, can be revoked at any time without notice, so don't reuse them.
        cached = credentials.expirationTime().isPresent() ? Optional.of(credentials) : Optional.empty();
        return credentials;
    }

    private AwsCredentials refresh()
    {
        // SDK providers have no explicit refresh operation. A new provider with the same settings
        // bypasses its cache without changing identity, role, token file, or STS client.
        switch (provider) {
            case StsAssumeRoleCredentialsProvider role -> {
                try (var refreshed = role.toBuilder().asyncCredentialUpdateEnabled(false).build()) {
                    return refreshed.resolveCredentials();
                }
            }
            case WebIdentityTokenFileCredentialsProvider webIdentity -> {
                try (var refreshed = webIdentity.toBuilder().asyncCredentialUpdateEnabled(false).build()) {
                    return refreshed.resolveCredentials();
                }
            }
            case DefaultCredentialsProvider defaults -> {
                try (var refreshed = defaults.toBuilder().asyncCredentialUpdateEnabled(false).build()) {
                    return refreshed.resolveCredentials();
                }
            }
            default -> {
                // Other providers may refresh on resolution.
                return provider.resolveCredentials();
            }
        }
    }

    static void validate(AwsCredentials credentials, Instant uriExpiration)
            throws IOException
    {
        requireKnownExpiration(credentials);
        if (!isValidAtLeastUntil(credentials, uriExpiration)) {
            throw new TrinoFileSystemException("S3 signing credentials do not cover the requested URI lifetime. " +
                    "Use a shorter URI TTL or configure a longer role session duration");
        }
    }

    private static boolean isValidAtLeastUntil(AwsCredentials credentials, Instant uriExpiration)
    {
        if (credentials.expirationTime().isPresent()) {
            return !credentials.expirationTime().orElseThrow().isBefore(uriExpiration);
        }
        // Session credentials without a known expiration may expire before the URI; other types (e.g. static keys) do not expire
        return !(credentials instanceof AwsSessionCredentialsIdentity);
    }

    private static void requireKnownExpiration(AwsCredentials credentials)
            throws IOException
    {
        if (credentials instanceof AwsSessionCredentialsIdentity && credentials.expirationTime().isEmpty()) {
            throw new TrinoFileSystemException("Cannot guarantee S3 presigned URI lifetime: temporary signing credentials have no expiration time");
        }
    }
}
