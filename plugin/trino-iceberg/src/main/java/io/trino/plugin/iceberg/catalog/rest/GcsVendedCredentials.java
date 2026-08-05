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

import com.google.common.collect.ImmutableMap;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

record GcsVendedCredentials(String token, Optional<Instant> expirationTime)
        implements VendedCredentials
{
    public GcsVendedCredentials
    {
        requireNonNull(token, "token is null");
        requireNonNull(expirationTime, "expirationTime is null");
    }

    @Override
    public Map<String, String> toExtraCredentials()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, token);
        expirationTime.ifPresent(expirationTime -> builder.put(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY, String.valueOf(expirationTime.toEpochMilli())));
        return builder.buildOrThrow();
    }

    @Override
    public Optional<Instant> expiresAt()
    {
        return expirationTime;
    }
}
