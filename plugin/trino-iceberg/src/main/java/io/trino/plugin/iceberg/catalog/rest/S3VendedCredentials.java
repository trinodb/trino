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

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ENDPOINT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_REGION_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

record S3VendedCredentials(
        String accessKey,
        String secretKey,
        String sessionToken,
        Optional<Instant> expirationTime,
        Optional<String> region,
        Optional<String> endpoint,
        Optional<String> crossRegionAccessEnabled,
        Optional<String> pathStyleAccess)
        implements VendedCredentials
{
    public S3VendedCredentials
    {
        requireNonNull(accessKey, "accessKey is null");
        requireNonNull(secretKey, "secretKey is null");
        requireNonNull(sessionToken, "sessionToken is null");
        requireNonNull(expirationTime, "expirationTime is null");
        requireNonNull(region, "region is null");
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(crossRegionAccessEnabled, "crossRegionAccessEnabled is null");
        requireNonNull(pathStyleAccess, "pathStyleAccess is null");
    }

    @Override
    public Optional<Instant> expiresAt()
    {
        return expirationTime;
    }

    @Override
    public Map<String, String> toExtraCredentials()
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, accessKey)
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, secretKey)
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, sessionToken);
        region.ifPresent(value -> builder.put(EXTRA_CREDENTIALS_REGION_PROPERTY, value));
        endpoint.ifPresent(value -> builder.put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, value));
        crossRegionAccessEnabled.ifPresent(value -> builder.put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, value));
        pathStyleAccess.ifPresent(value -> builder.put(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY, value));
        return builder.buildOrThrow();
    }
}
