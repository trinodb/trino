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

import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3CredentialsMapper;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3SecurityMappingResult;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ENDPOINT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_REGION_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

final class IcebergVendedCredentialsMapper
        implements S3CredentialsMapper
{
    private final Optional<String> staticRegion;
    private final Optional<String> staticEndpoint;
    private final boolean staticCrossRegionAccessEnabled;

    @Inject
    public IcebergVendedCredentialsMapper(S3FileSystemConfig config)
    {
        requireNonNull(config, "config is null");
        this.staticRegion = Optional.ofNullable(config.getRegion());
        this.staticEndpoint = Optional.ofNullable(config.getEndpoint());
        this.staticCrossRegionAccessEnabled = config.isCrossRegionAccessEnabled();
    }

    @Override
    public Optional<S3SecurityMappingResult> getMapping(ConnectorIdentity identity, Location location)
    {
        String accessKey = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY);
        String secretKey = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY);
        String sessionToken = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY);

        if (accessKey == null || secretKey == null || sessionToken == null) {
            return Optional.empty();
        }

        String vendedRegion = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_REGION_PROPERTY);
        String vendedEndpoint = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY);
        String vendedCrossRegionAccess = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY);

        Optional<String> region = Optional.ofNullable(vendedRegion).or(() -> staticRegion);
        Optional<String> endpoint = Optional.ofNullable(vendedEndpoint).or(() -> staticEndpoint);

        Optional<Boolean> crossRegionAccessEnabled;
        if (staticCrossRegionAccessEnabled) {
            crossRegionAccessEnabled = Optional.of(true);
        }
        else if (vendedCrossRegionAccess != null) {
            crossRegionAccessEnabled = Optional.of(Boolean.parseBoolean(vendedCrossRegionAccess));
        }
        else {
            crossRegionAccessEnabled = Optional.empty();
        }

        return Optional.of(new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create(accessKey, secretKey, sessionToken)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                endpoint,
                region,
                crossRegionAccessEnabled));
    }
}
