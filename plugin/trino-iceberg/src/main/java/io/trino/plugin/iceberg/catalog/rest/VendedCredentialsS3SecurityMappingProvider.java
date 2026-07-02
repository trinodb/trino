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

import io.trino.filesystem.Location;
import io.trino.filesystem.s3.S3SecurityMappingProvider;
import io.trino.filesystem.s3.S3SecurityMappingResult;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ENDPOINT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_REGION_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;

final class VendedCredentialsS3SecurityMappingProvider
        implements S3SecurityMappingProvider
{
    @Override
    public Optional<S3SecurityMappingResult> getMapping(ConnectorIdentity identity, Location location)
    {
        String accessKey = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY);
        String secretKey = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY);
        String sessionToken = identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY);

        if (accessKey == null || secretKey == null || sessionToken == null) {
            return Optional.empty();
        }

        return Optional.of(new S3SecurityMappingResult(
                Optional.of(AwsSessionCredentials.create(accessKey, secretKey, sessionToken)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.ofNullable(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY)),
                Optional.ofNullable(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_REGION_PROPERTY)),
                Optional.ofNullable(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY)).map(value -> parseStrictBoolean(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, value)),
                Optional.ofNullable(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY)).map(value -> parseStrictBoolean(EXTRA_CREDENTIALS_PATH_STYLE_ACCESS_PROPERTY, value))));
    }

    private static boolean parseStrictBoolean(String propertyName, String value)
    {
        if (value.equalsIgnoreCase("true")) {
            return true;
        }
        if (value.equalsIgnoreCase("false")) {
            return false;
        }
        throw new TrinoException(ICEBERG_CATALOG_ERROR, "Invalid value for %s: %s".formatted(propertyName, value));
    }
}
