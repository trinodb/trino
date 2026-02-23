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
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;

import java.util.Map;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ENDPOINT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_REGION_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
    private static final String VENDED_S3_ACCESS_KEY = "s3.access-key-id";
    private static final String VENDED_S3_SECRET_KEY = "s3.secret-access-key";
    private static final String VENDED_S3_SESSION_TOKEN = "s3.session-token";
    private static final String VENDED_CLIENT_REGION = "client.region";
    private static final String VENDED_S3_ENDPOINT = "s3.endpoint";
    private static final String VENDED_S3_CROSS_REGION_ACCESS_ENABLED = "s3.cross-region-access-enabled";

    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean vendedCredentialsEnabled;

    @Inject
    public IcebergRestCatalogFileSystemFactory(TrinoFileSystemFactory fileSystemFactory, IcebergRestCatalogConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.vendedCredentialsEnabled = config.isVendedCredentialsEnabled();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        if (!vendedCredentialsEnabled) {
            return fileSystemFactory.create(identity);
        }

        String accessKey = fileIoProperties.get(VENDED_S3_ACCESS_KEY);
        String secretKey = fileIoProperties.get(VENDED_S3_SECRET_KEY);
        String sessionToken = fileIoProperties.get(VENDED_S3_SESSION_TOKEN);

        if (accessKey == null || secretKey == null || sessionToken == null) {
            return fileSystemFactory.create(identity);
        }

        ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.<String, String>builder()
                .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, accessKey)
                .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, secretKey)
                .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, sessionToken);

        String region = fileIoProperties.get(VENDED_CLIENT_REGION);
        if (region != null) {
            extraCredentialsBuilder.put(EXTRA_CREDENTIALS_REGION_PROPERTY, region);
        }

        String endpoint = fileIoProperties.get(VENDED_S3_ENDPOINT);
        if (endpoint != null) {
            extraCredentialsBuilder.put(EXTRA_CREDENTIALS_ENDPOINT_PROPERTY, endpoint);
        }

        String crossRegionAccessEnabled = fileIoProperties.get(VENDED_S3_CROSS_REGION_ACCESS_ENABLED);
        if (crossRegionAccessEnabled != null) {
            extraCredentialsBuilder.put(EXTRA_CREDENTIALS_CROSS_REGION_ACCESS_ENABLED_PROPERTY, crossRegionAccessEnabled);
        }

        ConnectorIdentity identityWithVendedCredentials = ConnectorIdentity.forUser(identity.getUser())
                .withGroups(identity.getGroups())
                .withPrincipal(identity.getPrincipal())
                .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                .withConnectorRole(identity.getConnectorRole())
                .withExtraCredentials(extraCredentialsBuilder.buildOrThrow())
                .build();

        return fileSystemFactory.create(identityWithVendedCredentials);
    }
}
