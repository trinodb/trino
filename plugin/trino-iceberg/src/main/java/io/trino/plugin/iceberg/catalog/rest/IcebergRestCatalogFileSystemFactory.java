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
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.aws.s3.S3FileIOProperties;

import java.util.Map;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.REMOTE_SIGNING_ENABLED;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean vendedCredentialsEnabled;

    @Inject
    public IcebergRestCatalogFileSystemFactory(TrinoFileSystemFactory fileSystemFactory, IcebergRestCatalogConfig icebergRestCatalogConfig)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.vendedCredentialsEnabled = icebergRestCatalogConfig.isVendedCredentialsEnabled();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        if ("true".equals(fileIoProperties.get(REMOTE_SIGNING_ENABLED))) {
            throw new TrinoException(NOT_SUPPORTED, "Remote signing is not supported");
        }
        if (vendedCredentialsEnabled
                && fileIoProperties.containsKey(S3FileIOProperties.ACCESS_KEY_ID)
                && fileIoProperties.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY)
                && fileIoProperties.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
            ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                    .withGroups(identity.getGroups())
                    .withPrincipal(identity.getPrincipal())
                    .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                    .withConnectorRole(identity.getConnectorRole())
                    .withExtraCredentials(ImmutableMap.of(
                            EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, fileIoProperties.get(S3FileIOProperties.ACCESS_KEY_ID),
                            EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, fileIoProperties.get(S3FileIOProperties.SECRET_ACCESS_KEY),
                            EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, fileIoProperties.get(S3FileIOProperties.SESSION_TOKEN)))
                    .build();
            return fileSystemFactory.create(identityWithExtraCredentials);
        }
        else {
            return fileSystemFactory.create(identity);
        }
    }
}
