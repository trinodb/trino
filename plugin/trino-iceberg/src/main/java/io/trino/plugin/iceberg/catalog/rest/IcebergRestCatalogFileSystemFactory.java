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
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;

import java.util.Map;
import java.util.Map.Entry;

import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
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
        if (vendedCredentialsEnabled) {
            ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.builder();

            // handle s3
            if (fileIoProperties.containsKey(S3FileIOProperties.ACCESS_KEY_ID) &&
                    fileIoProperties.containsKey(S3FileIOProperties.SECRET_ACCESS_KEY) &&
                    fileIoProperties.containsKey(S3FileIOProperties.SESSION_TOKEN)) {
                extraCredentialsBuilder
                        .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, fileIoProperties.get(S3FileIOProperties.ACCESS_KEY_ID))
                        .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, fileIoProperties.get(S3FileIOProperties.SECRET_ACCESS_KEY))
                        .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, fileIoProperties.get(S3FileIOProperties.SESSION_TOKEN));
            }

            // handle gcs
            if (fileIoProperties.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
                extraCredentialsBuilder.put(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, fileIoProperties.get(GCPProperties.GCS_OAUTH2_TOKEN));
                addOptionalProperty(extraCredentialsBuilder, fileIoProperties, GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY);
                addOptionalProperty(extraCredentialsBuilder, fileIoProperties, GCPProperties.GCS_PROJECT_ID, EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY);
            }

            // handle azure
            for (Entry<String, String> entry : fileIoProperties.entrySet()) {
                if (entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)) {
                    String account = entry.getKey().substring(AzureProperties.ADLS_SAS_TOKEN_PREFIX.length());
                    extraCredentialsBuilder.put(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + account, entry.getValue());
                }
            }

            Map<String, String> extraCredentials = extraCredentialsBuilder.buildOrThrow();
            if (!extraCredentials.isEmpty()) {
                ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                        .withGroups(identity.getGroups())
                        .withPrincipal(identity.getPrincipal())
                        .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                        .withConnectorRole(identity.getConnectorRole())
                        .withExtraCredentials(extraCredentials)
                        .build();
                return fileSystemFactory.create(identityWithExtraCredentials);
            }
        }

        return fileSystemFactory.create(identity);
    }

    private static void addOptionalProperty(
            ImmutableMap.Builder<String, String> builder,
            Map<String, String> fileIoProperties,
            String vendedKey,
            String extraCredentialKey)
    {
        String value = fileIoProperties.get(vendedKey);
        if (value != null) {
            builder.put(extraCredentialKey, value);
        }
    }
}
