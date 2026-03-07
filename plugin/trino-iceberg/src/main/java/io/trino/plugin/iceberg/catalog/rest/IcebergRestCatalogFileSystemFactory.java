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

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
    private static final String VENDED_S3_ACCESS_KEY = "s3.access-key-id";
    private static final String VENDED_S3_SECRET_KEY = "s3.secret-access-key";
    private static final String VENDED_S3_SESSION_TOKEN = "s3.session-token";

    private static final String VENDED_GCS_OAUTH_TOKEN = "gcs.oauth2.token";
    private static final String VENDED_GCS_OAUTH_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";
    private static final String VENDED_GCS_PROJECT_ID = "gcs.project-id";
    private static final String VENDED_GCS_SERVICE_HOST = "gcs.service.host";
    private static final String VENDED_GCS_NO_AUTH = "gcs.no-auth";
    private static final String VENDED_GCS_USER_PROJECT = "gcs.user-project";

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
        if (vendedCredentialsEnabled &&
                fileIoProperties.containsKey(VENDED_S3_ACCESS_KEY) &&
                fileIoProperties.containsKey(VENDED_S3_SECRET_KEY) &&
                fileIoProperties.containsKey(VENDED_S3_SESSION_TOKEN)) {
            // Do not include original credentials as they should not be used in vended mode
            ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                    .withGroups(identity.getGroups())
                    .withPrincipal(identity.getPrincipal())
                    .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                    .withConnectorRole(identity.getConnectorRole())
                    .withExtraCredentials(ImmutableMap.<String, String>builder()
                            .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, fileIoProperties.get(VENDED_S3_ACCESS_KEY))
                            .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, fileIoProperties.get(VENDED_S3_SECRET_KEY))
                            .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, fileIoProperties.get(VENDED_S3_SESSION_TOKEN))
                            .buildOrThrow())
                    .build();
            return fileSystemFactory.create(identityWithExtraCredentials);
        }

        if (vendedCredentialsEnabled && hasAnyGcsVendedProperty(fileIoProperties)) {
            ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.builder();

            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, VENDED_GCS_OAUTH_TOKEN, EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY);
            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, VENDED_GCS_OAUTH_TOKEN_EXPIRES_AT, EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY);
            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, VENDED_GCS_PROJECT_ID, EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY);
            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, VENDED_GCS_SERVICE_HOST, EXTRA_CREDENTIALS_GCS_SERVICE_HOST_PROPERTY);
            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, VENDED_GCS_NO_AUTH, EXTRA_CREDENTIALS_GCS_NO_AUTH_PROPERTY);
            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, VENDED_GCS_USER_PROJECT, EXTRA_CREDENTIALS_GCS_USER_PROJECT_PROPERTY);
            ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                    .withGroups(identity.getGroups())
                    .withPrincipal(identity.getPrincipal())
                    .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                    .withConnectorRole(identity.getConnectorRole())
                    .withExtraCredentials(extraCredentialsBuilder.buildOrThrow())
                    .build();
            return fileSystemFactory.create(identityWithExtraCredentials);
        }

        return fileSystemFactory.create(identity);
    }

    private static boolean hasAnyGcsVendedProperty(Map<String, String> fileIoProperties)
    {
        return fileIoProperties.containsKey(VENDED_GCS_OAUTH_TOKEN) ||
                fileIoProperties.containsKey(VENDED_GCS_NO_AUTH) ||
                fileIoProperties.containsKey(VENDED_GCS_PROJECT_ID) ||
                fileIoProperties.containsKey(VENDED_GCS_SERVICE_HOST) ||
                fileIoProperties.containsKey(VENDED_GCS_USER_PROJECT);
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
