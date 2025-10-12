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
import org.apache.iceberg.util.PropertyUtil;

import java.util.Map;

import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_SAS_TOKEN_PROPERTY_PREFIX;
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

    private static final String VENDED_ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";

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
            ImmutableMap.Builder<String, String> overriddenCredentialsBuilder = ImmutableMap.builder();

            if (fileIoProperties.containsKey(VENDED_S3_ACCESS_KEY) &&
                    fileIoProperties.containsKey(VENDED_S3_SECRET_KEY) &&
                    fileIoProperties.containsKey(VENDED_S3_SESSION_TOKEN)) {
                // S3 vended credentials
                overriddenCredentialsBuilder
                        .put(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY, fileIoProperties.get(VENDED_S3_ACCESS_KEY))
                        .put(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY, fileIoProperties.get(VENDED_S3_SECRET_KEY))
                        .put(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY, fileIoProperties.get(VENDED_S3_SESSION_TOKEN));
            }
            else {
                // Azure vended credentials
                overriddenCredentialsBuilder.putAll(getAzureCredentials(fileIoProperties));
            }

            Map<String, String> overriddenCredentials = overriddenCredentialsBuilder.buildOrThrow();
            if (!overriddenCredentials.isEmpty()) {
                // Do not include original credentials as they should not be used in vended mode
                ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity
                        .forUser(identity.getUser())
                        .withGroups(identity.getGroups())
                        .withPrincipal(identity.getPrincipal())
                        .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                        .withConnectorRole(identity.getConnectorRole())
                        .withExtraCredentials(overriddenCredentials).build();

                return fileSystemFactory.create(identityWithExtraCredentials);
            }
        }

        return fileSystemFactory.create(identity);
    }

    private static Map<String, String> getAzureCredentials(Map<String, String> fileIoProperties)
    {
        ImmutableMap.Builder<String, String> azureCredentialBuilder = ImmutableMap.builder();

        PropertyUtil.propertiesWithPrefix(fileIoProperties, VENDED_ADLS_SAS_TOKEN_PREFIX)
                .forEach((host, token) -> {
                    String storageAccount = host.contains(".") ? host.substring(0, host.indexOf('.')) : host;

                    if (!storageAccount.isEmpty() && !token.isEmpty()) {
                        azureCredentialBuilder.put(EXTRA_SAS_TOKEN_PROPERTY_PREFIX + storageAccount, token);
                    }
                });

        return azureCredentialBuilder.build();
    }
}
