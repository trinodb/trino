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
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static java.util.Objects.requireNonNull;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean vendedCredentialsEnabled;
    private final Map<String, String> catalogProperties;

    @Inject
    public IcebergRestCatalogFileSystemFactory(
            TrinoFileSystemFactory fileSystemFactory,
            IcebergRestCatalogConfig config,
            IcebergRestCatalogPropertiesProvider catalogPropertiesProvider)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.vendedCredentialsEnabled = config.isVendedCredentialsEnabled();
        this.catalogProperties = ImmutableMap.copyOf(catalogPropertiesProvider.catalogProperties());
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        if (vendedCredentialsEnabled) {
            Optional<S3VendedCredentialsProvider> s3VendedCredentialsProvider = createVendedCredentialsProviderIfPresent(
                    fileIoProperties,
                    () -> new S3VendedCredentialsProvider(catalogProperties, fileIoProperties),
                    S3FileIOProperties.ACCESS_KEY_ID, S3FileIOProperties.SECRET_ACCESS_KEY, S3FileIOProperties.SESSION_TOKEN);
            Optional<GcsVendedCredentialsProvider> gcsVendedCredentialsProvider = createVendedCredentialsProviderIfPresent(
                    fileIoProperties,
                    () -> new GcsVendedCredentialsProvider(catalogProperties, fileIoProperties),
                    GCPProperties.GCS_OAUTH2_TOKEN);
            Optional<AzureVendedCredentialsProvider> azureVendedCredentialsProvider = createAzureVendedCredentialsProvider(fileIoProperties);

            ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.builder();
            // handle GCS
            if (fileIoProperties.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
                addOptionalProperty(extraCredentialsBuilder, fileIoProperties, GCPProperties.GCS_PROJECT_ID, EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY);
            }
            Map<String, String> extraCredentials = extraCredentialsBuilder.buildOrThrow();

            if (s3VendedCredentialsProvider.isPresent() || gcsVendedCredentialsProvider.isPresent() || azureVendedCredentialsProvider.isPresent()) {
                return new IcebergRestCatalogFileSystem(
                        location -> {
                            if (location.scheme().isEmpty()) {
                                throw new IllegalArgumentException("Location scheme is empty: " + location);
                            }
                            // Derive the vended credentials to load from the location scheme
                            Optional<VendedCredentials> vendedCredentials = switch (location.scheme().get()) {
                                case "s3", "s3a", "s3n" -> s3VendedCredentialsProvider.map(S3VendedCredentialsProvider::getCredentials);
                                case "gs" -> gcsVendedCredentialsProvider.map(GcsVendedCredentialsProvider::getCredentials);
                                case "abfs", "abfss", "wasb", "wasbs" -> azureVendedCredentialsProvider.map(AzureVendedCredentialsProvider::getCredentials);
                                default -> throw new IllegalArgumentException("Unsupported location scheme for vended credentials: " + location);
                            };
                            if (vendedCredentials.isEmpty()) {
                                throw new IllegalStateException("Failed to initialize the vended credentials from the provided fileIoProperties");
                            }

                            ConnectorIdentity identityWithExtraCredentials = ConnectorIdentity.forUser(identity.getUser())
                                    .withGroups(identity.getGroups())
                                    .withPrincipal(identity.getPrincipal())
                                    .withEnabledSystemRoles(identity.getEnabledSystemRoles())
                                    .withConnectorRole(identity.getConnectorRole())
                                    .withExtraCredentials(ImmutableMap.<String, String>builder()
                                            .putAll(extraCredentials)
                                            .putAll(ImmutableMap.copyOf(vendedCredentials.map(VendedCredentials::toExtraCredentials).orElse(ImmutableMap.of())))
                                            .buildOrThrow())
                                    .build();

                            return fileSystemFactory.create(identityWithExtraCredentials);
                        });
            }
        }
        return fileSystemFactory.create(identity);
    }

    private static <T> Optional<T> createVendedCredentialsProviderIfPresent(
            Map<String, String> fileIoProperties,
            Supplier<T> providerFactory,
            String... requiredKeys)
    {
        for (String key : requiredKeys) {
            if (!fileIoProperties.containsKey(key)) {
                return Optional.empty();
            }
        }
        return Optional.of(providerFactory.get());
    }

    private Optional<AzureVendedCredentialsProvider> createAzureVendedCredentialsProvider(Map<String, String> fileIoProperties)
    {
        for (String key : fileIoProperties.keySet()) {
            if (key.startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)) {
                return Optional.of(new AzureVendedCredentialsProvider(catalogProperties, fileIoProperties));
            }
        }
        return Optional.empty();
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
