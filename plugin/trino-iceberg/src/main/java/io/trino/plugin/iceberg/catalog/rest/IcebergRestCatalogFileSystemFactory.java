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

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
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

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final boolean vendedCredentialsEnabled;
    private final Map<String, String> catalogProperties;
    private final Cache<VendedCredentialsCacheKey, CachedVendedCredentialsProviders> vendedCredentialsProvidersCache = EvictableCacheBuilder.newBuilder()
            .maximumSize(1_000)
            .expireAfterWrite(1, HOURS)
            .build();

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
            CachedVendedCredentialsProviders cached = uncheckedCacheGet(
                    vendedCredentialsProvidersCache,
                    createVendedCredentialsCacheKey(identity, fileIoProperties),
                    () -> createVendedCredentialsProviders(fileIoProperties));

            if (cached.s3VendedCredentialsProvider().isPresent() || cached.gcsVendedCredentialsProvider().isPresent() || cached.azureVendedCredentialsProvider().isPresent()) {
                return new IcebergRestCatalogFileSystem(
                        location -> {
                            if (location.scheme().isEmpty()) {
                                throw new IllegalArgumentException("Location scheme is empty: " + location);
                            }
                            // Derive the vended credentials to load from the location scheme
                            Optional<VendedCredentials> vendedCredentials = switch (location.scheme().get()) {
                                case "s3", "s3a", "s3n" -> cached.s3VendedCredentialsProvider().map(S3VendedCredentialsProvider::getCredentials);
                                case "gs" -> cached.gcsVendedCredentialsProvider().map(GcsVendedCredentialsProvider::getCredentials);
                                case "abfs", "abfss", "wasb", "wasbs" -> cached.azureVendedCredentialsProvider().map(AzureVendedCredentialsProvider::getCredentials);
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
                                            .putAll(cached.extraCredentials())
                                            .putAll(ImmutableMap.copyOf(vendedCredentials.map(VendedCredentials::toExtraCredentials).orElse(ImmutableMap.of())))
                                            .buildOrThrow())
                                    .build();

                            return fileSystemFactory.create(identityWithExtraCredentials);
                        });
            }
        }
        return fileSystemFactory.create(identity);
    }

    private static VendedCredentialsCacheKey createVendedCredentialsCacheKey(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        Optional<S3VendedCredentials> s3VendedCredentials = createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> S3VendedCredentialsProvider.createVendedCredentials(fileIoProperties),
                S3FileIOProperties.ACCESS_KEY_ID, S3FileIOProperties.SECRET_ACCESS_KEY, S3FileIOProperties.SESSION_TOKEN);
        Optional<GcsVendedCredentials> gcsVendedCredentials = createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> GcsVendedCredentialsProvider.createVendedCredentials(fileIoProperties),
                GCPProperties.GCS_OAUTH2_TOKEN);
        Optional<AzureVendedCredentials> azureVendedCredentials = createAzureVendedCredentials(fileIoProperties);

        return new VendedCredentialsCacheKey(identity.getUser(), s3VendedCredentials, gcsVendedCredentials, azureVendedCredentials);
    }

    private static <T> Optional<T> createVendedCredentialsIfPresent(
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

    private static Optional<AzureVendedCredentials> createAzureVendedCredentials(Map<String, String> fileIoProperties)
    {
        for (String key : fileIoProperties.keySet()) {
            if (key.startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX)) {
                return Optional.of(AzureVendedCredentialsProvider.createVendedCredentials(fileIoProperties));
            }
        }
        return Optional.empty();
    }

    private CachedVendedCredentialsProviders createVendedCredentialsProviders(Map<String, String> fileIoProperties)
    {
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

        return new CachedVendedCredentialsProviders(
                s3VendedCredentialsProvider,
                gcsVendedCredentialsProvider,
                azureVendedCredentialsProvider,
                extraCredentialsBuilder.buildOrThrow());
    }

    private record VendedCredentialsCacheKey(
            String user,
            Optional<S3VendedCredentials> s3VendedCredentials,
            Optional<GcsVendedCredentials> gcsVendedCredentials,
            Optional<AzureVendedCredentials> azureVendedCredentials)
    {
        public VendedCredentialsCacheKey
        {
            requireNonNull(user, "user is null");
            requireNonNull(s3VendedCredentials, "s3VendedCredentials is null");
            requireNonNull(gcsVendedCredentials, "gcsVendedCredentials is null");
            requireNonNull(azureVendedCredentials, "azureVendedCredentials is null");
        }
    }

    private record CachedVendedCredentialsProviders(
            Optional<S3VendedCredentialsProvider> s3VendedCredentialsProvider,
            Optional<GcsVendedCredentialsProvider> gcsVendedCredentialsProvider,
            Optional<AzureVendedCredentialsProvider> azureVendedCredentialsProvider,
            Map<String, String> extraCredentials)
    {
        public CachedVendedCredentialsProviders
        {
            requireNonNull(s3VendedCredentialsProvider, "s3VendedCredentialsProvider is null");
            requireNonNull(gcsVendedCredentialsProvider, "gcsVendedCredentialsProvider is null");
            requireNonNull(azureVendedCredentialsProvider, "azureVendedCredentialsProvider is null");
            requireNonNull(extraCredentials, "extraCredentials is null");
        }
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
