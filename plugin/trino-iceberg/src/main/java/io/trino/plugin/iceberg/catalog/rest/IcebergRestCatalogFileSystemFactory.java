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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.IcebergStorageCredentials;
import io.trino.plugin.iceberg.IcebergTableCredentials;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;

import java.util.Comparator;
import java.util.List;
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

    @VisibleForTesting
    void flushVendedCredentialsCache()
    {
        vendedCredentialsProvidersCache.invalidateAll();
    }

    // Used by ioBuilder function in the RESTSessionCatalog
    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, Map<String, String> fileIoProperties)
    {
        if (vendedCredentialsEnabled) {
            return new IcebergRestCatalogFileSystem(new IcebergRestCatalogFileSystemLoader()
            {
                @Override
                public TrinoFileSystem create(Location location)
                {
                    if (location.scheme().isEmpty()) {
                        throw new IllegalArgumentException("Location scheme is empty: " + location);
                    }
                    CachedVendedCredentialsProviders cached = uncheckedCacheGet(
                            vendedCredentialsProvidersCache,
                            createVendedCredentialsCacheKey(identity, fileIoProperties, this.storageCredentials),
                            () -> createVendedCredentialsProviders(fileIoProperties, this.storageCredentials));

                    return getTrinoFileSystem(location, identity, cached);
                }
            });
        }
        return fileSystemFactory.create(identity);
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity, IcebergTableCredentials tableCredentials)
    {
        if (vendedCredentialsEnabled) {
            return new IcebergRestCatalogFileSystem(new IcebergRestCatalogFileSystemLoader()
            {
                @Override
                public TrinoFileSystem create(Location location)
                {
                    if (location.scheme().isEmpty()) {
                        throw new IllegalArgumentException("Location scheme is empty: " + location);
                    }
                    CachedVendedCredentialsProviders cached = uncheckedCacheGet(
                            vendedCredentialsProvidersCache,
                            createVendedCredentialsCacheKey(identity, tableCredentials.fileIoProperties(), tableCredentials.storageCredentials()),
                            () -> createVendedCredentialsProviders(tableCredentials.fileIoProperties(), tableCredentials.storageCredentials()));

                    return getTrinoFileSystem(location, identity, cached);
                }
            });
        }
        return fileSystemFactory.create(identity);
    }

    private TrinoFileSystem getTrinoFileSystem(Location location, ConnectorIdentity identity, CachedVendedCredentialsProviders cached)
    {
        // Derive the vended credentials to load from the location scheme
        Optional<VendedCredentials> vendedCredentials = switch (location.scheme().get()) {
            case "s3", "s3a", "s3n" -> findVendedCredentialsForLocation(cached.s3VendedCredentialsProviders(), location);
            case "gs" -> findVendedCredentialsForLocation(cached.gcsVendedCredentialsProviders(), location);
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
    }

    private static VendedCredentialsCacheKey createVendedCredentialsCacheKey(ConnectorIdentity identity, Map<String, String> fileIoProperties, List<IcebergStorageCredentials> storageCredentials)
    {
        return new VendedCredentialsCacheKey(
                identity.getUser(),
                createS3VendedCredentials(fileIoProperties, storageCredentials),
                createGcsVendedCredentials(fileIoProperties, storageCredentials),
                createAzureVendedCredentials(fileIoProperties));
    }

    private static Map<String, S3VendedCredentials> createS3VendedCredentials(Map<String, String> fileIoProperties, List<IcebergStorageCredentials> storageCredentialsList)
    {
        ImmutableMap.Builder<String, S3VendedCredentials> builder = ImmutableMap.builder();
        // Root-prefix entries built from fileIoProperties — act as catch-all fallback, matching any S3 path
        createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> S3VendedCredentialsProvider.createVendedCredentials(fileIoProperties),
                S3FileIOProperties.ACCESS_KEY_ID,
                S3FileIOProperties.SECRET_ACCESS_KEY,
                S3FileIOProperties.SESSION_TOKEN)
                .ifPresent(vendedCredentials -> builder.put("s3", vendedCredentials));

        for (IcebergStorageCredentials storageCredentials : storageCredentialsList) {
            if (isS3Prefix(storageCredentials.prefix())) {
                builder.put(
                        storageCredentials.prefix(),
                        S3VendedCredentialsProvider.createVendedCredentials(mergeConfigs(fileIoProperties, storageCredentials.config())));
            }
        }
        return builder.buildKeepingLast();
    }

    private static Map<String, GcsVendedCredentials> createGcsVendedCredentials(Map<String, String> fileIoProperties, List<IcebergStorageCredentials> storageCredentialsList)
    {
        ImmutableMap.Builder<String, GcsVendedCredentials> builder = ImmutableMap.builder();
        // Root-prefix entries built from fileIoProperties — act as catch-all fallback, matching any GCS path
        createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> GcsVendedCredentialsProvider.createVendedCredentials(fileIoProperties),
                GCPProperties.GCS_OAUTH2_TOKEN)
                .ifPresent(vendedCredentials -> builder.put("gs", vendedCredentials));

        for (IcebergStorageCredentials storageCredentials : storageCredentialsList) {
            if (isGcsPrefix(storageCredentials.prefix())) {
                builder.put(
                        storageCredentials.prefix(),
                        GcsVendedCredentialsProvider.createVendedCredentials(mergeConfigs(fileIoProperties, storageCredentials.config())));
            }
        }
        return builder.buildKeepingLast();
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

    private CachedVendedCredentialsProviders createVendedCredentialsProviders(Map<String, String> fileIoProperties, List<IcebergStorageCredentials> storageCredentialsList)
    {
        ImmutableMap.Builder<String, S3VendedCredentialsProvider> s3ProvidersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, GcsVendedCredentialsProvider> gcsProvidersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.builder();

        // Root-prefix providers built from fileIoProperties — act as catch-all fallback, matching any S3/GCS path
        createVendedCredentialsProviderIfPresent(
                fileIoProperties,
                () -> new S3VendedCredentialsProvider(catalogProperties, fileIoProperties),
                S3FileIOProperties.ACCESS_KEY_ID,
                S3FileIOProperties.SECRET_ACCESS_KEY,
                S3FileIOProperties.SESSION_TOKEN)
                .ifPresent(vendedCredentialsProvider -> s3ProvidersBuilder.put("s3", vendedCredentialsProvider));
        createVendedCredentialsProviderIfPresent(
                fileIoProperties,
                () -> new GcsVendedCredentialsProvider(catalogProperties, fileIoProperties),
                GCPProperties.GCS_OAUTH2_TOKEN)
                .ifPresent(vendedCredentialsProvider -> gcsProvidersBuilder.put("gs", vendedCredentialsProvider));
        if (fileIoProperties.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, GCPProperties.GCS_PROJECT_ID, EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY);
        }

        // Per-prefix providers built from merged config (storage credential config overrides fileIoProperties)
        for (IcebergStorageCredentials storageCredentials : storageCredentialsList) {
            String credentialPrefix = storageCredentials.prefix();
            if (isS3Prefix(credentialPrefix)) {
                s3ProvidersBuilder.put(credentialPrefix, new S3VendedCredentialsProvider(catalogProperties, mergeConfigs(fileIoProperties, storageCredentials.config())));
            }
            else if (isGcsPrefix(credentialPrefix)) {
                gcsProvidersBuilder.put(credentialPrefix, new GcsVendedCredentialsProvider(catalogProperties, mergeConfigs(fileIoProperties, storageCredentials.config())));
            }
        }

        // Azure does not make use of SupportsStorageCredentials mechanism; always from fileIoProperties
        Optional<AzureVendedCredentialsProvider> azureVendedCredentialsProvider = createAzureVendedCredentialsProvider(fileIoProperties);

        return new CachedVendedCredentialsProviders(
                s3ProvidersBuilder.buildKeepingLast(),
                gcsProvidersBuilder.buildKeepingLast(),
                azureVendedCredentialsProvider,
                extraCredentialsBuilder.buildOrThrow());
    }

    private record VendedCredentialsCacheKey(
            String user,
            Map<String, S3VendedCredentials> s3VendedCredentials,
            Map<String, GcsVendedCredentials> gcsVendedCredentials,
            Optional<AzureVendedCredentials> azureVendedCredentials)
    {
        public VendedCredentialsCacheKey
        {
            requireNonNull(user, "user is null");
            s3VendedCredentials = ImmutableMap.copyOf(s3VendedCredentials);
            gcsVendedCredentials = ImmutableMap.copyOf(gcsVendedCredentials);
            requireNonNull(azureVendedCredentials, "azureVendedCredentials is null");
        }
    }

    private record CachedVendedCredentialsProviders(
            Map<String, S3VendedCredentialsProvider> s3VendedCredentialsProviders,
            Map<String, GcsVendedCredentialsProvider> gcsVendedCredentialsProviders,
            Optional<AzureVendedCredentialsProvider> azureVendedCredentialsProvider,
            Map<String, String> extraCredentials)
    {
        public CachedVendedCredentialsProviders
        {
            s3VendedCredentialsProviders = ImmutableMap.copyOf(s3VendedCredentialsProviders);
            gcsVendedCredentialsProviders = ImmutableMap.copyOf(gcsVendedCredentialsProviders);
            requireNonNull(azureVendedCredentialsProvider, "azureVendedCredentialsProvider is null");
            extraCredentials = ImmutableMap.copyOf(extraCredentials);
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

    private static <V extends VendedCredentials, P extends VendedCredentialsProvider<V>> Optional<VendedCredentials> findVendedCredentialsForLocation(Map<String, P> providers, Location location)
    {
        return providers.entrySet().stream()
                .filter(e -> location.toString().startsWith(e.getKey()))
                .max(Comparator.comparingInt(e -> e.getKey().length()))
                .map(e -> e.getValue().getCredentials());
    }

    private static boolean isS3Prefix(String prefix)
    {
        return prefix.startsWith("s3://") || prefix.startsWith("s3a://") || prefix.startsWith("s3n://");
    }

    private static boolean isGcsPrefix(String prefix)
    {
        return prefix.startsWith("gs://");
    }

    private static Map<String, String> mergeConfigs(Map<String, String> base, Map<String, String> override)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(base)
                .putAll(override)
                .buildKeepingLast();
    }
}
