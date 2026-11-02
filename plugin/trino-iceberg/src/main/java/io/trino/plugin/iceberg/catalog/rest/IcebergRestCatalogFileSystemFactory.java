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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

public class IcebergRestCatalogFileSystemFactory
        implements IcebergFileSystemFactory
{
    // Prefixes for the fallback credentials from fileIoProperties. Every location of a storage starts with its prefix,
    // e.g. "s3" for s3://, s3a:// and s3n:// locations, so the fallback matches any location, while longest-prefix
    // matching prefers a storage credential whose prefix also matches.
    private static final String S3_ROOT_PREFIX = "s3";
    private static final String GCS_ROOT_PREFIX = "gs";
    // abfs:// and abfss:// locations start with "abfs", wasb:// and wasbs:// locations with "wasb"
    private static final List<String> AZURE_ROOT_PREFIXES = ImmutableList.of("abfs", "wasb");

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
            case "abfs", "abfss", "wasb", "wasbs" -> findVendedCredentialsForLocation(cached.azureVendedCredentialsProviders(), location);
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
                createAzureVendedCredentials(fileIoProperties, storageCredentials));
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
                .ifPresent(vendedCredentials -> builder.put(S3_ROOT_PREFIX, vendedCredentials));

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
                .ifPresent(vendedCredentials -> builder.put(GCS_ROOT_PREFIX, vendedCredentials));

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
        return createVendedCredentialsIfPresent(fileIoProperties, providerFactory, properties -> Arrays.stream(requiredKeys).allMatch(properties::containsKey));
    }

    private static <T> Optional<T> createVendedCredentialsIfPresent(
            Map<String, String> fileIoProperties,
            Supplier<T> providerFactory,
            Predicate<Map<String, String>> containsVendedCredentials)
    {
        if (containsVendedCredentials.test(fileIoProperties)) {
            return Optional.of(providerFactory.get());
        }
        return Optional.empty();
    }

    private static Map<String, AzureVendedCredentials> createAzureVendedCredentials(Map<String, String> fileIoProperties, List<IcebergStorageCredentials> storageCredentialsList)
    {
        ImmutableMap.Builder<String, AzureVendedCredentials> builder = ImmutableMap.builder();
        // Root-prefix entries built from fileIoProperties — act as catch-all fallback, matching any Azure path
        createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> AzureVendedCredentialsProvider.createVendedCredentials(fileIoProperties),
                IcebergRestCatalogFileSystemFactory::containsSasToken)
                .ifPresent(vendedCredentials -> AZURE_ROOT_PREFIXES.forEach(prefix -> builder.put(prefix, vendedCredentials)));

        for (IcebergStorageCredentials storageCredentials : storageCredentialsList) {
            if (isAzurePrefix(storageCredentials.prefix())) {
                builder.put(
                        storageCredentials.prefix(),
                        AzureVendedCredentialsProvider.createVendedCredentials(mergeAzureConfigs(fileIoProperties, storageCredentials.config())));
            }
        }
        return builder.buildKeepingLast();
    }

    private CachedVendedCredentialsProviders createVendedCredentialsProviders(Map<String, String> fileIoProperties, List<IcebergStorageCredentials> storageCredentialsList)
    {
        ImmutableMap.Builder<String, S3VendedCredentialsProvider> s3ProvidersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, GcsVendedCredentialsProvider> gcsProvidersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, AzureVendedCredentialsProvider> azureProvidersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, String> extraCredentialsBuilder = ImmutableMap.builder();

        // Root-prefix providers built from fileIoProperties — act as catch-all fallback, matching any S3/GCS/Azure path
        createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> new S3VendedCredentialsProvider(catalogProperties, fileIoProperties),
                S3FileIOProperties.ACCESS_KEY_ID,
                S3FileIOProperties.SECRET_ACCESS_KEY,
                S3FileIOProperties.SESSION_TOKEN)
                .ifPresent(vendedCredentialsProvider -> s3ProvidersBuilder.put(S3_ROOT_PREFIX, vendedCredentialsProvider));
        createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> new GcsVendedCredentialsProvider(catalogProperties, fileIoProperties),
                GCPProperties.GCS_OAUTH2_TOKEN)
                .ifPresent(vendedCredentialsProvider -> gcsProvidersBuilder.put(GCS_ROOT_PREFIX, vendedCredentialsProvider));
        if (fileIoProperties.containsKey(GCPProperties.GCS_OAUTH2_TOKEN)) {
            addOptionalProperty(extraCredentialsBuilder, fileIoProperties, GCPProperties.GCS_PROJECT_ID, EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY);
        }
        createVendedCredentialsIfPresent(
                fileIoProperties,
                () -> new AzureVendedCredentialsProvider(catalogProperties, fileIoProperties),
                IcebergRestCatalogFileSystemFactory::containsSasToken)
                .ifPresent(vendedCredentialsProvider -> AZURE_ROOT_PREFIXES.forEach(prefix -> azureProvidersBuilder.put(prefix, vendedCredentialsProvider)));

        // Per-prefix providers built from merged config (storage credential config overrides fileIoProperties)
        for (IcebergStorageCredentials storageCredentials : storageCredentialsList) {
            String credentialPrefix = storageCredentials.prefix();
            if (isS3Prefix(credentialPrefix)) {
                s3ProvidersBuilder.put(credentialPrefix, new S3VendedCredentialsProvider(catalogProperties, mergeConfigs(fileIoProperties, storageCredentials.config())));
            }
            else if (isGcsPrefix(credentialPrefix)) {
                gcsProvidersBuilder.put(credentialPrefix, new GcsVendedCredentialsProvider(catalogProperties, mergeConfigs(fileIoProperties, storageCredentials.config())));
            }
            else if (isAzurePrefix(credentialPrefix)) {
                azureProvidersBuilder.put(credentialPrefix, new AzureVendedCredentialsProvider(catalogProperties, mergeAzureConfigs(fileIoProperties, storageCredentials.config())));
            }
        }

        return new CachedVendedCredentialsProviders(
                s3ProvidersBuilder.buildKeepingLast(),
                gcsProvidersBuilder.buildKeepingLast(),
                azureProvidersBuilder.buildKeepingLast(),
                extraCredentialsBuilder.buildOrThrow());
    }

    private record VendedCredentialsCacheKey(
            String user,
            Map<String, S3VendedCredentials> s3VendedCredentials,
            Map<String, GcsVendedCredentials> gcsVendedCredentials,
            Map<String, AzureVendedCredentials> azureVendedCredentials)
    {
        public VendedCredentialsCacheKey
        {
            requireNonNull(user, "user is null");
            s3VendedCredentials = ImmutableMap.copyOf(s3VendedCredentials);
            gcsVendedCredentials = ImmutableMap.copyOf(gcsVendedCredentials);
            azureVendedCredentials = ImmutableMap.copyOf(azureVendedCredentials);
        }
    }

    private record CachedVendedCredentialsProviders(
            Map<String, S3VendedCredentialsProvider> s3VendedCredentialsProviders,
            Map<String, GcsVendedCredentialsProvider> gcsVendedCredentialsProviders,
            Map<String, AzureVendedCredentialsProvider> azureVendedCredentialsProviders,
            Map<String, String> extraCredentials)
    {
        public CachedVendedCredentialsProviders
        {
            s3VendedCredentialsProviders = ImmutableMap.copyOf(s3VendedCredentialsProviders);
            gcsVendedCredentialsProviders = ImmutableMap.copyOf(gcsVendedCredentialsProviders);
            azureVendedCredentialsProviders = ImmutableMap.copyOf(azureVendedCredentialsProviders);
            extraCredentials = ImmutableMap.copyOf(extraCredentials);
        }
    }

    private static boolean containsSasToken(Map<String, String> fileIoProperties)
    {
        return fileIoProperties.keySet().stream().anyMatch(key -> key.startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX));
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
        String locationString = location.toString();
        return providers.entrySet().stream()
                // Keys match as plain string prefixes, which the root prefixes rely on, e.g. "abfs" matches abfss:// locations.
                // Storage credential prefixes may carry a trailing slash (e.g. the table's root location); strip it so an
                // exact match against the table location itself (not just a path underneath it) is still recognized.
                .filter(e -> locationString.startsWith(stripTrailingSlash(e.getKey())))
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

    private static boolean isAzurePrefix(String prefix)
    {
        return prefix.startsWith("abfs://") || prefix.startsWith("abfss://") || prefix.startsWith("wasb://") || prefix.startsWith("wasbs://");
    }

    private static Map<String, String> mergeConfigs(Map<String, String> base, Map<String, String> override)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(base)
                .putAll(override)
                .buildKeepingLast();
    }

    private static Map<String, String> mergeAzureConfigs(Map<String, String> fileIoProperties, Map<String, String> storageCredentialConfig)
    {
        // SAS token keys for the same account may differ in form, so a plain merge would keep stale root tokens alongside the prefix ones
        Map<String, String> fileIoPropertiesWithoutSasTokens = Maps.filterKeys(
                fileIoProperties,
                key -> !key.startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX) && !key.startsWith(AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX));
        return mergeConfigs(fileIoPropertiesWithoutSasTokens, storageCredentialConfig);
    }
}
