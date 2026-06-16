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
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.Security;
import io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogConfig.SessionType;
import io.trino.plugin.iceberg.fileio.ForwardingFileIoFactory;
import io.trino.spi.NodeVersion;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.RESTUtil;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.rest.auth.OAuth2Properties.CREDENTIAL;
import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;

public class TrinoIcebergRestCatalogFactory
        implements TrinoCatalogFactory
{
    private static final Logger log = Logger.get(TrinoIcebergRestCatalogFactory.class);

    private final IcebergFileSystemFactory fileSystemFactory;
    private final ForwardingFileIoFactory fileIoFactory;
    private final CatalogName catalogName;
    private final String trinoVersion;
    private final boolean nestedNamespaceEnabled;
    private final Security security;
    private final SessionType sessionType;
    private final boolean tokenDelegation;
    private final boolean viewEndpointsEnabled;
    private final SecurityProperties securityProperties;
    private final IcebergRestCatalogPropertiesProvider catalogPropertiesProvider;
    private final Optional<OidcStsCredentialExchanger> stsExchanger;
    private final UserTokenProvider tokenProvider;
    private final boolean uniqueTableLocation;
    private final TypeManager typeManager;
    private final boolean caseInsensitiveNameMatching;
    private final Cache<Namespace, Namespace> remoteNamespaceMappingCache;
    private final Cache<TableIdentifier, TableIdentifier> remoteTableMappingCache;

    @GuardedBy("this")
    private RESTSessionCatalog icebergCatalog;

    @Inject
    public TrinoIcebergRestCatalogFactory(
            IcebergFileSystemFactory fileSystemFactory,
            ForwardingFileIoFactory fileIoFactory,
            CatalogName catalogName,
            IcebergRestCatalogConfig restConfig,
            SecurityProperties securityProperties,
            IcebergRestCatalogPropertiesProvider catalogPropertiesProvider,
            Optional<OidcStsCredentialExchanger> stsExchanger,
            UserTokenProvider tokenProvider,
            IcebergConfig icebergConfig,
            TypeManager typeManager,
            NodeVersion nodeVersion)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.fileIoFactory = requireNonNull(fileIoFactory, "fileIoFactory is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.trinoVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(restConfig, "restConfig is null");
        this.nestedNamespaceEnabled = restConfig.isNestedNamespaceEnabled();
        this.security = restConfig.getSecurity();
        this.sessionType = restConfig.getSessionType();
        this.tokenDelegation = restConfig.isTokenDelegation();
        this.viewEndpointsEnabled = restConfig.isViewEndpointsEnabled();
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.catalogPropertiesProvider = requireNonNull(catalogPropertiesProvider, "catalogPropertiesProvider is null");
        this.stsExchanger = requireNonNull(stsExchanger, "stsExchanger is null");
        this.tokenProvider = requireNonNull(tokenProvider, "tokenProvider is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.uniqueTableLocation = icebergConfig.isUniqueTableLocation();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.caseInsensitiveNameMatching = restConfig.isCaseInsensitiveNameMatching();
        this.remoteNamespaceMappingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
        this.remoteTableMappingCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(restConfig.getCaseInsensitiveNameMatchingCacheTtl().toMillis(), MILLISECONDS)
                .shareNothingWhenDisabled()
                .build();
    }

    @Override
    public synchronized TrinoCatalog create(ConnectorIdentity identity)
    {
        // Creation of the RESTSessionCatalog is lazy due to required network calls
        // for authorization and config route
        if (icebergCatalog == null) {
            Map<String, String> initProperties = catalogPropertiesProvider.catalogProperties();
            if (tokenDelegation) {
                Optional<String> catalogToken = tokenProvider.catalogToken(identity.getExtraCredentials().get(TOKEN));
                if (catalogToken.isPresent()) {
                    initProperties = ImmutableMap.<String, String>builder()
                            .putAll(Maps.filterKeys(initProperties, key -> !key.equals(TOKEN)))
                            .put(TOKEN, catalogToken.get())
                            .buildOrThrow();
                }
            }
            if (stsExchanger.isPresent()) {
                // tokenProvider.catalogToken() runs the OIDC→catalog token exchange first (if configured)
                // before passing the token to AssumeRoleWithWebIdentity
                Optional<String> catalogToken = tokenProvider.catalogToken(identity.getExtraCredentials().get(TOKEN));
                if (catalogToken.isPresent()) {
                    AwsSessionCredentials awsCreds = stsExchanger.get().getCredentials(catalogToken.get());
                    log.debug("Injecting STS credentials into catalog init properties: accessKeyId=%s", awsCreds.accessKeyId());
                    Set<String> stsKeys = Set.of(AwsProperties.REST_ACCESS_KEY_ID, AwsProperties.REST_SECRET_ACCESS_KEY, AwsProperties.REST_SESSION_TOKEN);
                    initProperties = ImmutableMap.<String, String>builder()
                            .putAll(Maps.filterKeys(initProperties, key -> !stsKeys.contains(key)))
                            .put(AwsProperties.REST_ACCESS_KEY_ID, awsCreds.accessKeyId())
                            .put(AwsProperties.REST_SECRET_ACCESS_KEY, awsCreds.secretAccessKey())
                            .put(AwsProperties.REST_SESSION_TOKEN, awsCreds.sessionToken())
                            .buildOrThrow();
                }
                else {
                    log.warn("STS exchanger present but no OIDC token found in user credentials — catalog will initialize without STS credentials");
                }
            }
            RESTSessionCatalog icebergCatalogInstance = new RESTSessionCatalog(
                    config -> HTTPClient.builder(config)
                            .uri(config.get(CatalogProperties.URI))
                            .withHeaders(RESTUtil.configHeaders(config))
                            .build(),
                    (context, config) -> {
                        ConnectorIdentity currentIdentity = (context.wrappedIdentity() != null)
                                ? ((ConnectorIdentity) context.wrappedIdentity())
                                : ConnectorIdentity.ofUser("fake");
                        // When STS credentials are present in the session context (rest.access-key-id),
                        // also expose them as s3.* keys so IcebergRestCatalogFileSystemFactory picks
                        // them up for actual S3 reads. Skipped when the catalog already returned vended
                        // s3.* credentials in the table config response.
                        Map<String, String> fileIoConfig = config;
                        if (context.credentials() != null
                                && context.credentials().containsKey(AwsProperties.REST_ACCESS_KEY_ID)
                                && !config.containsKey(S3FileIOProperties.ACCESS_KEY_ID)) {
                            fileIoConfig = ImmutableMap.<String, String>builder()
                                    .putAll(config)
                                    .put(S3FileIOProperties.ACCESS_KEY_ID, context.credentials().get(AwsProperties.REST_ACCESS_KEY_ID))
                                    .put(S3FileIOProperties.SECRET_ACCESS_KEY, context.credentials().get(AwsProperties.REST_SECRET_ACCESS_KEY))
                                    .put(S3FileIOProperties.SESSION_TOKEN, context.credentials().get(AwsProperties.REST_SESSION_TOKEN))
                                    .buildOrThrow();
                        }
                        return fileIoFactory.create(fileSystemFactory.create(currentIdentity, fileIoConfig), true, fileIoConfig);
                    });
            icebergCatalogInstance.initialize(catalogName.toString(), initProperties);

            icebergCatalog = icebergCatalogInstance;
        }

        // `OAuth2Properties.SCOPE` is not set as scope passed through credentials is unused in
        // https://github.com/apache/iceberg/blob/229d8f6fcd109e6c8943ea7cbb41dab746c6d0ed/core/src/main/java/org/apache/iceberg/rest/auth/OAuth2Util.java#L714-L721
        Map<String, String> credentials = Maps.filterKeys(securityProperties.get(), key -> Set.of(TOKEN, CREDENTIAL).contains(key));
        log.debug("SigV4 security properties passed to catalog session: %s", securityProperties.get());

        return new TrinoRestCatalog(
                fileSystemFactory,
                icebergCatalog,
                catalogName,
                security,
                sessionType,
                credentials,
                stsExchanger,
                tokenProvider,
                nestedNamespaceEnabled,
                trinoVersion,
                typeManager,
                uniqueTableLocation,
                caseInsensitiveNameMatching,
                remoteNamespaceMappingCache,
                remoteTableMappingCache,
                viewEndpointsEnabled);
    }
}
