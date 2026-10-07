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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.filesystem.s3.S3RemoteSignerProvider;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import software.amazon.awssdk.http.auth.spi.signer.HttpSigner;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class IcebergRestCatalogS3RemoteSignerProvider
        implements S3RemoteSignerProvider, AutoCloseable
{
    static final String EXTRA_CREDENTIALS_PREFIX = "internal$iceberg_rest_s3_signer$";

    private static final Set<String> TABLE_AUTHENTICATION_PROPERTIES = ImmutableSet.of(
            OAuth2Properties.TOKEN,
            OAuth2Properties.CREDENTIAL,
            OAuth2Properties.TOKEN_EXPIRES_IN_MS);

    private final Map<String, String> catalogProperties;
    private final Map<String, String> authenticationProperties;
    private final HTTPClient httpClient;
    private final AuthManager authManager;

    @Inject
    public IcebergRestCatalogS3RemoteSignerProvider(IcebergRestCatalogPropertiesProvider propertiesProvider)
    {
        this(propertiesProvider.catalogProperties());
    }

    IcebergRestCatalogS3RemoteSignerProvider(Map<String, String> catalogProperties)
    {
        this.catalogProperties = ImmutableMap.copyOf(requireNonNull(catalogProperties, "catalogProperties is null"));
        this.authenticationProperties = ImmutableMap.<String, String>builder()
                .putAll(catalogProperties)
                .put(AuthProperties.AUTH_TYPE, catalogProperties.getOrDefault(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2))
                .putAll(OAuth2Util.buildOptionalParam(catalogProperties, "sign"))
                .put(OAuth2Properties.OAUTH2_SERVER_URI, RESTUtil.resolveEndpoint(
                        catalogProperties.get(CatalogProperties.URI),
                        catalogProperties.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens())))
                .buildKeepingLast();
        Map<String, String> sharedHeaders = new HashMap<>(RESTUtil.configHeaders(catalogProperties));
        sharedHeaders.keySet().removeIf("Authorization"::equalsIgnoreCase);
        this.httpClient = HTTPClient.builder(catalogProperties)
                .withHeaders(sharedHeaders)
                .build();
        this.authManager = AuthManagers.loadAuthManager("s3-signer", authenticationProperties);
        if (authManager instanceof OAuth2Manager) {
            Map<String, String> bootstrapProperties = new HashMap<>(authenticationProperties);
            bootstrapProperties.remove(OAuth2Properties.TOKEN);
            bootstrapProperties.remove(OAuth2Properties.CREDENTIAL);
            // Initialize the refresh client without retaining the first table's authentication.
            authManager.catalogSession(httpClient, bootstrapProperties).close();
        }
    }

    @Override
    public Optional<HttpSigner<AwsCredentialsIdentity>> getSigner(ConnectorIdentity identity)
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        identity.getExtraCredentials().forEach((key, value) -> {
            if (key.startsWith(EXTRA_CREDENTIALS_PREFIX)) {
                properties.put(key.substring(EXTRA_CREDENTIALS_PREFIX.length()), value);
            }
        });

        Map<String, String> signerProperties = properties.buildOrThrow();
        if (signerProperties.isEmpty()) {
            return Optional.empty();
        }

        Map<String, String> mergedProperties = RESTUtil.merge(catalogProperties, signerProperties);
        Map<String, String> tableAuthenticationProperties = new HashMap<>(authenticationProperties);
        for (String property : TABLE_AUTHENTICATION_PROPERTIES) {
            String value = mergedProperties.get(property);
            if (value != null) {
                tableAuthenticationProperties.put(property, value);
            }
        }
        String token = mergedProperties.get(OAuth2Properties.TOKEN);
        if (token != null && !token.equals(catalogProperties.get(OAuth2Properties.TOKEN))) {
            // A delegated token must not refresh into the catalog's service identity.
            tableAuthenticationProperties.remove(OAuth2Properties.CREDENTIAL);
        }
        Map<String, String> authentication = ImmutableMap.copyOf(tableAuthenticationProperties);
        return Optional.of(new IcebergS3RemoteSigner(
                httpClient,
                () -> authManager.tableSession(httpClient, authentication),
                mergedProperties));
    }

    @PreDestroy
    @Override
    public void close()
            throws IOException
    {
        try {
            authManager.close();
        }
        finally {
            httpClient.close();
        }
    }
}
