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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Base class for storage-specific vended credential providers that refresh
 * credentials by calling the Iceberg REST catalog's credentials endpoint
 * 5 minutes before they expire, similar to {@code org.apache.iceberg.aws.s3.VendedCredentialsProvider}.
 */
abstract class AbstractIcebergRestVendedCredentialsProvider<T extends VendedCredentials>
        implements VendedCredentialsProvider<T>
{
    static final long PREFETCH_MINUTES = 5;

    private final Map<String, String> properties;
    private final String catalogEndpoint;
    private final boolean refreshCredentialsEnabled;
    private final Optional<String> credentialsEndpoint;
    private volatile T cached;

    AbstractIcebergRestVendedCredentialsProvider(
            Map<String, String> catalogProperties,
            Map<String, String> fileIoProperties,
            boolean refreshCredentialsEnabled,
            Optional<String> credentialsEndpoint,
            T initialCredentials)
    {
        requireNonNull(catalogProperties, "catalogProperties is null");
        requireNonNull(fileIoProperties, "fileIoProperties is null");
        // Follows the merge logic of catalog & table properties from org.apache.iceberg.rest.RESTSessionCatalog.tableFileIO
        // https://github.com/apache/iceberg/blob/b25cb522ccaddacc4e7be4dcf48fc80dd7b823dd/core/src/main/java/org/apache/iceberg/rest/RESTSessionCatalog.java#L1216
        this.properties = RESTUtil.merge(catalogProperties, fileIoProperties);
        this.catalogEndpoint = requireNonNull(properties.get(CatalogProperties.URI), "catalog endpoint is null");
        this.refreshCredentialsEnabled = refreshCredentialsEnabled;
        this.credentialsEndpoint = requireNonNull(credentialsEndpoint, "credentialsEndpoint is null");
        this.cached = requireNonNull(initialCredentials, "initialCredentials is null");
    }

    @Override
    public T getCredentials()
    {
        T current = cached;
        if (shouldRefresh(current)) {
            synchronized (this) {
                current = cached;
                if (shouldRefresh(current)) {
                    current = refreshCachedState();
                    cached = current;
                }
            }
        }
        return current;
    }

    private boolean shouldRefresh(T vendedCredentials)
    {
        return refreshCredentialsEnabled &&
                credentialsEndpoint.isPresent() &&
                isStale(vendedCredentials);
    }

    private boolean isStale(T vendedCredentials)
    {
        Optional<Instant> expiresAt = vendedCredentials.expiresAt();
        return expiresAt.isPresent() && Instant.now().isAfter(expiresAt.get().minus(PREFETCH_MINUTES, ChronoUnit.MINUTES));
    }

    private T refreshCachedState()
    {
        LoadCredentialsResponse response = fetchCredentials();
        return applyRefreshedCredentials(response.credentials());
    }

    private LoadCredentialsResponse fetchCredentials()
    {
        try (AuthManager authManager = AuthManagers.loadAuthManager("credentials-refresh", properties);
                HTTPClient httpClient = HTTPClient.builder(properties)
                        .uri(catalogEndpoint)
                        .withHeaders(RESTUtil.configHeaders(properties))
                        .build()) {
            AuthSession authSession = authManager.catalogSession(httpClient, properties);
            return httpClient.withAuthSession(authSession).get(
                    credentialsEndpoint.orElseThrow(),
                    null,
                    LoadCredentialsResponse.class,
                    Map.of(),
                    ErrorHandlers.defaultErrorHandler());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to fetch vended credentials", e);
        }
    }

    protected abstract T applyRefreshedCredentials(List<Credential> credentials);

    protected static boolean parseBoolean(Map<String, String> properties, String key, boolean defaultValue)
    {
        String value = properties.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
            return Boolean.parseBoolean(value);
        }
        throw new IllegalArgumentException("Invalid boolean value for property '%s': %s".formatted(key, value));
    }

    protected static Optional<Instant> parseInstantEpochMillis(Map<String, String> properties, String key)
    {
        String value = properties.get(key);
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(Instant.ofEpochMilli(Long.parseLong(value)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid epoch millis value for property '%s': %s".formatted(key, value), e);
        }
    }
}
