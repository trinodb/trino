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
package io.trino.plugin.iceberg.catalog.nessie;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.catalog.nessie.IcebergNessieCatalogConfig.Security.BEARER;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Predicate.isEqual;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_CONNECT_TIMEOUT_MILLIS;
import static org.projectnessie.client.NessieConfigConstants.DEFAULT_READ_TIMEOUT_MILLIS;

public class IcebergNessieCatalogConfig
{
    public enum Security
    {
        BEARER,
    }

    public enum ClientApiVersion
    {
        V1,
        V2,
    }

    private String defaultReferenceName = "main";
    private String defaultWarehouseDir;
    private URI serverUri;
    private Duration readTimeout = new Duration(DEFAULT_READ_TIMEOUT_MILLIS, MILLISECONDS);
    private Duration connectionTimeout = new Duration(DEFAULT_CONNECT_TIMEOUT_MILLIS, MILLISECONDS);
    private boolean enableCompression = true;
    private Security security;
    private Optional<String> bearerToken = Optional.empty();
    private Optional<ClientApiVersion> clientAPIVersion = Optional.empty();
    private static final Pattern VERSION_PATTERN = Pattern.compile("/v(\\d+)$");

    @NotNull
    public String getDefaultReferenceName()
    {
        return defaultReferenceName;
    }

    @Config("iceberg.nessie-catalog.ref")
    @ConfigDescription("The default Nessie reference to work on")
    public IcebergNessieCatalogConfig setDefaultReferenceName(String defaultReferenceName)
    {
        this.defaultReferenceName = defaultReferenceName;
        return this;
    }

    @NotNull
    public URI getServerUri()
    {
        return serverUri;
    }

    @Config("iceberg.nessie-catalog.uri")
    @ConfigDescription("The URI to connect to the Nessie server")
    public IcebergNessieCatalogConfig setServerUri(URI serverUri)
    {
        this.serverUri = serverUri;
        return this;
    }

    @NotEmpty
    public String getDefaultWarehouseDir()
    {
        return defaultWarehouseDir;
    }

    @Config("iceberg.nessie-catalog.default-warehouse-dir")
    @ConfigDescription("The default warehouse to use for Nessie")
    public IcebergNessieCatalogConfig setDefaultWarehouseDir(String defaultWarehouseDir)
    {
        this.defaultWarehouseDir = defaultWarehouseDir;
        return this;
    }

    @MinDuration("1ms")
    public Duration getReadTimeout()
    {
        return readTimeout;
    }

    @Config("iceberg.nessie-catalog.read-timeout")
    @ConfigDescription("The read timeout for the client.")
    public IcebergNessieCatalogConfig setReadTimeout(Duration readTimeout)
    {
        this.readTimeout = readTimeout;
        return this;
    }

    @MinDuration("1ms")
    public Duration getConnectionTimeout()
    {
        return connectionTimeout;
    }

    @Config("iceberg.nessie-catalog.connection-timeout")
    @ConfigDescription("The connection timeout for the client.")
    public IcebergNessieCatalogConfig setConnectionTimeout(Duration connectionTimeout)
    {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public boolean isCompressionEnabled()
    {
        return enableCompression;
    }

    @Config("iceberg.nessie-catalog.enable-compression")
    @ConfigDescription("Configure whether compression should be enabled or not.")
    public IcebergNessieCatalogConfig setCompressionEnabled(boolean enableCompression)
    {
        this.enableCompression = enableCompression;
        return this;
    }

    public Optional<Security> getSecurity()
    {
        return Optional.ofNullable(security);
    }

    @Config("iceberg.nessie-catalog.authentication.type")
    @ConfigDescription("The authentication type to use")
    public IcebergNessieCatalogConfig setSecurity(Security security)
    {
        this.security = security;
        return this;
    }

    public Optional<String> getBearerToken()
    {
        return bearerToken;
    }

    @Config("iceberg.nessie-catalog.authentication.token")
    @ConfigDescription("The token to use with BEARER authentication")
    @ConfigSecuritySensitive
    public IcebergNessieCatalogConfig setBearerToken(String token)
    {
        this.bearerToken = Optional.ofNullable(token);
        return this;
    }

    @AssertTrue(message = "'iceberg.nessie-catalog.authentication.token' must be configured only with 'iceberg.nessie-catalog.authentication.type' BEARER")
    public boolean isTokenConfiguredWithoutType()
    {
        return getSecurity().filter(isEqual(BEARER)).isPresent() || getBearerToken().isEmpty();
    }

    @AssertTrue(message = "'iceberg.nessie-catalog.authentication.token' must be configured with 'iceberg.nessie-catalog.authentication.type' BEARER")
    public boolean isMissingTokenForBearerAuth()
    {
        return getSecurity().filter(isEqual(BEARER)).isEmpty() || getBearerToken().isPresent();
    }

    public Optional<ClientApiVersion> getClientAPIVersion()
    {
        return clientAPIVersion;
    }

    @Config("iceberg.nessie-catalog.client-api-version")
    @ConfigDescription("Client API version to use")
    public IcebergNessieCatalogConfig setClientAPIVersion(ClientApiVersion version)
    {
        this.clientAPIVersion = Optional.ofNullable(version);
        return this;
    }

    protected IcebergNessieCatalogConfig.ClientApiVersion inferVersionFromURI()
    {
        checkArgument(serverUri != null, "URI is not specified in the catalog properties");
        // match for uri ending with /v1, /v2 etc
        Matcher matcher = VERSION_PATTERN.matcher(serverUri.toString());
        if (!matcher.find()) {
            throw new IllegalArgumentException("URI doesn't end with the version: %s. Please configure `client-api-version` in the catalog properties explicitly.".formatted(serverUri));
        }

        return switch (matcher.group(1)) {
            case "1" -> ClientApiVersion.V1;
            case "2" -> ClientApiVersion.V2;
            default -> throw new IllegalArgumentException("Unknown API version in the URI: " + matcher.group(1));
        };
    }
}
