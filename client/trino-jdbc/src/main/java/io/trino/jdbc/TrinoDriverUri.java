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
package io.trino.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import io.trino.client.ClientException;
import io.trino.client.ClientSelectedRole;
import io.trino.client.auth.external.CompositeRedirectHandler;
import io.trino.client.auth.external.ExternalAuthenticator;
import io.trino.client.auth.external.HttpTokenPoller;
import io.trino.client.auth.external.RedirectHandler;
import io.trino.client.auth.external.TokenPoller;
import okhttp3.OkHttpClient;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.client.KerberosUtil.defaultCredentialCachePath;
import static io.trino.client.OkHttpUtil.basicAuth;
import static io.trino.client.OkHttpUtil.setupCookieJar;
import static io.trino.client.OkHttpUtil.setupHttpProxy;
import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.client.OkHttpUtil.setupKerberos;
import static io.trino.client.OkHttpUtil.setupSocksProxy;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.client.OkHttpUtil.tokenAuth;
import static io.trino.jdbc.ConnectionProperties.ACCESS_TOKEN;
import static io.trino.jdbc.ConnectionProperties.APPLICATION_NAME_PREFIX;
import static io.trino.jdbc.ConnectionProperties.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS;
import static io.trino.jdbc.ConnectionProperties.CLIENT_INFO;
import static io.trino.jdbc.ConnectionProperties.CLIENT_TAGS;
import static io.trino.jdbc.ConnectionProperties.DISABLE_COMPRESSION;
import static io.trino.jdbc.ConnectionProperties.EXTERNAL_AUTHENTICATION;
import static io.trino.jdbc.ConnectionProperties.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS;
import static io.trino.jdbc.ConnectionProperties.EXTERNAL_AUTHENTICATION_TIMEOUT;
import static io.trino.jdbc.ConnectionProperties.EXTERNAL_AUTHENTICATION_TOKEN_CACHE;
import static io.trino.jdbc.ConnectionProperties.EXTRA_CREDENTIALS;
import static io.trino.jdbc.ConnectionProperties.HTTP_PROXY;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_CONFIG_PATH;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_CREDENTIAL_CACHE_PATH;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_DELEGATION;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_KEYTAB_PATH;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_PRINCIPAL;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_REMOTE_SERVICE_NAME;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_SERVICE_PRINCIPAL_PATTERN;
import static io.trino.jdbc.ConnectionProperties.KERBEROS_USE_CANONICAL_HOSTNAME;
import static io.trino.jdbc.ConnectionProperties.PASSWORD;
import static io.trino.jdbc.ConnectionProperties.ROLES;
import static io.trino.jdbc.ConnectionProperties.SESSION_PROPERTIES;
import static io.trino.jdbc.ConnectionProperties.SESSION_USER;
import static io.trino.jdbc.ConnectionProperties.SOCKS_PROXY;
import static io.trino.jdbc.ConnectionProperties.SOURCE;
import static io.trino.jdbc.ConnectionProperties.SSL;
import static io.trino.jdbc.ConnectionProperties.SSL_KEY_STORE_PASSWORD;
import static io.trino.jdbc.ConnectionProperties.SSL_KEY_STORE_PATH;
import static io.trino.jdbc.ConnectionProperties.SSL_KEY_STORE_TYPE;
import static io.trino.jdbc.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static io.trino.jdbc.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static io.trino.jdbc.ConnectionProperties.SSL_TRUST_STORE_TYPE;
import static io.trino.jdbc.ConnectionProperties.SSL_USE_SYSTEM_TRUST_STORE;
import static io.trino.jdbc.ConnectionProperties.SSL_VERIFICATION;
import static io.trino.jdbc.ConnectionProperties.SslVerificationMode;
import static io.trino.jdbc.ConnectionProperties.SslVerificationMode.CA;
import static io.trino.jdbc.ConnectionProperties.SslVerificationMode.FULL;
import static io.trino.jdbc.ConnectionProperties.SslVerificationMode.NONE;
import static io.trino.jdbc.ConnectionProperties.TRACE_TOKEN;
import static io.trino.jdbc.ConnectionProperties.USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Parses and extracts parameters from a Trino JDBC URL.
 */
public final class TrinoDriverUri
{
    private static final String JDBC_URL_PREFIX = "jdbc:";
    private static final String JDBC_URL_START = JDBC_URL_PREFIX + "trino:";

    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);
    private static final AtomicReference<RedirectHandler> REDIRECT_HANDLER = new AtomicReference<>(null);
    private final HostAndPort address;
    private final URI uri;

    private final Properties properties;

    private Optional<String> catalog = Optional.empty();
    private Optional<String> schema = Optional.empty();

    private final boolean useSecureConnection;

    private TrinoDriverUri(String url, Properties driverProperties)
            throws SQLException
    {
        this(parseDriverUrl(url), driverProperties);
    }

    private TrinoDriverUri(URI uri, Properties driverProperties)
            throws SQLException
    {
        this.uri = requireNonNull(uri, "uri is null");
        address = HostAndPort.fromParts(uri.getHost(), uri.getPort());
        properties = mergeConnectionProperties(uri, driverProperties);

        validateConnectionProperties(properties);

        // enable SSL by default for standard port
        useSecureConnection = SSL.getValue(properties).orElse(uri.getPort() == 443);

        initCatalogAndSchema();
    }

    public static TrinoDriverUri create(String url, Properties properties)
            throws SQLException
    {
        return new TrinoDriverUri(url, firstNonNull(properties, new Properties()));
    }

    public static boolean acceptsURL(String url)
    {
        return url.startsWith(JDBC_URL_START);
    }

    public URI getJdbcUri()
    {
        return uri;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public URI getHttpUri()
    {
        return buildHttpUri();
    }

    public String getRequiredUser()
            throws SQLException
    {
        return USER.getRequiredValue(properties);
    }

    public Optional<String> getUser()
            throws SQLException
    {
        return USER.getValue(properties);
    }

    public Optional<String> getSessionUser()
            throws SQLException
    {
        return SESSION_USER.getValue(properties);
    }

    public Map<String, ClientSelectedRole> getRoles()
            throws SQLException
    {
        return ROLES.getValue(properties).orElse(ImmutableMap.of());
    }

    public Optional<String> getApplicationNamePrefix()
            throws SQLException
    {
        return APPLICATION_NAME_PREFIX.getValue(properties);
    }

    public Properties getProperties()
    {
        return properties;
    }

    public Map<String, String> getExtraCredentials()
            throws SQLException
    {
        return EXTRA_CREDENTIALS.getValue(properties).orElse(ImmutableMap.of());
    }

    public Optional<String> getClientInfo()
            throws SQLException
    {
        return CLIENT_INFO.getValue(properties);
    }

    public Optional<String> getClientTags()
            throws SQLException
    {
        return CLIENT_TAGS.getValue(properties);
    }

    public Optional<String> getTraceToken()
            throws SQLException
    {
        return TRACE_TOKEN.getValue(properties);
    }

    public Map<String, String> getSessionProperties()
            throws SQLException
    {
        return SESSION_PROPERTIES.getValue(properties).orElse(ImmutableMap.of());
    }

    public Optional<String> getSource()
            throws SQLException
    {
        return SOURCE.getValue(properties);
    }

    public boolean isCompressionDisabled()
            throws SQLException
    {
        return DISABLE_COMPRESSION.getValue(properties).orElse(false);
    }

    public boolean isAssumeLiteralNamesInMetadataCallsForNonConformingClients()
            throws SQLException
    {
        return ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.getValue(properties).orElse(false);
    }

    public void setupClient(OkHttpClient.Builder builder)
            throws SQLException
    {
        try {
            setupCookieJar(builder);
            setupSocksProxy(builder, SOCKS_PROXY.getValue(properties));
            setupHttpProxy(builder, HTTP_PROXY.getValue(properties));

            // TODO: fix Tempto to allow empty passwords
            String password = PASSWORD.getValue(properties).orElse("");
            if (!password.isEmpty() && !password.equals("***empty***")) {
                if (!useSecureConnection) {
                    throw new SQLException("Authentication using username/password requires SSL to be enabled");
                }
                builder.addInterceptor(basicAuth(getRequiredUser(), password));
            }

            if (useSecureConnection) {
                SslVerificationMode sslVerificationMode = SSL_VERIFICATION.getValue(properties).orElse(FULL);
                if (sslVerificationMode.equals(FULL) || sslVerificationMode.equals(CA)) {
                    setupSsl(
                            builder,
                            SSL_KEY_STORE_PATH.getValue(properties),
                            SSL_KEY_STORE_PASSWORD.getValue(properties),
                            SSL_KEY_STORE_TYPE.getValue(properties),
                            SSL_TRUST_STORE_PATH.getValue(properties),
                            SSL_TRUST_STORE_PASSWORD.getValue(properties),
                            SSL_TRUST_STORE_TYPE.getValue(properties),
                            SSL_USE_SYSTEM_TRUST_STORE.getValue(properties).orElse(false));
                }

                if (sslVerificationMode.equals(CA)) {
                    builder.hostnameVerifier((hostname, session) -> true);
                }

                if (sslVerificationMode.equals(NONE)) {
                    setupInsecureSsl(builder);
                }
            }

            if (KERBEROS_REMOTE_SERVICE_NAME.getValue(properties).isPresent()) {
                if (!useSecureConnection) {
                    throw new SQLException("Authentication using Kerberos requires SSL to be enabled");
                }
                setupKerberos(
                        builder,
                        KERBEROS_SERVICE_PRINCIPAL_PATTERN.getRequiredValue(properties),
                        KERBEROS_REMOTE_SERVICE_NAME.getRequiredValue(properties),
                        KERBEROS_USE_CANONICAL_HOSTNAME.getRequiredValue(properties),
                        KERBEROS_PRINCIPAL.getValue(properties),
                        KERBEROS_CONFIG_PATH.getValue(properties),
                        KERBEROS_KEYTAB_PATH.getValue(properties),
                        Optional.ofNullable(KERBEROS_CREDENTIAL_CACHE_PATH.getValue(properties)
                                .orElseGet(() -> defaultCredentialCachePath().map(File::new).orElse(null))),
                        KERBEROS_DELEGATION.getRequiredValue(properties));
            }

            if (ACCESS_TOKEN.getValue(properties).isPresent()) {
                if (!useSecureConnection) {
                    throw new SQLException("Authentication using an access token requires SSL to be enabled");
                }
                builder.addInterceptor(tokenAuth(ACCESS_TOKEN.getValue(properties).get()));
            }

            if (EXTERNAL_AUTHENTICATION.getValue(properties).orElse(false)) {
                if (!useSecureConnection) {
                    throw new SQLException("Authentication using external authorization requires SSL to be enabled");
                }

                // create HTTP client that shares the same settings, but without the external authenticator
                TokenPoller poller = new HttpTokenPoller(builder.build());

                Duration timeout = EXTERNAL_AUTHENTICATION_TIMEOUT.getValue(properties)
                        .map(value -> Duration.ofMillis(value.toMillis()))
                        .orElse(Duration.ofMinutes(2));

                KnownTokenCache knownTokenCache = EXTERNAL_AUTHENTICATION_TOKEN_CACHE.getValue(properties).get();

                Optional<RedirectHandler> configuredHandler = EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS.getValue(properties)
                        .map(CompositeRedirectHandler::new)
                        .map(RedirectHandler.class::cast);

                RedirectHandler redirectHandler = Optional.ofNullable(REDIRECT_HANDLER.get())
                        .orElseGet(() -> configuredHandler.orElseThrow(() -> new RuntimeException("External authentication redirect handler is not configured")));

                ExternalAuthenticator authenticator = new ExternalAuthenticator(redirectHandler, poller, knownTokenCache.create(), timeout);

                builder.authenticator(authenticator);
                builder.addInterceptor(authenticator);
            }
        }
        catch (ClientException e) {
            throw new SQLException(e.getMessage(), e);
        }
        catch (RuntimeException e) {
            throw new SQLException("Error setting up connection", e);
        }
    }

    private static Map<String, String> parseParameters(String query)
            throws SQLException
    {
        Map<String, String> result = new HashMap<>();

        if (query != null) {
            Iterable<String> queryArgs = QUERY_SPLITTER.split(query);
            for (String queryArg : queryArgs) {
                List<String> parts = ARG_SPLITTER.splitToList(queryArg);
                if (parts.size() != 2) {
                    throw new SQLException(format("Connection argument is not valid connection property: '%s'", queryArg));
                }
                if (result.put(parts.get(0), parts.get(1)) != null) {
                    throw new SQLException(format("Connection property '%s' is in URL multiple times", parts.get(0)));
                }
            }
        }

        return result;
    }

    private static URI parseDriverUrl(String url)
            throws SQLException
    {
        if (!url.startsWith(JDBC_URL_START)) {
            throw new SQLException("Invalid JDBC URL: " + url);
        }

        if (url.equals(JDBC_URL_START)) {
            throw new SQLException("Empty JDBC URL: " + url);
        }

        URI uri;
        try {
            uri = new URI(url.substring(JDBC_URL_PREFIX.length()));
        }
        catch (URISyntaxException e) {
            throw new SQLException("Invalid JDBC URL: " + url, e);
        }

        if (isNullOrEmpty(uri.getHost())) {
            throw new SQLException("No host specified: " + url);
        }
        if (uri.getPort() == -1) {
            throw new SQLException("No port number specified: " + url);
        }
        if ((uri.getPort() < 1) || (uri.getPort() > 65535)) {
            throw new SQLException("Invalid port number: " + url);
        }
        return uri;
    }

    private URI buildHttpUri()
    {
        String scheme = useSecureConnection ? "https" : "http";
        try {
            return new URI(scheme, null, address.getHost(), address.getPort(), null, null, null);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void initCatalogAndSchema()
            throws SQLException
    {
        String path = uri.getPath();
        if (isNullOrEmpty(uri.getPath()) || path.equals("/")) {
            return;
        }

        // remove first slash
        if (!path.startsWith("/")) {
            throw new SQLException("Path does not start with a slash: " + uri);
        }
        path = path.substring(1);

        List<String> parts = Splitter.on("/").splitToList(path);
        // remove last item due to a trailing slash
        if (parts.get(parts.size() - 1).isEmpty()) {
            parts = parts.subList(0, parts.size() - 1);
        }

        if (parts.size() > 2) {
            throw new SQLException("Invalid path segments in URL: " + uri);
        }

        if (parts.get(0).isEmpty()) {
            throw new SQLException("Catalog name is empty: " + uri);
        }

        catalog = Optional.ofNullable(parts.get(0));

        if (parts.size() > 1) {
            if (parts.get(1).isEmpty()) {
                throw new SQLException("Schema name is empty: " + uri);
            }

            schema = Optional.ofNullable(parts.get(1));
        }
    }

    private static Properties mergeConnectionProperties(URI uri, Properties driverProperties)
            throws SQLException
    {
        Map<String, String> defaults = ConnectionProperties.getDefaults();
        Map<String, String> urlProperties = parseParameters(uri.getQuery());
        Map<String, String> suppliedProperties = Maps.fromProperties(driverProperties);

        for (String key : urlProperties.keySet()) {
            if (suppliedProperties.containsKey(key)) {
                throw new SQLException(format("Connection property '%s' is both in the URL and an argument", key));
            }
        }

        Properties result = new Properties();
        setProperties(result, defaults);
        setProperties(result, urlProperties);
        setProperties(result, suppliedProperties);
        return result;
    }

    private static void setProperties(Properties properties, Map<String, String> values)
    {
        for (Entry<String, String> entry : values.entrySet()) {
            properties.setProperty(entry.getKey(), entry.getValue());
        }
    }

    private static void validateConnectionProperties(Properties connectionProperties)
            throws SQLException
    {
        for (String propertyName : connectionProperties.stringPropertyNames()) {
            if (ConnectionProperties.forKey(propertyName) == null) {
                throw new SQLException(format("Unrecognized connection property '%s'", propertyName));
            }
        }

        for (ConnectionProperty<?> property : ConnectionProperties.allProperties()) {
            property.validate(connectionProperties);
        }
    }

    @VisibleForTesting
    static void setRedirectHandler(RedirectHandler handler)
    {
        REDIRECT_HANDLER.set(requireNonNull(handler, "handler is null"));
    }
}
