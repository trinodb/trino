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
package io.trino.client.uri;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.client.ClientSelectedRole;
import io.trino.client.ClientSession;
import io.trino.client.DnsResolver;
import io.trino.client.auth.external.ExternalRedirectStrategy;
import io.trino.client.auth.external.RedirectHandler;
import org.ietf.jgss.GSSCredential;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.client.uri.ConnectionProperties.ACCESS_TOKEN;
import static io.trino.client.uri.ConnectionProperties.APPLICATION_NAME_PREFIX;
import static io.trino.client.uri.ConnectionProperties.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS;
import static io.trino.client.uri.ConnectionProperties.ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS;
import static io.trino.client.uri.ConnectionProperties.ASSUME_NULL_CATALOG_MEANS_CURRENT_CATALOG;
import static io.trino.client.uri.ConnectionProperties.CATALOG;
import static io.trino.client.uri.ConnectionProperties.CLIENT_INFO;
import static io.trino.client.uri.ConnectionProperties.CLIENT_TAGS;
import static io.trino.client.uri.ConnectionProperties.DISABLE_COMPRESSION;
import static io.trino.client.uri.ConnectionProperties.DNS_RESOLVER;
import static io.trino.client.uri.ConnectionProperties.DNS_RESOLVER_CONTEXT;
import static io.trino.client.uri.ConnectionProperties.ENCODING;
import static io.trino.client.uri.ConnectionProperties.EXPLICIT_PREPARE;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION_TIMEOUT;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION_TOKEN_CACHE;
import static io.trino.client.uri.ConnectionProperties.EXTRA_CREDENTIALS;
import static io.trino.client.uri.ConnectionProperties.HOSTNAME_IN_CERTIFICATE;
import static io.trino.client.uri.ConnectionProperties.HTTP_LOGGING_LEVEL;
import static io.trino.client.uri.ConnectionProperties.HTTP_PROXY;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_CONFIG_PATH;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_CONSTRAINED_DELEGATION;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_CREDENTIAL_CACHE_PATH;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_DELEGATION;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_KEYTAB_PATH;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_PRINCIPAL;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_REMOTE_SERVICE_NAME;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_SERVICE_PRINCIPAL_PATTERN;
import static io.trino.client.uri.ConnectionProperties.KERBEROS_USE_CANONICAL_HOSTNAME;
import static io.trino.client.uri.ConnectionProperties.LOCALE;
import static io.trino.client.uri.ConnectionProperties.PASSWORD;
import static io.trino.client.uri.ConnectionProperties.RESOURCE_ESTIMATES;
import static io.trino.client.uri.ConnectionProperties.ROLES;
import static io.trino.client.uri.ConnectionProperties.SCHEMA;
import static io.trino.client.uri.ConnectionProperties.SESSION_PROPERTIES;
import static io.trino.client.uri.ConnectionProperties.SESSION_USER;
import static io.trino.client.uri.ConnectionProperties.SOCKS_PROXY;
import static io.trino.client.uri.ConnectionProperties.SOURCE;
import static io.trino.client.uri.ConnectionProperties.SQL_PATH;
import static io.trino.client.uri.ConnectionProperties.SSL;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_PASSWORD;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_PATH;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_TYPE;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_TYPE;
import static io.trino.client.uri.ConnectionProperties.SSL_USE_SYSTEM_KEY_STORE;
import static io.trino.client.uri.ConnectionProperties.SSL_USE_SYSTEM_TRUST_STORE;
import static io.trino.client.uri.ConnectionProperties.SSL_VERIFICATION;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.FULL;
import static io.trino.client.uri.ConnectionProperties.TIMEOUT;
import static io.trino.client.uri.ConnectionProperties.TIMEZONE;
import static io.trino.client.uri.ConnectionProperties.TRACE_TOKEN;
import static io.trino.client.uri.ConnectionProperties.USER;
import static io.trino.client.uri.LoggingLevel.NONE;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Parses and extracts parameters from a Trino URL.
 */
public class TrinoUri
{
    private static final String URL_START = "trino:";

    public static final int DEFAULT_INSECURE_PORT = 80;
    public static final int DEFAULT_SECURE_PORT = 443;

    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);

    private static final AtomicReference<RedirectHandler> REDIRECT_HANDLER = new AtomicReference<>(null);

    private final URI uri;
    private final List<PropertyName> restrictedProperties;
    private final Properties properties;

    private TrinoUri(List<PropertyName> restrictedProperties, URI uri, Properties explicitProperties)
    {
        this.restrictedProperties = ImmutableList.copyOf(requireNonNull(restrictedProperties, "restrictedProperties is null"));
        this.uri = requireNonNull(uri, "uri is null");
        this.properties = mergeConnectionProperties(extractPropertiesFromUri(uri, restrictedProperties), requireNonNull(explicitProperties, "explicitProperties is null"));

        // Validate passed properties
        validateConnectionProperties(properties);
    }

    protected TrinoUri(String url, Properties properties)
    {
        this(parseDriverUrl(url), properties);
    }

    protected TrinoUri(URI uri, Properties driverProperties)
    {
        this(ImmutableList.of(), uri, driverProperties);
    }

    public static TrinoUri create(String url, Properties properties)
    {
        return new TrinoUri(url, firstNonNull(properties, new Properties()));
    }

    public static TrinoUri create(URI uri, Properties properties)
    {
        return new TrinoUri(uri, firstNonNull(properties, new Properties()));
    }

    public URI getUri()
    {
        return uri;
    }

    public Optional<Boolean> getSSL()
    {
        return resolveOptional(SSL);
    }

    public Optional<String> getSchema()
    {
        return resolveOptional(SCHEMA);
    }

    public Optional<String> getCatalog()
    {
        return resolveOptional(CATALOG);
    }

    public URI getHttpUri()
    {
        return URI.create(format("%s://%s:%d", isUseSecureConnection() ? "https" : "http", uri.getHost(), getPort()));
    }

    private int getPort()
    {
        if (uri.getPort() > 0) {
            return uri.getPort();
        }

        return isUseSecureConnection() ? DEFAULT_SECURE_PORT : DEFAULT_INSECURE_PORT;
    }

    public String getRequiredUser()
    {
        return resolveRequired(USER);
    }

    public Optional<String> getUser()
    {
        return resolveOptional(USER);
    }

    public boolean hasPassword()
    {
        return !isNullOrEmpty(resolveOptional(PASSWORD).orElse(""));
    }

    public Optional<String> getSessionUser()
    {
        return resolveOptional(SESSION_USER);
    }

    public Map<String, ClientSelectedRole> getRoles()
    {
        return resolveWithDefault(ROLES, ImmutableMap.of());
    }

    public Optional<String> getApplicationNamePrefix()
    {
        return resolveOptional(APPLICATION_NAME_PREFIX);
    }

    public Map<String, String> getExtraCredentials()
    {
        return resolveWithDefault(EXTRA_CREDENTIALS, ImmutableMap.of());
    }

    public Optional<String> getClientInfo()
    {
        return resolveOptional(CLIENT_INFO);
    }

    public Optional<Set<String>> getClientTags()
    {
        return resolveOptional(CLIENT_TAGS);
    }

    public Optional<String> getTraceToken()
    {
        return resolveOptional(TRACE_TOKEN);
    }

    public Map<String, String> getSessionProperties()
    {
        return resolveWithDefault(SESSION_PROPERTIES, ImmutableMap.of());
    }

    public Optional<String> getSource()
    {
        return resolveOptional(SOURCE);
    }

    public Optional<List<String>> getPath()
    {
        return resolveOptional(SQL_PATH);
    }

    public Optional<HostAndPort> getSocksProxy()
    {
        return resolveOptional(SOCKS_PROXY);
    }

    public Optional<HostAndPort> getHttpProxy()
    {
        return resolveOptional(HTTP_PROXY);
    }

    public boolean isUseSecureConnection()
    {
        return resolveRequired(SSL);
    }

    public Optional<String> getPassword()
    {
        return resolveOptional(PASSWORD);
    }

    public SslVerificationMode getSslVerification()
    {
        return resolveWithDefault(SSL_VERIFICATION, FULL);
    }

    public Optional<String> getSslKeyStorePath()
    {
        return resolveOptional(SSL_KEY_STORE_PATH);
    }

    public Optional<String> getSslKeyStorePassword()
    {
        return resolveOptional(SSL_KEY_STORE_PASSWORD);
    }

    public Optional<String> getSslKeyStoreType()
    {
        return resolveOptional(SSL_KEY_STORE_TYPE);
    }

    public boolean getSslUseSystemKeyStore()
    {
        return resolveWithDefault(SSL_USE_SYSTEM_KEY_STORE, false);
    }

    public Optional<String> getSslTrustStorePath()
    {
        return resolveOptional(SSL_TRUST_STORE_PATH);
    }

    public Optional<String> getSslTrustStorePassword()
    {
        return resolveOptional(SSL_TRUST_STORE_PASSWORD);
    }

    public Optional<String> getSslTrustStoreType()
    {
        return resolveOptional(SSL_TRUST_STORE_TYPE);
    }

    public boolean getSslUseSystemTrustStore()
    {
        return resolveWithDefault(SSL_USE_SYSTEM_TRUST_STORE, false);
    }

    public Optional<String> getHostnameInCertificate()
    {
        return resolveOptional(HOSTNAME_IN_CERTIFICATE);
    }

    public String getRequiredKerberosServicePrincipalPattern()
    {
        return resolveWithDefault(KERBEROS_SERVICE_PRINCIPAL_PATTERN, "${SERVICE}@${HOST}");
    }

    public Optional<String> getKerberosRemoteServiceName()
    {
        return resolveOptional(KERBEROS_REMOTE_SERVICE_NAME);
    }

    public String getRequiredKerberosRemoteServiceName()
    {
        return resolveRequired(KERBEROS_REMOTE_SERVICE_NAME);
    }

    public boolean getRequiredKerberosUseCanonicalHostname()
    {
        return resolveWithDefault(KERBEROS_USE_CANONICAL_HOSTNAME, false);
    }

    public Optional<String> getKerberosPrincipal()
    {
        return resolveOptional(KERBEROS_PRINCIPAL);
    }

    public Optional<File> getKerberosConfigPath()
    {
        return resolveOptional(KERBEROS_CONFIG_PATH);
    }

    public Optional<File> getKerberosKeytabPath()
    {
        return resolveOptional(KERBEROS_KEYTAB_PATH);
    }

    public Optional<File> getKerberosCredentialCachePath()
    {
        return resolveOptional(KERBEROS_CREDENTIAL_CACHE_PATH);
    }

    public boolean getKerberosDelegation()
    {
        return resolveWithDefault(KERBEROS_DELEGATION, false);
    }

    public Optional<GSSCredential> getKerberosConstrainedDelegation()
    {
        return resolveOptional(KERBEROS_CONSTRAINED_DELEGATION);
    }

    public Optional<String> getAccessToken()
    {
        return resolveOptional(ACCESS_TOKEN);
    }

    public boolean isExternalAuthenticationEnabled()
    {
        return resolveWithDefault(EXTERNAL_AUTHENTICATION, false);
    }

    public Optional<Duration> getExternalAuthenticationTimeout()
    {
        return resolveOptional(EXTERNAL_AUTHENTICATION_TIMEOUT);
    }

    public Optional<List<ExternalRedirectStrategy>> getExternalRedirectStrategies()
    {
        return resolveOptional(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS);
    }

    public KnownTokenCache getExternalAuthenticationTokenCache()
    {
        return resolveWithDefault(EXTERNAL_AUTHENTICATION_TOKEN_CACHE, KnownTokenCache.NONE);
    }

    public String getDnsResolverContext()
    {
        return resolveWithDefault(DNS_RESOLVER_CONTEXT, null);
    }

    public Optional<Class<? extends DnsResolver>> getDnsResolver()
    {
        return resolveOptional(DNS_RESOLVER);
    }

    public Optional<Boolean> getExplicitPrepare()
    {
        return resolveOptional(EXPLICIT_PREPARE);
    }

    public Optional<Boolean> getAssumeNullCatalogMeansCurrentCatalog()
    {
        return resolveOptional(ASSUME_NULL_CATALOG_MEANS_CURRENT_CATALOG);
    }

    public boolean isCompressionDisabled()
    {
        return resolveWithDefault(DISABLE_COMPRESSION, false);
    }

    public Optional<String> getEncoding()
    {
        return resolveOptional(ENCODING);
    }

    public boolean isAssumeLiteralNamesInMetadataCallsForNonConformingClients()
    {
        return resolveWithDefault(ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS, false);
    }

    public boolean isAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()
    {
        return resolveWithDefault(ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS, false);
    }

    public ZoneId getTimeZone()
    {
        return resolveWithDefault(TIMEZONE, ZoneId.systemDefault());
    }

    public Locale getLocale()
    {
        return resolveWithDefault(LOCALE, Locale.getDefault());
    }

    public Duration getTimeout()
    {
        return resolveWithDefault(TIMEOUT, Duration.valueOf("30s"));
    }

    public LoggingLevel getHttpLoggingLevel()
    {
        return resolveWithDefault(HTTP_LOGGING_LEVEL, NONE);
    }

    private Map<String, String> getResourceEstimates()
    {
        return resolveWithDefault(RESOURCE_ESTIMATES, ImmutableMap.of());
    }

    @VisibleForTesting
    public Properties getProperties()
    {
        return properties;
    }

    private <V, T> T resolveRequired(ConnectionProperty<V, T> property)
    {
        return property.getRequiredValue(properties);
    }

    private <V, T> Optional<T> resolveOptional(ConnectionProperty<V, T> property)
    {
        return property.getValue(properties);
    }

    private <V, T> T resolveWithDefault(ConnectionProperty<V, T> property, T defaultValue)
    {
        return property.getValueOrDefault(properties, defaultValue);
    }

    public static boolean isSecureConnection(URI uri)
    {
        return uri.getScheme().equals("https") || (uri.getScheme().equals("trino") && uri.getPort() == 443);
    }

    public ClientSession.Builder toClientSessionBuilder()
    {
        return ClientSession.builder()
                .server(getHttpUri())
                .principal(getUser())
                .path(getPath().orElse(ImmutableList.of()))
                .clientRequestTimeout(getTimeout())
                .user(getSessionUser())
                .clientTags(getClientTags().orElse(ImmutableSet.of()))
                .source(getSource().orElse(null))
                .traceToken(getTraceToken())
                .clientInfo(getClientInfo().orElse(null))
                .catalog(getCatalog().orElse(null))
                .schema(getSchema().orElse(null))
                .timeZone(getTimeZone())
                .locale(getLocale())
                .properties(getSessionProperties())
                .credentials(getExtraCredentials())
                .transactionId(null)
                .resourceEstimates(getResourceEstimates())
                .compressionDisabled(isCompressionDisabled())
                .encoding(getEncoding());
    }

    protected static Set<ConnectionProperty<?, ?>> allProperties()
    {
        // This is needed to expose properties to TrinoDriverUri
        return ConnectionProperties.allProperties();
    }

    private static Properties extractPropertiesFromUri(URI uri, List<PropertyName> restrictedProperties)
    {
        Properties result = new Properties();
        CatalogAndSchema catalogAndSchema = parseCatalogAndSchema(uri);
        if (catalogAndSchema.getCatalog().isPresent() && restrictedProperties.contains(CATALOG.getPropertyName())) {
            throw new RestrictedPropertyException(PropertyName.CATALOG, "Catalog cannot be set in the URL");
        }

        if (catalogAndSchema.getSchema().isPresent() && restrictedProperties.contains(SCHEMA.getPropertyName())) {
            throw new RestrictedPropertyException(PropertyName.SCHEMA, "Schema cannot be set in the URL");
        }

        catalogAndSchema.getCatalog().ifPresent(value -> result.put(CATALOG.getKey(), value));
        catalogAndSchema.getSchema().ifPresent(value -> result.put(SCHEMA.getKey(), value));

        if (isSecureConnection(uri)) {
            result.put(SSL.getKey(), SSL.encodeValue(true));
        }

        if (isNullOrEmpty(uri.getQuery())) {
            return result;
        }

        for (String arg : QUERY_SPLITTER.split(uri.getQuery())) {
            List<String> parts = ARG_SPLITTER.splitToList(arg);
            if (parts.size() != 2) {
                throw new RuntimeException(format("Connection argument is not a valid connection property: '%s'", parts.get(0)));
            }
            PropertyName name = PropertyName.findByKey(parts.get(0)).orElseThrow(() -> new RuntimeException(format("Unrecognized connection property '%s'", parts.get(0))));
            if (restrictedProperties.contains(name)) {
                throw new RestrictedPropertyException(name, format("Connection property %s cannot be set in the URL", name));
            }
            if (result.containsKey(parts.get(0)) && !isUrlOverridableProperty(parts.get(0))) {
                throw new RuntimeException(format("Connection property %s is in the URL multiple times", parts.get(0)));
            }
            result.put(parts.get(0), parts.get(1));
        }
        return result;
    }

    private static boolean isUrlOverridableProperty(String name)
    {
        return name.equals(SSL.getKey());
    }

    private static URI parseDriverUrl(String url)
    {
        validatePrefix(url);
        URI uri = parseUrl(url);
        if (isNullOrEmpty(uri.getHost())) {
            throw new RuntimeException("No host specified: " + url);
        }
        if (uri.getPort() == 0 || uri.getPort() > 65535) {
            throw new RuntimeException("Invalid port number: " + url);
        }
        return uri;
    }

    private static URI parseUrl(String url)
    {
        try {
            return new URI(url);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException("Invalid Trino URL: " + url, e);
        }
    }

    private static void validatePrefix(String url)
    {
        if (!url.startsWith(URL_START)) {
            throw new RuntimeException("Invalid Trino URL: " + url);
        }

        if (url.equals(URL_START)) {
            throw new RuntimeException("Empty Trino URL: " + url);
        }
    }

    private static CatalogAndSchema parseCatalogAndSchema(URI uri)
    {
        String path = uri.getPath();
        if (isNullOrEmpty(path) || path.equals("/")) {
            return new CatalogAndSchema(Optional.empty(), Optional.empty());
        }

        // remove first slash
        if (!path.startsWith("/")) {
            throw new RuntimeException("Path does not start with a slash: " + uri);
        }
        path = path.substring(1);

        List<String> parts = Splitter.on("/").splitToList(path);
        // remove last item due to a trailing slash
        if (parts.get(parts.size() - 1).isEmpty()) {
            parts = parts.subList(0, parts.size() - 1);
        }

        if (parts.size() > 2) {
            throw new RuntimeException("Invalid path segments in URL: " + uri);
        }

        if (parts.get(0).isEmpty()) {
            throw new RuntimeException("Catalog name is empty: " + uri);
        }

        Optional<String> catalogName = Optional.of(parts.get(0));
        Optional<String> schemaName = Optional.empty();

        if (parts.size() > 1) {
            if (parts.get(1).isEmpty()) {
                throw new RuntimeException("Schema name is empty: " + uri);
            }

            schemaName = Optional.of(parts.get(1));
        }

        return new CatalogAndSchema(catalogName, schemaName);
    }

    private Properties mergeConnectionProperties(Properties urlProperties, Properties properties)
    {
        for (Object key : urlProperties.keySet()) {
            if (key != SSL.getKey() && properties.containsKey(key)) {
                throw new RuntimeException(format("Connection property %s is passed both by URL and properties", key));
            }
        }

        Properties result = new Properties();
        result.putAll(urlProperties);
        // Order is important if i.e. explicitly passed properties disables SSL
        result.putAll(properties);

        return result;
    }

    private void validateConnectionProperties(Properties connectionProperties)
    {
        ImmutableList.Builder<RuntimeException> violations = ImmutableList.builder();

        for (String propertyName : connectionProperties.stringPropertyNames()) {
            if (ConnectionProperties.forKey(propertyName) == null) {
                violations.add(new IllegalArgumentException(format("Unrecognized connection property '%s'", propertyName)));
            }
        }

        for (ConnectionProperty<?, ?> property : allProperties()) {
            Optional<RuntimeException> validationError = property.validate(connectionProperties);
            validationError.ifPresent(violations::add);
        }

        if (hasPassword() && !isUseSecureConnection()) {
            violations.add(new IllegalStateException("TLS/SSL is required for authentication with username and password"));
        }

        List<RuntimeException> errors = violations.build();
        if (errors.size() == 1) {
            throw errors.get(0);
        }
        else if (!errors.isEmpty()) {
            String multipleViolations = errors.stream()
                    .map(RuntimeException::getMessage)
                    .sorted(CASE_INSENSITIVE_ORDER) // To make tests assertions predictable
                    .collect(joining("\n"));

            throw new RuntimeException("Provided connection properties are invalid:\n" + multipleViolations);
        }
    }

    @VisibleForTesting
    public static void setRedirectHandler(RedirectHandler handler)
    {
        REDIRECT_HANDLER.set(requireNonNull(handler, "handler is null"));
    }

    public static Optional<RedirectHandler> getRedirectHandler()
    {
        return Optional.ofNullable(REDIRECT_HANDLER.get());
    }

    private static class CatalogAndSchema
    {
        private final Optional<String> catalog;
        private final Optional<String> schema;

        public CatalogAndSchema(Optional<String> catalog, Optional<String> schema)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.schema = requireNonNull(schema, "schema is null");
        }

        public Optional<String> getCatalog()
        {
            return catalog;
        }

        public Optional<String> getSchema()
        {
            return schema;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(TrinoUri trinoUri)
    {
        return new Builder()
                .setUri(trinoUri.uri)
                .setProperties(trinoUri.properties)
                .setRestrictedProperties(trinoUri.restrictedProperties);
    }

    public static final class Builder
    {
        private URI uri;
        private List<PropertyName> restrictedProperties = ImmutableList.of();
        private ImmutableMap.Builder<Object, Object> properties = ImmutableMap.builder();

        private Builder() {}

        public Builder setUri(URI uri)
        {
            this.uri = requireNonNull(uri, "uri is null");
            return this;
        }

        public Builder setCatalog(String catalog)
        {
            return setProperty(CATALOG, requireNonNull(catalog, "catalog is null"));
        }

        public Builder setSchema(String schema)
        {
            return setProperty(SCHEMA, requireNonNull(schema, "schema is null"));
        }

        public Builder setRestrictedProperties(List<PropertyName> restrictedProperties)
        {
            this.restrictedProperties = requireNonNull(restrictedProperties, "restrictedProperties is null");
            return this;
        }

        public Builder setUser(String user)
        {
            return setProperty(USER, requireNonNull(user, "user is null"));
        }

        public Builder setPassword(String password)
        {
            return setProperty(PASSWORD, requireNonNull(password, "password is null"));
        }

        public Builder setSessionUser(String sessionUser)
        {
            return setProperty(SESSION_USER, requireNonNull(sessionUser, "sessionUser is null"));
        }

        public Builder setRoles(Map<String, ClientSelectedRole> roles)
        {
            return setProperty(ROLES, requireNonNull(roles, "roles is null"));
        }

        public Builder setSocksProxy(HostAndPort socksProxy)
        {
            return setProperty(SOCKS_PROXY, requireNonNull(socksProxy, "socksProxy is null"));
        }

        public Builder setHttpProxy(HostAndPort httpProxy)
        {
            return setProperty(HTTP_PROXY, requireNonNull(httpProxy, "httpProxy is null"));
        }

        public Builder setApplicationNamePrefix(String applicationNamePrefix)
        {
            return setProperty(APPLICATION_NAME_PREFIX, requireNonNull(applicationNamePrefix, "applicationNamePrefix is null"));
        }

        public Builder setDisableCompression(Boolean disableCompression)
        {
            return setProperty(DISABLE_COMPRESSION, requireNonNull(disableCompression, "disableCompression is null"));
        }

        public Builder setEncoding(String encoding)
        {
            return setProperty(ENCODING, requireNonNull(encoding, "encoding is null"));
        }

        public Builder setAssumeLiteralNamesInMetadataCallsForNonConformingClients(boolean value)
        {
            return setProperty(ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS, value);
        }

        public Builder setAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients(boolean value)
        {
            return setProperty(ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS, value);
        }

        public Builder setSsl(Boolean ssl)
        {
            return setProperty(SSL, requireNonNull(ssl, "ssl is null"));
        }

        public Builder setSslKeyStorePath(String sslKeyStorePath)
        {
            return setProperty(SSL_KEY_STORE_PATH, requireNonNull(sslKeyStorePath, "sslKeyStorePath is null"));
        }

        public Builder setSslKeyStorePassword(String sslKeyStorePassword)
        {
            return setProperty(SSL_KEY_STORE_PASSWORD, requireNonNull(sslKeyStorePassword, "sslKeyStorePassword is null"));
        }

        public Builder setSslKeyStoreType(String sslKeyStoreType)
        {
            return setProperty(SSL_KEY_STORE_TYPE, requireNonNull(sslKeyStoreType, "sslKeyStoreType is null"));
        }

        public Builder setSslUseSystemKeyStore(boolean sslUseSystemKeyStore)
        {
            return setProperty(SSL_USE_SYSTEM_KEY_STORE, sslUseSystemKeyStore);
        }

        public Builder setSslTrustStorePath(String sslTrustStorePath)
        {
            return setProperty(SSL_TRUST_STORE_PATH, requireNonNull(sslTrustStorePath, "sslTrustStorePath is null"));
        }

        public Builder setSslTrustStorePassword(String sslTrustStorePassword)
        {
            return setProperty(SSL_TRUST_STORE_PASSWORD, requireNonNull(sslTrustStorePassword, "sslTrustStorePassword is null"));
        }

        public Builder setSslTrustStoreType(String sslTrustStoreType)
        {
            return setProperty(SSL_TRUST_STORE_TYPE, requireNonNull(sslTrustStoreType, "sslTrustStoreType is null"));
        }

        public Builder setSslUseSystemTrustStore(Boolean sslUseSystemTrustStore)
        {
            return setProperty(SSL_USE_SYSTEM_TRUST_STORE, requireNonNull(sslUseSystemTrustStore, "sslUseSystemTrustStore is null"));
        }

        public Builder setKerberosServicePrincipalPattern(String kerberosServicePrincipalPattern)
        {
            return setProperty(KERBEROS_SERVICE_PRINCIPAL_PATTERN, requireNonNull(kerberosServicePrincipalPattern, "kerberosServicePrincipalPattern is null"));
        }

        public Builder setKerberosUseCanonicalHostname(Boolean kerberosUseCanonicalHostname)
        {
            return setProperty(KERBEROS_USE_CANONICAL_HOSTNAME, requireNonNull(kerberosUseCanonicalHostname, "kerberosUseCanonicalHostname is null"));
        }

        public Builder setKerberosPrincipal(String kerberosPrincipal)
        {
            return setProperty(KERBEROS_PRINCIPAL, requireNonNull(kerberosPrincipal, "kerberosPrincipal is null"));
        }

        public Builder setKerberosConfigPath(String kerberosConfigPath)
        {
            return setKerberosConfigPath(new File(requireNonNull(kerberosConfigPath, "kerberosConfigPath is null")));
        }

        public Builder setKerberosConfigPath(File kerberosConfigPath)
        {
            return setProperty(KERBEROS_CONFIG_PATH, requireNonNull(kerberosConfigPath, "kerberosConfigPath is null"));
        }

        public Builder setKerberosKeytabPath(String kerberosKeytabPath)
        {
            return setKerberosKeytabPath(new File(requireNonNull(kerberosKeytabPath, "kerberosKeytabPath is null")));
        }

        public Builder setKerberosKeytabPath(File kerberosKeytabPath)
        {
            return setProperty(KERBEROS_KEYTAB_PATH, requireNonNull(kerberosKeytabPath, "kerberosKeytabPath is null"));
        }

        public Builder setKerberosCredentialCachePath(String kerberosCredentialCachePath)
        {
            return setKerberosCredentialCachePath(new File(requireNonNull(kerberosCredentialCachePath, "kerberosCredentialCachePath is null")));
        }

        public Builder setKerberosCredentialCachePath(File kerberosCredentialCachePath)
        {
            return setProperty(KERBEROS_CREDENTIAL_CACHE_PATH, requireNonNull(kerberosCredentialCachePath, "kerberosCredentialCachePath is null"));
        }

        public Builder setKerberosDelegation(Boolean kerberosDelegation)
        {
            return setProperty(KERBEROS_DELEGATION, requireNonNull(kerberosDelegation, "kerberosDelegation is null"));
        }

        public Builder setKerberosConstrainedDelegation(GSSCredential kerberosConstrainedDelegation)
        {
            return setProperty(KERBEROS_CONSTRAINED_DELEGATION, requireNonNull(kerberosConstrainedDelegation, "kerberosConstrainedDelegation is null"));
        }

        public Builder setAccessToken(String accessToken)
        {
            return setProperty(ACCESS_TOKEN, requireNonNull(accessToken, "accessToken is null"));
        }

        public Builder setExternalAuthentication(Boolean externalAuthentication)
        {
            return setProperty(EXTERNAL_AUTHENTICATION, requireNonNull(externalAuthentication, "externalAuthentication is null"));
        }

        public Builder setExternalAuthenticationTimeout(io.airlift.units.Duration externalAuthenticationTimeout)
        {
            return setProperty(EXTERNAL_AUTHENTICATION_TIMEOUT, requireNonNull(externalAuthenticationTimeout, "externalAuthenticationTimeout is null"));
        }

        public Builder setExternalAuthenticationRedirectHandlers(List<ExternalRedirectStrategy> externalRedirectStrategies)
        {
            return setProperty(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS, requireNonNull(externalRedirectStrategies, "externalRedirectStrategies is null"));
        }

        public Builder setExternalAuthenticationTokenCache(KnownTokenCache externalAuthenticationTokenCache)
        {
            return setProperty(EXTERNAL_AUTHENTICATION_TOKEN_CACHE, requireNonNull(externalAuthenticationTokenCache, "externalAuthenticationTokenCache is null"));
        }

        public Builder setExtraCredentials(Map<String, String> extraCredentials)
        {
            return setProperty(EXTRA_CREDENTIALS, requireNonNull(extraCredentials, "extraCredentials is null"));
        }

        public Builder setHostnameInCertificate(String hostnameInCertificate)
        {
            return setProperty(HOSTNAME_IN_CERTIFICATE, requireNonNull(hostnameInCertificate, "hostnameInCertificate is null"));
        }

        public Builder setTimeZone(ZoneId zoneId)
        {
            return setProperty(TIMEZONE, requireNonNull(zoneId, "zoneId is null"));
        }

        public Builder setSslVerification(SslVerificationMode sslVerification)
        {
            return setProperty(SSL_VERIFICATION, requireNonNull(sslVerification, "sslVerification is null"));
        }

        public Builder setSslVerificationNone()
        {
            return setProperty(SSL_VERIFICATION, SslVerificationMode.NONE);
        }

        public Builder setLocale(Locale locale)
        {
            return setProperty(LOCALE, requireNonNull(locale, "locale is null"));
        }

        public Builder setClientInfo(String clientInfo)
        {
            return setProperty(CLIENT_INFO, requireNonNull(clientInfo, "clientInfo is null"));
        }

        public Builder setClientTags(Set<String> clientTags)
        {
            return setProperty(CLIENT_TAGS, requireNonNull(clientTags, "clientTags is null"));
        }

        public Builder setTraceToken(String traceToken)
        {
            return setProperty(TRACE_TOKEN, requireNonNull(traceToken, "traceToken is null"));
        }

        public Builder setSessionProperties(Map<String, String> sessionProperties)
        {
            return setProperty(SESSION_PROPERTIES, requireNonNull(sessionProperties, "sessionProperties is null"));
        }

        public Builder setSource(String source)
        {
            return setProperty(SOURCE, requireNonNull(source, "source is null"));
        }

        public Builder setExplicitPrepare(boolean explicitPrepare)
        {
            return setProperty(EXPLICIT_PREPARE, explicitPrepare);
        }

        public Builder setAssumeNullCatalogMeansCurrentCatalog(boolean assumeNullCatalogMeansCurrentCatalog)
        {
            return setProperty(ASSUME_NULL_CATALOG_MEANS_CURRENT_CATALOG, assumeNullCatalogMeansCurrentCatalog);
        }

        public Builder setKerberosRemoteServiceName(String kerberosRemoteServiceName)
        {
            return setProperty(KERBEROS_REMOTE_SERVICE_NAME, requireNonNull(kerberosRemoteServiceName, "kerberosRemoteServiceName is null"));
        }

        public Builder setDnsResolverContext(String dnsResolverContext)
        {
            return setProperty(DNS_RESOLVER_CONTEXT, requireNonNull(dnsResolverContext, "dnsResolverContext is null"));
        }

        public Builder setDnsResolver(Class<? extends DnsResolver> dnsResolver)
        {
            return setProperty(DNS_RESOLVER, requireNonNull(dnsResolver, "dnsResolver is null"));
        }

        public Builder setTimeout(Duration timeout)
        {
            return setProperty(TIMEOUT, requireNonNull(timeout, "timeout is null"));
        }

        public Builder setHttpLoggingLevel(LoggingLevel level)
        {
            return setProperty(HTTP_LOGGING_LEVEL, requireNonNull(level, "level is null"));
        }

        public Builder setResourceEstimates(Map<String, String> resourceEstimates)
        {
            return setProperty(RESOURCE_ESTIMATES, requireNonNull(resourceEstimates, "resourceEstimates is null"));
        }

        public Builder setPath(List<String> path)
        {
            return setProperty(SQL_PATH, requireNonNull(path, "path is null"));
        }

        <V, T> Builder setProperty(ConnectionProperty<V, T> connectionProperty, T value)
        {
            properties.put(connectionProperty.getKey(), connectionProperty.encodeValue(value));
            return this;
        }

        <T> Builder setProperties(Map<ConnectionProperty<?, T>, T> values)
        {
            values.forEach(this::setProperty);
            return this;
        }

        Builder setProperties(Properties properties)
        {
            this.properties.putAll(properties);
            return this;
        }

        public TrinoUri build()
        {
            return new TrinoUri(restrictedProperties, uri, toProperties(properties.buildOrThrow()));
        }

        private Properties toProperties(Map<Object, Object> values)
        {
            Properties properties = new Properties();
            properties.putAll(values);
            return properties;
        }
    }
}
