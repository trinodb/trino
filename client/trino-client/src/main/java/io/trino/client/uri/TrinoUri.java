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
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.trino.client.ClientException;
import io.trino.client.ClientSelectedRole;
import io.trino.client.DnsResolver;
import io.trino.client.OkHttpUtil;
import io.trino.client.auth.external.CompositeRedirectHandler;
import io.trino.client.auth.external.ExternalAuthenticator;
import io.trino.client.auth.external.ExternalRedirectStrategy;
import io.trino.client.auth.external.HttpTokenPoller;
import io.trino.client.auth.external.RedirectHandler;
import io.trino.client.auth.external.TokenPoller;
import okhttp3.OkHttpClient;
import org.ietf.jgss.GSSCredential;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.client.KerberosUtil.defaultCredentialCachePath;
import static io.trino.client.OkHttpUtil.basicAuth;
import static io.trino.client.OkHttpUtil.setupAlternateHostnameVerification;
import static io.trino.client.OkHttpUtil.setupCookieJar;
import static io.trino.client.OkHttpUtil.setupHttpProxy;
import static io.trino.client.OkHttpUtil.setupInsecureSsl;
import static io.trino.client.OkHttpUtil.setupKerberos;
import static io.trino.client.OkHttpUtil.setupSocksProxy;
import static io.trino.client.OkHttpUtil.setupSsl;
import static io.trino.client.OkHttpUtil.tokenAuth;
import static io.trino.client.uri.ConnectionProperties.ACCESS_TOKEN;
import static io.trino.client.uri.ConnectionProperties.APPLICATION_NAME_PREFIX;
import static io.trino.client.uri.ConnectionProperties.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS;
import static io.trino.client.uri.ConnectionProperties.ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS;
import static io.trino.client.uri.ConnectionProperties.CLIENT_INFO;
import static io.trino.client.uri.ConnectionProperties.CLIENT_TAGS;
import static io.trino.client.uri.ConnectionProperties.DISABLE_COMPRESSION;
import static io.trino.client.uri.ConnectionProperties.DNS_RESOLVER;
import static io.trino.client.uri.ConnectionProperties.DNS_RESOLVER_CONTEXT;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION_TIMEOUT;
import static io.trino.client.uri.ConnectionProperties.EXTERNAL_AUTHENTICATION_TOKEN_CACHE;
import static io.trino.client.uri.ConnectionProperties.EXTRA_CREDENTIALS;
import static io.trino.client.uri.ConnectionProperties.HOSTNAME_IN_CERTIFICATE;
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
import static io.trino.client.uri.ConnectionProperties.PASSWORD;
import static io.trino.client.uri.ConnectionProperties.ROLES;
import static io.trino.client.uri.ConnectionProperties.SESSION_PROPERTIES;
import static io.trino.client.uri.ConnectionProperties.SESSION_USER;
import static io.trino.client.uri.ConnectionProperties.SOCKS_PROXY;
import static io.trino.client.uri.ConnectionProperties.SOURCE;
import static io.trino.client.uri.ConnectionProperties.SSL;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_PASSWORD;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_PATH;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_TYPE;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_TYPE;
import static io.trino.client.uri.ConnectionProperties.SSL_USE_SYSTEM_TRUST_STORE;
import static io.trino.client.uri.ConnectionProperties.SSL_VERIFICATION;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.CA;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.FULL;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.NONE;
import static io.trino.client.uri.ConnectionProperties.TRACE_TOKEN;
import static io.trino.client.uri.ConnectionProperties.USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Parses and extracts parameters from a Trino URL.
 */
public class TrinoUri
{
    private static final String URL_START = "trino:";

    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);
    private static final AtomicReference<RedirectHandler> REDIRECT_HANDLER = new AtomicReference<>(null);
    private final HostAndPort address;
    private final URI uri;

    private final Properties properties;

    private Optional<String> user;
    private Optional<String> password;
    private Optional<String> sessionUser;
    private Optional<Map<String, ClientSelectedRole>> roles;
    private Optional<HostAndPort> socksProxy;
    private Optional<HostAndPort> httpProxy;
    private Optional<String> applicationNamePrefix;
    private Optional<Boolean> disableCompression;
    private Optional<Boolean> assumeLiteralNamesInMetadataCallsForNonConformingClients;
    private Optional<Boolean> assumeLiteralUnderscoreInMetadataCallsForNonConformingClients;
    private Optional<Boolean> ssl;
    private Optional<SslVerificationMode> sslVerification;
    private Optional<String> sslKeyStorePath;
    private Optional<String> sslKeyStorePassword;
    private Optional<String> sslKeyStoreType;
    private Optional<String> sslTrustStorePath;
    private Optional<String> sslTrustStorePassword;
    private Optional<String> sslTrustStoreType;
    private Optional<Boolean> sslUseSystemTrustStore;
    private Optional<String> kerberosServicePrincipalPattern;
    private Optional<String> kerberosRemoteServiceName;
    private Optional<Boolean> kerberosUseCanonicalHostname;
    private Optional<String> kerberosPrincipal;
    private Optional<File> kerberosConfigPath;
    private Optional<File> kerberosKeytabPath;
    private Optional<File> kerberosCredentialCachePath;
    private Optional<Boolean> kerberosDelegation;
    private Optional<GSSCredential> kerberosConstrainedDelegation;
    private Optional<String> accessToken;
    private Optional<Boolean> externalAuthentication;
    private Optional<io.airlift.units.Duration> externalAuthenticationTimeout;
    private Optional<List<ExternalRedirectStrategy>> externalRedirectStrategies;
    private Optional<KnownTokenCache> externalAuthenticationTokenCache;
    private Optional<Map<String, String>> extraCredentials;
    private Optional<String> hostnameInCertificate;
    private Optional<String> clientInfo;
    private Optional<String> clientTags;
    private Optional<String> traceToken;
    private Optional<Map<String, String>> sessionProperties;
    private Optional<String> source;

    private Optional<String> catalog = Optional.empty();
    private Optional<String> schema = Optional.empty();
    private final List<PropertyName> restrictedProperties;

    private final boolean useSecureConnection;

    private TrinoUri(
            URI uri,
            Optional<String> catalog,
            Optional<String> schema,
            List<PropertyName> restrictedProperties,
            Optional<String> user,
            Optional<String> password,
            Optional<String> sessionUser,
            Optional<Map<String, ClientSelectedRole>> roles,
            Optional<HostAndPort> socksProxy,
            Optional<HostAndPort> httpProxy,
            Optional<String> applicationNamePrefix,
            Optional<Boolean> disableCompression,
            Optional<Boolean> assumeLiteralNamesInMetadataCallsForNonConformingClients,
            Optional<Boolean> assumeLiteralUnderscoreInMetadataCallsForNonConformingClients,
            Optional<Boolean> ssl,
            Optional<SslVerificationMode> sslVerification,
            Optional<String> sslKeyStorePath,
            Optional<String> sslKeyStorePassword,
            Optional<String> sslKeyStoreType,
            Optional<String> sslTrustStorePath,
            Optional<String> sslTrustStorePassword,
            Optional<String> sslTrustStoreType,
            Optional<Boolean> sslUseSystemTrustStore,
            Optional<String> kerberosServicePrincipalPattern,
            Optional<String> kerberosRemoteServiceName,
            Optional<Boolean> kerberosUseCanonicalHostname,
            Optional<String> kerberosPrincipal,
            Optional<File> kerberosConfigPath,
            Optional<File> kerberosKeytabPath,
            Optional<File> kerberosCredentialCachePath,
            Optional<Boolean> kerberosDelegation,
            Optional<GSSCredential> kerberosConstrainedDelegation,
            Optional<String> accessToken,
            Optional<Boolean> externalAuthentication,
            Optional<io.airlift.units.Duration> externalAuthenticationTimeout,
            Optional<List<ExternalRedirectStrategy>> externalRedirectStrategies,
            Optional<KnownTokenCache> externalAuthenticationTokenCache,
            Optional<Map<String, String>> extraCredentials,
            Optional<String> hostnameInCertificate,
            Optional<String> clientInfo,
            Optional<String> clientTags,
            Optional<String> traceToken,
            Optional<Map<String, String>> sessionProperties,
            Optional<String> source)
            throws SQLException
    {
        this.uri = requireNonNull(uri, "uri is null");
        this.catalog = catalog;
        this.schema = schema;
        this.restrictedProperties = restrictedProperties;

        Map<String, Object> urlParameters = parseParameters(uri.getQuery());
        Properties urlProperties = new Properties();
        urlProperties.putAll(urlParameters);

        this.user = USER.getValueOrDefault(urlProperties, user);
        this.password = PASSWORD.getValueOrDefault(urlProperties, password);
        this.sessionUser = SESSION_USER.getValueOrDefault(urlProperties, sessionUser);
        this.roles = ROLES.getValueOrDefault(urlProperties, roles);
        this.socksProxy = SOCKS_PROXY.getValueOrDefault(urlProperties, socksProxy);
        this.httpProxy = HTTP_PROXY.getValueOrDefault(urlProperties, httpProxy);
        this.applicationNamePrefix = APPLICATION_NAME_PREFIX.getValueOrDefault(urlProperties, applicationNamePrefix);
        this.disableCompression = DISABLE_COMPRESSION.getValueOrDefault(urlProperties, disableCompression);
        this.assumeLiteralNamesInMetadataCallsForNonConformingClients = ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.getValueOrDefault(urlProperties, assumeLiteralNamesInMetadataCallsForNonConformingClients);
        this.assumeLiteralUnderscoreInMetadataCallsForNonConformingClients = ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.getValueOrDefault(urlProperties, assumeLiteralUnderscoreInMetadataCallsForNonConformingClients);
        this.ssl = SSL.getValueOrDefault(urlProperties, ssl);
        this.sslVerification = SSL_VERIFICATION.getValueOrDefault(urlProperties, sslVerification);
        this.sslKeyStorePath = SSL_KEY_STORE_PATH.getValueOrDefault(urlProperties, sslKeyStorePath);
        this.sslKeyStorePassword = SSL_KEY_STORE_PASSWORD.getValueOrDefault(urlProperties, sslKeyStorePassword);
        this.sslKeyStoreType = SSL_KEY_STORE_TYPE.getValueOrDefault(urlProperties, sslKeyStoreType);
        this.sslTrustStorePath = SSL_TRUST_STORE_PATH.getValueOrDefault(urlProperties, sslTrustStorePath);
        this.sslTrustStorePassword = SSL_TRUST_STORE_PASSWORD.getValueOrDefault(urlProperties, sslTrustStorePassword);
        this.sslTrustStoreType = SSL_TRUST_STORE_TYPE.getValueOrDefault(urlProperties, sslTrustStoreType);
        this.sslUseSystemTrustStore = SSL_USE_SYSTEM_TRUST_STORE.getValueOrDefault(urlProperties, sslUseSystemTrustStore);
        this.kerberosServicePrincipalPattern = KERBEROS_SERVICE_PRINCIPAL_PATTERN.getValueOrDefault(urlProperties, kerberosServicePrincipalPattern);
        this.kerberosRemoteServiceName = KERBEROS_REMOTE_SERVICE_NAME.getValueOrDefault(urlProperties, kerberosRemoteServiceName);
        this.kerberosUseCanonicalHostname = KERBEROS_USE_CANONICAL_HOSTNAME.getValueOrDefault(urlProperties, kerberosUseCanonicalHostname);
        this.kerberosPrincipal = KERBEROS_PRINCIPAL.getValueOrDefault(urlProperties, kerberosPrincipal);
        this.kerberosConfigPath = KERBEROS_CONFIG_PATH.getValueOrDefault(urlProperties, kerberosConfigPath);
        this.kerberosKeytabPath = KERBEROS_KEYTAB_PATH.getValueOrDefault(urlProperties, kerberosKeytabPath);
        this.kerberosCredentialCachePath = KERBEROS_CREDENTIAL_CACHE_PATH.getValueOrDefault(urlProperties, kerberosCredentialCachePath);
        this.kerberosDelegation = KERBEROS_DELEGATION.getValueOrDefault(urlProperties, kerberosDelegation);
        this.kerberosConstrainedDelegation = KERBEROS_CONSTRAINED_DELEGATION.getValueOrDefault(urlProperties, kerberosConstrainedDelegation);
        this.accessToken = ACCESS_TOKEN.getValueOrDefault(urlProperties, accessToken);
        this.externalAuthentication = EXTERNAL_AUTHENTICATION.getValueOrDefault(urlProperties, externalAuthentication);
        this.externalAuthenticationTimeout = EXTERNAL_AUTHENTICATION_TIMEOUT.getValueOrDefault(urlProperties, externalAuthenticationTimeout);
        this.externalRedirectStrategies = EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS.getValueOrDefault(urlProperties, externalRedirectStrategies);
        this.externalAuthenticationTokenCache = EXTERNAL_AUTHENTICATION_TOKEN_CACHE.getValueOrDefault(urlProperties, externalAuthenticationTokenCache);
        this.extraCredentials = EXTRA_CREDENTIALS.getValueOrDefault(urlProperties, extraCredentials);
        this.hostnameInCertificate = HOSTNAME_IN_CERTIFICATE.getValueOrDefault(urlProperties, hostnameInCertificate);
        this.clientInfo = CLIENT_INFO.getValueOrDefault(urlProperties, clientInfo);
        this.clientTags = CLIENT_TAGS.getValueOrDefault(urlProperties, clientTags);
        this.traceToken = TRACE_TOKEN.getValueOrDefault(urlProperties, traceToken);
        this.sessionProperties = SESSION_PROPERTIES.getValueOrDefault(urlProperties, sessionProperties);
        this.source = SOURCE.getValueOrDefault(urlProperties, source);

        properties = buildProperties();

        // enable SSL by default for the trino schema and the standard port
        useSecureConnection = SSL.getValue(properties).orElse(uri.getScheme().equals("https") || (uri.getScheme().equals("trino") && uri.getPort() == 443));
        if (!password.orElse("").isEmpty()) {
            if (!useSecureConnection) {
                throw new SQLException("TLS/SSL required for authentication with username and password");
            }
        }
        validateConnectionProperties(properties);

        this.address = HostAndPort.fromParts(uri.getHost(), uri.getPort() == -1 ? (useSecureConnection ? 443 : 80) : uri.getPort());
        initCatalogAndSchema();
    }

    private Properties buildProperties()
    {
        Properties properties = new Properties();
        user.ifPresent(value -> properties.setProperty(PropertyName.USER.toString(), value));
        password.ifPresent(value -> properties.setProperty(PropertyName.PASSWORD.toString(), value));
        sessionUser.ifPresent(value -> properties.setProperty(PropertyName.SESSION_USER.toString(), value));
        roles.ifPresent(value -> properties.setProperty(
                PropertyName.ROLES.toString(),
                value.entrySet().stream()
                        .map(entry -> entry.getKey() + ":" + entry.getValue())
                        .collect(Collectors.joining(";"))));
        socksProxy.ifPresent(value -> properties.setProperty(PropertyName.SOCKS_PROXY.toString(), value.toString()));
        httpProxy.ifPresent(value -> properties.setProperty(PropertyName.HTTP_PROXY.toString(), value.toString()));
        applicationNamePrefix.ifPresent(value -> properties.setProperty(PropertyName.APPLICATION_NAME_PREFIX.toString(), value));
        disableCompression.ifPresent(value -> properties.setProperty(PropertyName.DISABLE_COMPRESSION.toString(), Boolean.toString(value)));
        assumeLiteralNamesInMetadataCallsForNonConformingClients.ifPresent(
                value -> properties.setProperty(
                        PropertyName.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.toString(),
                        Boolean.toString(value)));
        assumeLiteralUnderscoreInMetadataCallsForNonConformingClients.ifPresent(
                value -> properties.setProperty(
                        PropertyName.ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.toString(),
                        Boolean.toString(value)));
        ssl.ifPresent(value -> properties.setProperty(PropertyName.SSL.toString(), Boolean.toString(value)));
        sslVerification.ifPresent(value -> properties.setProperty(PropertyName.SSL_VERIFICATION.toString(), value.toString()));
        sslKeyStoreType.ifPresent(value -> properties.setProperty(PropertyName.SSL_KEY_STORE_TYPE.toString(), value));
        sslKeyStorePath.ifPresent(value -> properties.setProperty(PropertyName.SSL_KEY_STORE_PATH.toString(), value));
        sslKeyStorePassword.ifPresent(value -> properties.setProperty(PropertyName.SSL_KEY_STORE_PASSWORD.toString(), value));
        sslTrustStoreType.ifPresent(value -> properties.setProperty(PropertyName.SSL_TRUST_STORE_TYPE.toString(), value));
        sslTrustStorePath.ifPresent(value -> properties.setProperty(PropertyName.SSL_TRUST_STORE_PATH.toString(), value));
        sslTrustStorePassword.ifPresent(value -> properties.setProperty(PropertyName.SSL_TRUST_STORE_PASSWORD.toString(), value));
        sslUseSystemTrustStore.ifPresent(value -> properties.setProperty(PropertyName.SSL_USE_SYSTEM_TRUST_STORE.toString(), Boolean.toString(value)));
        kerberosServicePrincipalPattern.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_SERVICE_PRINCIPAL_PATTERN.toString(), value));
        kerberosRemoteServiceName.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_REMOTE_SERVICE_NAME.toString(), value));
        kerberosUseCanonicalHostname.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_USE_CANONICAL_HOSTNAME.toString(), Boolean.toString(value)));
        kerberosPrincipal.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_PRINCIPAL.toString(), value));
        kerberosConfigPath.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_CONFIG_PATH.toString(), value.getPath()));
        kerberosKeytabPath.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_KEYTAB_PATH.toString(), value.getPath()));
        kerberosCredentialCachePath.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_CREDENTIAL_CACHE_PATH.toString(), value.getPath()));
        kerberosDelegation.ifPresent(value -> properties.setProperty(PropertyName.KERBEROS_DELEGATION.toString(), Boolean.toString(value)));
        kerberosConstrainedDelegation.ifPresent(value -> properties.put(PropertyName.KERBEROS_CONSTRAINED_DELEGATION.toString(), value));
        accessToken.ifPresent(value -> properties.setProperty(PropertyName.ACCESS_TOKEN.toString(), value));
        externalAuthentication.ifPresent(value -> properties.setProperty(PropertyName.EXTERNAL_AUTHENTICATION.toString(), Boolean.toString(value)));
        externalAuthenticationTimeout.ifPresent(value -> properties.setProperty(PropertyName.EXTERNAL_AUTHENTICATION_TIMEOUT.toString(), value.toString()));
        externalRedirectStrategies.ifPresent(value ->
                properties.setProperty(
                        PropertyName.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS.toString(),
                        value.stream()
                                .map(ExternalRedirectStrategy::toString)
                                .collect(Collectors.joining(","))));
        externalAuthenticationTokenCache.ifPresent(value -> properties.setProperty(PropertyName.EXTERNAL_AUTHENTICATION_TOKEN_CACHE.toString(), value.toString()));
        extraCredentials.ifPresent(value ->
                properties.setProperty(
                        PropertyName.EXTRA_CREDENTIALS.toString(),
                        value.entrySet().stream()
                                .map(entry -> entry.getKey() + ":" + entry.getValue())
                                .collect(Collectors.joining(";"))));
        sessionProperties.ifPresent(value ->
                properties.setProperty(
                        PropertyName.SESSION_PROPERTIES.toString(),
                        value.entrySet().stream()
                                .map(entry -> entry.getKey() + ":" + entry.getValue())
                                .collect(Collectors.joining(";"))));
        hostnameInCertificate.ifPresent(value -> properties.setProperty(PropertyName.HOSTNAME_IN_CERTIFICATE.toString(), value));
        clientInfo.ifPresent(value -> properties.setProperty(PropertyName.CLIENT_INFO.toString(), value));
        clientTags.ifPresent(value -> properties.setProperty(PropertyName.CLIENT_TAGS.toString(), value));
        traceToken.ifPresent(value -> properties.setProperty(PropertyName.TRACE_TOKEN.toString(), value));
        source.ifPresent(value -> properties.setProperty(PropertyName.SOURCE.toString(), value));
        return properties;
    }

    protected TrinoUri(String url, Properties properties)
            throws SQLException
    {
        this(parseDriverUrl(url), properties);
    }

    protected TrinoUri(URI uri, Properties driverProperties)
            throws SQLException
    {
        this.restrictedProperties = Collections.emptyList();
        this.uri = requireNonNull(uri, "uri is null");
        properties = mergeConnectionProperties(uri, driverProperties);

        validateConnectionProperties(properties);

        this.user = USER.getValue(properties);
        this.password = PASSWORD.getValue(properties);
        this.sessionUser = SESSION_USER.getValue(properties);
        this.roles = ROLES.getValue(properties);
        this.socksProxy = SOCKS_PROXY.getValue(properties);
        this.httpProxy = HTTP_PROXY.getValue(properties);
        this.applicationNamePrefix = APPLICATION_NAME_PREFIX.getValue(properties);
        this.disableCompression = DISABLE_COMPRESSION.getValue(properties);
        this.assumeLiteralNamesInMetadataCallsForNonConformingClients = ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.getValue(properties);
        this.assumeLiteralUnderscoreInMetadataCallsForNonConformingClients = ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.getValue(properties);
        this.ssl = SSL.getValue(properties);
        this.sslVerification = SSL_VERIFICATION.getValue(properties);
        this.sslKeyStorePath = SSL_KEY_STORE_PATH.getValue(properties);
        this.sslKeyStorePassword = SSL_KEY_STORE_PASSWORD.getValue(properties);
        this.sslKeyStoreType = SSL_KEY_STORE_TYPE.getValue(properties);
        this.sslTrustStorePath = SSL_TRUST_STORE_PATH.getValue(properties);
        this.sslTrustStorePassword = SSL_TRUST_STORE_PASSWORD.getValue(properties);
        this.sslTrustStoreType = SSL_TRUST_STORE_TYPE.getValue(properties);
        this.sslUseSystemTrustStore = SSL_USE_SYSTEM_TRUST_STORE.getValue(properties);
        this.kerberosServicePrincipalPattern = KERBEROS_SERVICE_PRINCIPAL_PATTERN.getValue(properties);
        this.kerberosRemoteServiceName = KERBEROS_REMOTE_SERVICE_NAME.getValue(properties);
        this.kerberosUseCanonicalHostname = KERBEROS_USE_CANONICAL_HOSTNAME.getValue(properties);
        this.kerberosPrincipal = KERBEROS_PRINCIPAL.getValue(properties);
        this.kerberosConfigPath = KERBEROS_CONFIG_PATH.getValue(properties);
        this.kerberosKeytabPath = KERBEROS_KEYTAB_PATH.getValue(properties);
        this.kerberosCredentialCachePath = KERBEROS_CREDENTIAL_CACHE_PATH.getValue(properties);
        this.kerberosDelegation = KERBEROS_DELEGATION.getValue(properties);
        this.kerberosConstrainedDelegation = KERBEROS_CONSTRAINED_DELEGATION.getValue(properties);
        this.accessToken = ACCESS_TOKEN.getValue(properties);
        this.externalAuthentication = EXTERNAL_AUTHENTICATION.getValue(properties);
        this.externalAuthenticationTimeout = EXTERNAL_AUTHENTICATION_TIMEOUT.getValue(properties);
        this.externalRedirectStrategies = EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS.getValue(properties);
        this.externalAuthenticationTokenCache = EXTERNAL_AUTHENTICATION_TOKEN_CACHE.getValue(properties);
        this.extraCredentials = EXTRA_CREDENTIALS.getValue(properties);
        this.hostnameInCertificate = HOSTNAME_IN_CERTIFICATE.getValue(properties);
        this.clientInfo = CLIENT_INFO.getValue(properties);
        this.clientTags = CLIENT_TAGS.getValue(properties);
        this.traceToken = TRACE_TOKEN.getValue(properties);
        this.sessionProperties = SESSION_PROPERTIES.getValue(properties);
        this.source = SOURCE.getValue(properties);

        // enable SSL by default for the trino schema and the standard port
        useSecureConnection = ssl.orElse(uri.getScheme().equals("https") || (uri.getScheme().equals("trino") && uri.getPort() == 443));
        address = HostAndPort.fromParts(uri.getHost(), uri.getPort() == -1 ? (useSecureConnection ? 443 : 80) : uri.getPort());

        initCatalogAndSchema();
    }

    public static TrinoUri create(String url, Properties properties)
            throws SQLException
    {
        return new TrinoUri(url, firstNonNull(properties, new Properties()));
    }

    public static TrinoUri create(URI uri, Properties properties)
            throws SQLException
    {
        return new TrinoUri(uri, firstNonNull(properties, new Properties()));
    }

    public URI getUri()
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
        return checkRequired(user, PropertyName.USER);
    }

    public static <T> T checkRequired(Optional<T> obj, PropertyName name)
            throws SQLException
    {
        return obj.orElseThrow(() -> new SQLException(format("Connection property '%s' is required", name)));
    }

    public Optional<String> getUser()
    {
        return user;
    }

    public boolean hasPassword()
    {
        return password.isPresent();
    }

    public Optional<String> getSessionUser()
    {
        return sessionUser;
    }

    public Map<String, ClientSelectedRole> getRoles()
    {
        return roles.orElse(ImmutableMap.of());
    }

    public Optional<String> getApplicationNamePrefix()
    {
        return applicationNamePrefix;
    }

    public Map<String, String> getExtraCredentials()
    {
        return extraCredentials.orElse(ImmutableMap.of());
    }

    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    public Optional<String> getClientTags()
    {
        return clientTags;
    }

    public Optional<String> getTraceToken()
    {
        return traceToken;
    }

    public Map<String, String> getSessionProperties()
    {
        return sessionProperties.orElse(ImmutableMap.of());
    }

    public Optional<String> getSource()
    {
        return source;
    }

    public boolean isCompressionDisabled()
    {
        return disableCompression.orElse(false);
    }

    public boolean isAssumeLiteralNamesInMetadataCallsForNonConformingClients()
    {
        return assumeLiteralNamesInMetadataCallsForNonConformingClients.orElse(false);
    }

    public boolean isAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()
    {
        return assumeLiteralUnderscoreInMetadataCallsForNonConformingClients.orElse(false);
    }

    public Properties getProperties()
    {
        return properties;
    }

    public static DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
    {
        Properties properties = urlProperties(url, info);

        return ConnectionProperties.allProperties().stream()
                .filter(property -> property.isValid(properties))
                .map(property -> property.getDriverPropertyInfo(properties))
                .toArray(DriverPropertyInfo[]::new);
    }

    private static Properties urlProperties(String url, Properties info)
    {
        try {
            return create(url, info).getProperties();
        }
        catch (SQLException e) {
            return info;
        }
    }

    public Consumer<OkHttpClient.Builder> getSetupSsl()
    {
        if (!useSecureConnection) {
            return OkHttpUtil::setupInsecureSsl;
        }
        SslVerificationMode sslVerificationMode = sslVerification.orElse(FULL);
        if (sslVerificationMode.equals(NONE)) {
            return OkHttpUtil::setupInsecureSsl;
        }
        return builder -> setupSsl(
                builder,
                sslKeyStorePath,
                sslKeyStorePassword,
                sslKeyStoreType,
                sslTrustStorePath,
                sslTrustStorePassword,
                sslTrustStoreType,
                sslUseSystemTrustStore.orElse(false));
    }

    public void setupClient(OkHttpClient.Builder builder)
            throws SQLException
    {
        try {
            setupCookieJar(builder);
            setupSocksProxy(builder, socksProxy);
            setupHttpProxy(builder, httpProxy);

            String password = this.password.orElse("");
            if (!password.isEmpty()) {
                if (!useSecureConnection) {
                    throw new SQLException("TLS/SSL is required for authentication with username and password");
                }
                builder.addInterceptor(basicAuth(getRequiredUser(), password));
            }

            if (useSecureConnection) {
                SslVerificationMode sslVerificationMode = sslVerification.orElse(FULL);
                if (sslVerificationMode.equals(FULL) || sslVerificationMode.equals(CA)) {
                    setupSsl(
                            builder,
                            sslKeyStorePath,
                            sslKeyStorePassword,
                            sslKeyStoreType,
                            sslTrustStorePath,
                            sslTrustStorePassword,
                            sslTrustStoreType,
                            sslUseSystemTrustStore.orElse(false));
                }
                if (sslVerificationMode.equals(FULL)) {
                    HOSTNAME_IN_CERTIFICATE.getValue(properties).ifPresent(certHostname ->
                            setupAlternateHostnameVerification(builder, certHostname));
                }

                if (sslVerificationMode.equals(CA)) {
                    builder.hostnameVerifier((hostname, session) -> true);
                }

                if (sslVerificationMode.equals(NONE)) {
                    setupInsecureSsl(builder);
                }
            }

            if (kerberosRemoteServiceName.isPresent()) {
                if (!useSecureConnection) {
                    throw new SQLException("TLS/SSL is required for Kerberos authentication");
                }
                setupKerberos(
                        builder,
                        checkRequired(kerberosServicePrincipalPattern, PropertyName.KERBEROS_SERVICE_PRINCIPAL_PATTERN),
                        checkRequired(kerberosRemoteServiceName, PropertyName.KERBEROS_REMOTE_SERVICE_NAME),
                        checkRequired(kerberosUseCanonicalHostname, PropertyName.KERBEROS_USE_CANONICAL_HOSTNAME),
                        kerberosPrincipal,
                        kerberosConfigPath,
                        kerberosKeytabPath,
                        Optional.ofNullable(kerberosCredentialCachePath
                                .orElseGet(() -> defaultCredentialCachePath().map(File::new).orElse(null))),
                        kerberosDelegation.orElse(false),
                        kerberosConstrainedDelegation);
            }

            if (accessToken.isPresent()) {
                if (!useSecureConnection) {
                    throw new SQLException("TLS/SSL required for authentication using an access token");
                }
                builder.addInterceptor(tokenAuth(accessToken.get()));
            }

            if (externalAuthentication.orElse(false)) {
                if (!useSecureConnection) {
                    throw new SQLException("TLS/SSL required for authentication using external authorization");
                }

                // create HTTP client that shares the same settings, but without the external authenticator
                TokenPoller poller = new HttpTokenPoller(builder.build());

                Duration timeout = externalAuthenticationTimeout
                        .map(value -> Duration.ofMillis(value.toMillis()))
                        .orElse(Duration.ofMinutes(2));

                KnownTokenCache knownTokenCache = externalAuthenticationTokenCache.orElse(KnownTokenCache.NONE);

                Optional<RedirectHandler> configuredHandler = externalRedirectStrategies
                        .map(CompositeRedirectHandler::new)
                        .map(RedirectHandler.class::cast);

                RedirectHandler redirectHandler = Optional.ofNullable(REDIRECT_HANDLER.get())
                        .orElseGet(() -> configuredHandler.orElseThrow(() -> new RuntimeException("External authentication redirect handler is not configured")));

                ExternalAuthenticator authenticator = new ExternalAuthenticator(
                        redirectHandler, poller, knownTokenCache.create(), timeout);

                builder.authenticator(authenticator);
                builder.addInterceptor(authenticator);
            }

            Optional<String> resolverContext = DNS_RESOLVER_CONTEXT.getValue(properties);
            DNS_RESOLVER.getValue(properties).ifPresent(resolverClass -> builder.dns(instantiateDnsResolver(resolverClass, resolverContext)::lookup));
        }
        catch (ClientException e) {
            throw new SQLException(e.getMessage(), e);
        }
        catch (RuntimeException e) {
            throw new SQLException("Error setting up connection", e);
        }
    }

    private static DnsResolver instantiateDnsResolver(Class<? extends DnsResolver> resolverClass, Optional<String> context)
    {
        try {
            return resolverClass.getConstructor(String.class).newInstance(context.orElse(null));
        }
        catch (ReflectiveOperationException e) {
            throw new ClientException("Unable to instantiate custom DNS resolver " + resolverClass.getName(), e);
        }
    }

    private Map<String, Object> parseParameters(String query)
            throws SQLException
    {
        Map<String, Object> result = new HashMap<>();

        if (query == null) {
            return result;
        }

        Iterable<String> queryArgs = QUERY_SPLITTER.split(query);
        for (String queryArg : queryArgs) {
            List<String> parts = ARG_SPLITTER.splitToList(queryArg);
            if (parts.size() != 2) {
                throw new SQLException(format("Connection argument is not a valid connection property: '%s'", queryArg));
            }

            String key = parts.get(0);
            PropertyName name = PropertyName.findByKey(key).orElseThrow(() -> new SQLException(format("Unrecognized connection property '%s'", key)));
            if (restrictedProperties.contains(name)) {
                throw new RestrictedPropertyException(name, format("Connection property %s cannot be set in the URL", parts.get(0)));
            }
            if (result.put(parts.get(0), parts.get(1)) != null) {
                throw new SQLException(format("Connection property %s is in the URL multiple times", parts.get(0)));
            }
        }

        return result;
    }

    private static URI parseDriverUrl(String url)
            throws SQLException
    {
        validatePrefix(url);
        URI uri = parseUrl(url);

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

    private static URI parseUrl(String url)
            throws SQLException
    {
        try {
            return new URI(url);
        }
        catch (URISyntaxException e) {
            throw new SQLException("Invalid Trino URL: " + url, e);
        }
    }

    private static void validatePrefix(String url)
            throws SQLException
    {
        if (!url.startsWith(URL_START)) {
            throw new SQLException("Invalid Trino URL: " + url);
        }

        if (url.equals(URL_START)) {
            throw new SQLException("Empty Trino URL: " + url);
        }
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

        if (catalog.isPresent()) {
            throw new RestrictedPropertyException(PropertyName.CATALOG, "Catalog cannot be set in the URL");
        }
        catalog = Optional.ofNullable(parts.get(0));

        if (parts.size() > 1) {
            if (parts.get(1).isEmpty()) {
                throw new SQLException("Schema name is empty: " + uri);
            }

            if (schema.isPresent()) {
                throw new RestrictedPropertyException(PropertyName.SCHEMA, "Schema cannot be set in the URL");
            }
            schema = Optional.ofNullable(parts.get(1));
        }
    }

    private Properties mergeConnectionProperties(URI uri, Properties driverProperties)
            throws SQLException
    {
        Map<String, Object> urlProperties = parseParameters(uri.getQuery());
        Map<String, Object> suppliedProperties = driverProperties.entrySet().stream()
                .collect(toImmutableMap(entry -> (String) entry.getKey(), Entry::getValue));

        for (String key : urlProperties.keySet()) {
            if (suppliedProperties.containsKey(key)) {
                throw new SQLException(format("Connection property %s is both in the URL and an argument", key));
            }
        }

        Properties result = new Properties();
        setProperties(result, suppliedProperties);
        setProperties(result, urlProperties);
        return result;
    }

    private static void setProperties(Properties properties, Map<String, Object> values)
    {
        properties.putAll(values);
    }

    private static void validateConnectionProperties(Properties connectionProperties)
            throws SQLException
    {
        for (String propertyName : connectionProperties.stringPropertyNames()) {
            if (ConnectionProperties.forKey(propertyName) == null) {
                throw new SQLException(format("Unrecognized connection property '%s'", propertyName));
            }
        }

        for (ConnectionProperty<?, ?> property : ConnectionProperties.allProperties()) {
            property.validate(connectionProperties);
        }
    }

    @VisibleForTesting
    public static void setRedirectHandler(RedirectHandler handler)
    {
        REDIRECT_HANDLER.set(requireNonNull(handler, "handler is null"));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private URI uri;
        private String catalog;
        private String schema;
        private List<PropertyName> restrictedProperties;
        private String user;
        private String password;
        private String sessionUser;
        private Map<String, ClientSelectedRole> roles;
        private HostAndPort socksProxy;
        private HostAndPort httpProxy;
        private String applicationNamePrefix;
        private Boolean disableCompression;
        private Boolean assumeLiteralNamesInMetadataCallsForNonConformingClients;
        private Boolean assumeLiteralUnderscoreInMetadataCallsForNonConformingClients;
        private Boolean ssl;
        private SslVerificationMode sslVerification;
        private String sslKeyStorePath;
        private String sslKeyStorePassword;
        private String sslKeyStoreType;
        private String sslTrustStorePath;
        private String sslTrustStorePassword;
        private String sslTrustStoreType;
        private Boolean sslUseSystemTrustStore;
        private String kerberosServicePrincipalPattern;
        private String kerberosRemoteServiceName;
        private Boolean kerberosUseCanonicalHostname;
        private String kerberosPrincipal;
        private File kerberosConfigPath;
        private File kerberosKeytabPath;
        private File kerberosCredentialCachePath;
        private Boolean kerberosDelegation;
        private GSSCredential kerberosConstrainedDelegation;
        private String accessToken;
        private Boolean externalAuthentication;
        private io.airlift.units.Duration externalAuthenticationTimeout;
        private List<ExternalRedirectStrategy> externalRedirectStrategies;
        private KnownTokenCache externalAuthenticationTokenCache;
        private Map<String, String> extraCredentials;
        private String hostnameInCertificate;
        private String clientInfo;
        private String clientTags;
        private String traceToken;
        private Map<String, String> sessionProperties;
        private String source;

        private Builder() {}

        public Builder setUri(URI uri)
        {
            this.uri = requireNonNull(uri, "uri is null");
            return this;
        }

        public Builder setCatalog(String catalog)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            return this;
        }

        public Builder setSchema(String schema)
        {
            this.schema = requireNonNull(schema, "schema is null");
            return this;
        }

        public Builder setRestrictedProperties(List<PropertyName> restrictedProperties)
        {
            this.restrictedProperties = requireNonNull(restrictedProperties, "restrictedProperties is null");
            return this;
        }

        public Builder setUser(String user)
        {
            this.user = requireNonNull(user, "user is null");
            return this;
        }

        public Builder setPassword(String password)
        {
            this.password = requireNonNull(password, "password is null");
            return this;
        }

        public Builder setSessionUser(String sessionUser)
        {
            this.sessionUser = requireNonNull(sessionUser, "sessionUser is null");
            return this;
        }

        public Builder setRoles(Map<String, ClientSelectedRole> roles)
        {
            this.roles = requireNonNull(roles, "roles is null");
            return this;
        }

        public Builder setSocksProxy(HostAndPort socksProxy)
        {
            this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
            return this;
        }

        public Builder setHttpProxy(HostAndPort httpProxy)
        {
            this.httpProxy = requireNonNull(httpProxy, "httpProxy is null");
            return this;
        }

        public Builder setApplicationNamePrefix(String applicationNamePrefix)
        {
            this.applicationNamePrefix = requireNonNull(applicationNamePrefix, "applicationNamePrefix is null");
            return this;
        }

        public Builder setDisableCompression(Boolean disableCompression)
        {
            this.disableCompression = requireNonNull(disableCompression, "disableCompression is null");
            return this;
        }

        public Builder setAssumeLiteralNamesInMetadataCallsForNonConformingClients(Boolean assumeLiteralNamesInMetadataCallsForNonConformingClients)
        {
            this.assumeLiteralNamesInMetadataCallsForNonConformingClients = requireNonNull(assumeLiteralNamesInMetadataCallsForNonConformingClients, "assumeLiteralNamesInMetadataCallsForNonConformingClients is null");
            return this;
        }

        public Builder setAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients(Boolean assumeLiteralUnderscoreInMetadataCallsForNonConformingClients)
        {
            this.assumeLiteralUnderscoreInMetadataCallsForNonConformingClients = requireNonNull(assumeLiteralUnderscoreInMetadataCallsForNonConformingClients, "assumeLiteralUnderscoreInMetadataCallsForNonConformingClients is null");
            return this;
        }

        public Builder setSsl(Boolean ssl)
        {
            this.ssl = requireNonNull(ssl, "ssl is null");
            return this;
        }

        public Builder setSslVerificationNone()
        {
            this.sslVerification = NONE;
            return this;
        }

        public Builder setSslKeyStorePath(String sslKeyStorePath)
        {
            this.sslKeyStorePath = requireNonNull(sslKeyStorePath, "sslKeyStorePath is null");
            return this;
        }

        public Builder setSslKeyStorePassword(String sslKeyStorePassword)
        {
            this.sslKeyStorePassword = requireNonNull(sslKeyStorePassword, "sslKeyStorePassword is null");
            return this;
        }

        public Builder setSslKeyStoreType(String sslKeyStoreType)
        {
            this.sslKeyStoreType = requireNonNull(sslKeyStoreType, "sslKeyStoreType is null");
            return this;
        }

        public Builder setSslTrustStorePath(String sslTrustStorePath)
        {
            this.sslTrustStorePath = requireNonNull(sslTrustStorePath, "sslTrustStorePath is null");
            return this;
        }

        public Builder setSslTrustStorePassword(String sslTrustStorePassword)
        {
            this.sslTrustStorePassword = requireNonNull(sslTrustStorePassword, "sslTrustStorePassword is null");
            return this;
        }

        public Builder setSslTrustStoreType(String sslTrustStoreType)
        {
            this.sslTrustStoreType = requireNonNull(sslTrustStoreType, "sslTrustStoreType is null");
            return this;
        }

        public Builder setSslUseSystemTrustStore(Boolean sslUseSystemTrustStore)
        {
            this.sslUseSystemTrustStore = requireNonNull(sslUseSystemTrustStore, "sslUseSystemTrustStore is null");
            return this;
        }

        public Builder setKerberosServicePrincipalPattern(String kerberosServicePrincipalPattern)
        {
            this.kerberosServicePrincipalPattern = requireNonNull(kerberosServicePrincipalPattern, "kerberosServicePrincipalPattern is null");
            return this;
        }

        public Builder setKerberosRemoveServiceName(String kerberosRemoteServiceName)
        {
            this.kerberosRemoteServiceName = requireNonNull(kerberosRemoteServiceName, "kerberosRemoteServiceName is null");
            return this;
        }

        public Builder setKerberosUseCanonicalHostname(Boolean kerberosUseCanonicalHostname)
        {
            this.kerberosUseCanonicalHostname = requireNonNull(kerberosUseCanonicalHostname, "kerberosUseCanonicalHostname is null");
            return this;
        }

        public Builder setKerberosPrincipal(String kerberosPrincipal)
        {
            this.kerberosPrincipal = requireNonNull(kerberosPrincipal, "kerberosPrincipal is null");
            return this;
        }

        public Builder setKerberosConfigPath(String kerberosConfigPath)
        {
            return setKerberosConfigPath(new File(requireNonNull(kerberosConfigPath, "kerberosConfigPath is null")));
        }

        public Builder setKerberosConfigPath(File kerberosConfigPath)
        {
            this.kerberosConfigPath = requireNonNull(kerberosConfigPath, "kerberosConfigPath is null");
            return this;
        }

        public Builder setKerberosKeytabPath(String kerberosKeytabPath)
        {
            return setKerberosKeytabPath(new File(requireNonNull(kerberosKeytabPath, "kerberosKeytabPath is null")));
        }

        public Builder setKerberosKeytabPath(File kerberosKeytabPath)
        {
            this.kerberosKeytabPath = requireNonNull(kerberosKeytabPath, "kerberosKeytabPath is null");
            return this;
        }

        public Builder setKerberosCredentialCachePath(String kerberosCredentialCachePath)
        {
            return setKerberosCredentialCachePath(new File(requireNonNull(kerberosCredentialCachePath, "kerberosCredentialCachePath is null")));
        }

        public Builder setKerberosCredentialCachePath(File kerberosCredentialCachePath)
        {
            this.kerberosCredentialCachePath = requireNonNull(kerberosCredentialCachePath, "kerberosCredentialCachePath is null");
            return this;
        }

        public Builder setKerberosDelegation(Boolean kerberosDelegation)
        {
            this.kerberosDelegation = requireNonNull(kerberosDelegation, "kerberosDelegation is null");
            return this;
        }

        public Builder setKerberosConstrainedDelegation(GSSCredential kerberosConstrainedDelegation)
        {
            this.kerberosConstrainedDelegation = requireNonNull(kerberosConstrainedDelegation, "kerberosConstrainedDelegation is null");
            return this;
        }

        public Builder setAccessToken(String accessToken)
        {
            this.accessToken = requireNonNull(accessToken, "accessToken is null");
            return this;
        }

        public Builder setExternalAuthentication(Boolean externalAuthentication)
        {
            this.externalAuthentication = requireNonNull(externalAuthentication, "externalAuthentication is null");
            return this;
        }

        public Builder setExternalAuthenticationTimeout(io.airlift.units.Duration externalAuthenticationTimeout)
        {
            this.externalAuthenticationTimeout = requireNonNull(externalAuthenticationTimeout, "externalAuthenticationTimeout is null");
            return this;
        }

        public Builder setExternalRedirectStrategies(List<ExternalRedirectStrategy> externalRedirectStrategies)
        {
            this.externalRedirectStrategies = requireNonNull(externalRedirectStrategies, "externalRedirectStrategies is null");
            return this;
        }

        public Builder setExternalAuthenticationTokenCache(KnownTokenCache externalAuthenticationTokenCache)
        {
            this.externalAuthenticationTokenCache = requireNonNull(externalAuthenticationTokenCache, "externalAuthenticationTokenCache is null");
            return this;
        }

        public Builder setExtraCredentials(Map<String, String> extraCredentials)
        {
            this.extraCredentials = requireNonNull(extraCredentials, "extraCredentials is null");
            return this;
        }

        public Builder setHostnameInCertificate(String hostnameInCertificate)
        {
            this.hostnameInCertificate = requireNonNull(hostnameInCertificate, "hostnameInCertificate is null");
            return this;
        }

        public Builder setClientInfo(String clientInfo)
        {
            this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
            return this;
        }

        public Builder setClientTags(String clientTags)
        {
            this.clientTags = requireNonNull(clientTags, "clientTags is null");
            return this;
        }

        public Builder setTraceToken(String traceToken)
        {
            this.traceToken = requireNonNull(traceToken, "traceToken is null");
            return this;
        }

        public Builder setSessionProperties(Map<String, String> sessionProperties)
        {
            this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
            return this;
        }

        public Builder setSource(String source)
        {
            this.source = requireNonNull(source, "source is null");
            return this;
        }

        public TrinoUri build()
                throws SQLException
        {
            return new TrinoUri(
                    uri,
                    Optional.ofNullable(catalog),
                    Optional.ofNullable(schema),
                    restrictedProperties,
                    Optional.ofNullable(user),
                    Optional.ofNullable(password),
                    Optional.ofNullable(sessionUser),
                    Optional.ofNullable(roles),
                    Optional.ofNullable(socksProxy),
                    Optional.ofNullable(httpProxy),
                    Optional.ofNullable(applicationNamePrefix),
                    Optional.ofNullable(disableCompression),
                    Optional.ofNullable(assumeLiteralNamesInMetadataCallsForNonConformingClients),
                    Optional.ofNullable(assumeLiteralUnderscoreInMetadataCallsForNonConformingClients),
                    Optional.ofNullable(ssl),
                    Optional.ofNullable(sslVerification),
                    Optional.ofNullable(sslKeyStorePath),
                    Optional.ofNullable(sslKeyStorePassword),
                    Optional.ofNullable(sslKeyStoreType),
                    Optional.ofNullable(sslTrustStorePath),
                    Optional.ofNullable(sslTrustStorePassword),
                    Optional.ofNullable(sslTrustStoreType),
                    Optional.ofNullable(sslUseSystemTrustStore),
                    Optional.ofNullable(kerberosServicePrincipalPattern),
                    Optional.ofNullable(kerberosRemoteServiceName),
                    Optional.ofNullable(kerberosUseCanonicalHostname),
                    Optional.ofNullable(kerberosPrincipal),
                    Optional.ofNullable(kerberosConfigPath),
                    Optional.ofNullable(kerberosKeytabPath),
                    Optional.ofNullable(kerberosCredentialCachePath),
                    Optional.ofNullable(kerberosDelegation),
                    Optional.ofNullable(kerberosConstrainedDelegation),
                    Optional.ofNullable(accessToken),
                    Optional.ofNullable(externalAuthentication),
                    Optional.ofNullable(externalAuthenticationTimeout),
                    Optional.ofNullable(externalRedirectStrategies),
                    Optional.ofNullable(externalAuthenticationTokenCache),
                    Optional.ofNullable(extraCredentials),
                    Optional.ofNullable(hostnameInCertificate),
                    Optional.ofNullable(clientInfo),
                    Optional.ofNullable(clientTags),
                    Optional.ofNullable(traceToken),
                    Optional.ofNullable(sessionProperties),
                    Optional.ofNullable(source));
        }
    }
}
