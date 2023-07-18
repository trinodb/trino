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

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public enum PropertyName
{
    USER("user", "User name"),
    PASSWORD("password", "User password"),
    SESSION_USER("sessionUser", "Name of the user to impersonate"),
    ROLES("roles", "Access roles that will be used to access catalogs"),
    SOCKS_PROXY("socksProxy", "SOCKS proxy address"),
    HTTP_PROXY("httpProxy", "HTTP proxy adress"),
    APPLICATION_NAME_PREFIX("applicationNamePrefix", "Application name"),
    DISABLE_COMPRESSION("disableCompression", "Disable compression"),
    ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS("assumeLiteralNamesInMetadataCallsForNonConformingClients", "tbd"),
    ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS("assumeLiteralUnderscoreInMetadataCallsForNonConformingClients", "tbd"),
    SSL("SSL", "Enable SSL/TLS connections"),
    SSL_VERIFICATION("SSLVerification", "SSL/TLS verification mode"),
    SSL_KEY_STORE_PATH("SSLKeyStorePath", "Path to the Java Key Store"),
    SSL_KEY_STORE_PASSWORD("SSLKeyStorePassword", "Password to the Java Key Store"),
    SSL_KEY_STORE_TYPE("SSLKeyStoreType", "Type of the Java Key Store"),
    SSL_TRUST_STORE_PATH("SSLTrustStorePath", "Path to the Java Trust Store"),
    SSL_TRUST_STORE_PASSWORD("SSLTrustStorePassword", "Password tothe Java Trust Store"),
    SSL_TRUST_STORE_TYPE("SSLTrustStoreType", "Type of the Java Trust Store"),
    SSL_USE_SYSTEM_TRUST_STORE("SSLUseSystemTrustStore", "Use system-provided Java Trust Store"),
    KERBEROS_SERVICE_PRINCIPAL_PATTERN("KerberosServicePrincipalPattern", "Kerberos service principal pattern"),
    KERBEROS_REMOTE_SERVICE_NAME("KerberosRemoteServiceName", "Kerberos remote service name"),
    KERBEROS_USE_CANONICAL_HOSTNAME("KerberosUseCanonicalHostname", "Canonicalize Kerberos hostnames"),
    KERBEROS_PRINCIPAL("KerberosPrincipal", "Kerberos principal name"),
    KERBEROS_CONFIG_PATH("KerberosConfigPath", "Kerberos configuration file path"),
    KERBEROS_KEYTAB_PATH("KerberosKeytabPath", "Path to the Kerberos keytab file"),
    KERBEROS_CREDENTIAL_CACHE_PATH("KerberosCredentialCachePath", "Path to the Kerberos ccache file"),
    KERBEROS_DELEGATION("KerberosDelegation", "Enable delegated Kerberos authentication"),
    KERBEROS_CONSTRAINED_DELEGATION("KerberosConstrainedDelegation", "Enable delegated constainted Kerberos authentication"),
    ACCESS_TOKEN("accessToken", "Access token to use while connecting to the OAuth2-secured cluster"),
    EXTERNAL_AUTHENTICATION("externalAuthentication", "Enable external authentication support for OAuth2-secured cluster"),
    EXTERNAL_AUTHENTICATION_TIMEOUT("externalAuthenticationTimeout", "Timeout for obtaining access token in the external authentication flow"),
    EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS("externalAuthenticationRedirectHandlers", "Redirection handlers to use in the external authentication flow"),
    EXTERNAL_AUTHENTICATION_TOKEN_CACHE("externalAuthenticationTokenCache", "Token cache to use"),
    EXTRA_CREDENTIALS("extraCredentials", "Extra credentials to pass"),
    CLIENT_INFO("clientInfo", "Additional client information"),
    CLIENT_TAGS("clientTags", "Client tags"),
    TRACE_TOKEN("traceToken", "Trace token"),
    SESSION_PROPERTIES("sessionProperties", "Session properties"),
    SOURCE("source", "Source"),
    LEGACY_PREPARED_STATEMENTS("legacyPreparedStatements", "Use legacy prepared statements"),
    DNS_RESOLVER("dnsResolver", "Custom DNS resolver class name"),
    DNS_RESOLVER_CONTEXT("dnsResolverContext", "Custom DNS resolver context"),
    HOSTNAME_IN_CERTIFICATE("hostnameInCertificate", "Enables legacy mode of TLS/SSL certificate validation"),
    // these two are not actual properties but parts of the path
    CATALOG("catalog", "Default catalog to use"),
    SCHEMA("schema", "Default schema to use"),
    TIMEZONE("timezone", "Timezone"),
    LOCALE("locale", "Locale"),
    TIMEOUT("timeout", "Connection read/write operation timeout"),
    HTTP_LOGGING_LEVEL("httpLoggingLevel", "Enable HTTP logging"),
    TARGET_RESULT_SIZE("targetResultSize", "Target result size");

    private final String key;
    private final String description;

    private static final Map<String, PropertyName> lookup = stream(values())
            .collect(toImmutableMap(PropertyName::toString, identity()));

    PropertyName(final String key, final String description)
    {
        this.key = requireNonNull(key, "key is null");
        this.description = requireNonNull(description, "description is null");
    }

    public String getDescription()
    {
        return description;
    }

    @Override
    public String toString()
    {
        return key;
    }

    public static Optional<PropertyName> findByKey(String key)
    {
        return Optional.ofNullable(lookup.get(key));
    }
}
