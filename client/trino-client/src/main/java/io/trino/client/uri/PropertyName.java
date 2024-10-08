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
import static java.util.function.Function.identity;

public enum PropertyName
{
    ACCESS_TOKEN("accessToken"),
    APPLICATION_NAME_PREFIX("applicationNamePrefix"),
    ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS("assumeLiteralNamesInMetadataCallsForNonConformingClients"),
    ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS("assumeLiteralUnderscoreInMetadataCallsForNonConformingClients"),
    ASSUME_NULL_CATALOG_MEANS_CURRENT_CATALOG("assumeNullCatalogMeansCurrentCatalog"),
    CATALOG("catalog"), // this is not actual property but part of the path
    CLIENT_INFO("clientInfo"),
    CLIENT_TAGS("clientTags"),
    DISABLE_COMPRESSION("disableCompression"),
    DNS_RESOLVER("dnsResolver"),
    DNS_RESOLVER_CONTEXT("dnsResolverContext"),
    ENCODING("encoding"),
    EXPLICIT_PREPARE("explicitPrepare"),
    EXTERNAL_AUTHENTICATION("externalAuthentication"),
    EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS("externalAuthenticationRedirectHandlers"),
    EXTERNAL_AUTHENTICATION_TIMEOUT("externalAuthenticationTimeout"),
    EXTERNAL_AUTHENTICATION_TOKEN_CACHE("externalAuthenticationTokenCache"),
    EXTRA_CREDENTIALS("extraCredentials"),
    HOSTNAME_IN_CERTIFICATE("hostnameInCertificate"),
    HTTP_LOGGING_LEVEL("httpLoggingLevel"),
    HTTP_PROXY("httpProxy"),
    KERBEROS_CONFIG_PATH("KerberosConfigPath"),
    KERBEROS_CONSTRAINED_DELEGATION("KerberosConstrainedDelegation"),
    KERBEROS_CREDENTIAL_CACHE_PATH("KerberosCredentialCachePath"),
    KERBEROS_DELEGATION("KerberosDelegation"),
    KERBEROS_KEYTAB_PATH("KerberosKeytabPath"),
    KERBEROS_PRINCIPAL("KerberosPrincipal"),
    KERBEROS_REMOTE_SERVICE_NAME("KerberosRemoteServiceName"),
    KERBEROS_SERVICE_PRINCIPAL_PATTERN("KerberosServicePrincipalPattern"),
    KERBEROS_USE_CANONICAL_HOSTNAME("KerberosUseCanonicalHostname"),
    LOCALE("locale"),
    PASSWORD("password"),
    SQL_PATH("path"),
    RESOURCE_ESTIMATES("resourceEstimates"),
    ROLES("roles"),
    SCHEMA("schema"), // this is not actual property but part of the path
    SESSION_PROPERTIES("sessionProperties"),
    SESSION_USER("sessionUser"),
    SOCKS_PROXY("socksProxy"),
    SOURCE("source"),
    SSL("SSL"),
    SSL_KEY_STORE_PASSWORD("SSLKeyStorePassword"),
    SSL_KEY_STORE_PATH("SSLKeyStorePath"),
    SSL_KEY_STORE_TYPE("SSLKeyStoreType"),
    SSL_USE_SYSTEM_KEY_STORE("SSLUseSystemKeyStore"),
    SSL_TRUST_STORE_PASSWORD("SSLTrustStorePassword"),
    SSL_TRUST_STORE_PATH("SSLTrustStorePath"),
    SSL_TRUST_STORE_TYPE("SSLTrustStoreType"),
    SSL_USE_SYSTEM_TRUST_STORE("SSLUseSystemTrustStore"),
    SSL_VERIFICATION("SSLVerification"),
    TIMEOUT("timeout"),
    TIMEZONE("timezone"),
    TRACE_TOKEN("traceToken"),
    USER("user");

    private final String key;

    private static final Map<String, PropertyName> lookup = stream(values())
            .collect(toImmutableMap(PropertyName::toString, identity()));

    PropertyName(final String key)
    {
        this.key = key;
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
