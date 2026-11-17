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
package io.trino.cli;

/**
 * Names of the Trino JDBC driver connection properties the CLI options map to.
 */
final class TrinoConnectionProperties
{
    static final String ACCESS_TOKEN = "accessToken";
    static final String CATALOG = "catalog";
    static final String CLIENT_INFO = "clientInfo";
    static final String CLIENT_TAGS = "clientTags";
    static final String DISABLE_COMPRESSION = "disableCompression";
    static final String ENCODING = "encoding";
    static final String EXTERNAL_AUTHENTICATION = "externalAuthentication";
    static final String EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS = "externalAuthenticationRedirectHandlers";
    static final String EXTERNAL_AUTHENTICATION_TOKEN_CACHE = "externalAuthenticationTokenCache";
    static final String EXTRA_CREDENTIALS = "extraCredentials";
    static final String EXTRA_HEADERS = "extraHeaders";
    static final String HTTP_LOGGING_LEVEL = "httpLoggingLevel";
    static final String HTTP_PROXY = "httpProxy";
    static final String KERBEROS_CONFIG_PATH = "KerberosConfigPath";
    static final String KERBEROS_CREDENTIAL_CACHE_PATH = "KerberosCredentialCachePath";
    static final String KERBEROS_KEYTAB_PATH = "KerberosKeytabPath";
    static final String KERBEROS_PRINCIPAL = "KerberosPrincipal";
    static final String KERBEROS_REMOTE_SERVICE_NAME = "KerberosRemoteServiceName";
    static final String KERBEROS_SERVICE_PRINCIPAL_PATTERN = "KerberosServicePrincipalPattern";
    static final String KERBEROS_USE_CANONICAL_HOSTNAME = "KerberosUseCanonicalHostname";
    static final String OAUTH2_CLIENT_ID = "oauth2ClientId";
    static final String OAUTH2_CLIENT_SECRET = "oauth2ClientSecret";
    static final String OAUTH2_SCOPE = "oauth2Scope";
    static final String OAUTH2_TOKEN_ENDPOINT = "oauth2TokenEndpoint";
    static final String PASSWORD = "password";
    static final String RESOURCE_ESTIMATES = "resourceEstimates";
    static final String SCHEMA = "schema";
    static final String SESSION_PROPERTIES = "sessionProperties";
    static final String SESSION_USER = "sessionUser";
    static final String SOCKS_PROXY = "socksProxy";
    static final String SOURCE = "source";
    static final String SQL_PATH = "path";
    static final String SSL = "SSL";
    static final String SSL_KEY_STORE_PASSWORD = "SSLKeyStorePassword";
    static final String SSL_KEY_STORE_PATH = "SSLKeyStorePath";
    static final String SSL_KEY_STORE_TYPE = "SSLKeyStoreType";
    static final String SSL_TRUST_STORE_PASSWORD = "SSLTrustStorePassword";
    static final String SSL_TRUST_STORE_PATH = "SSLTrustStorePath";
    static final String SSL_TRUST_STORE_TYPE = "SSLTrustStoreType";
    static final String SSL_USE_SYSTEM_KEY_STORE = "SSLUseSystemKeyStore";
    static final String SSL_USE_SYSTEM_TRUST_STORE = "SSLUseSystemTrustStore";
    static final String SSL_VERIFICATION = "SSLVerification";
    static final String TIMEOUT = "timeout";
    static final String TIMEZONE = "timezone";
    static final String TRACE_TOKEN = "traceToken";
    static final String USER = "user";

    private TrinoConnectionProperties() {}
}
