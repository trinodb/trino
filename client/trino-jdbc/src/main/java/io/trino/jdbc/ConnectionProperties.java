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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.client.ClientSelectedRole;
import io.trino.client.auth.external.ExternalRedirectStrategy;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static io.trino.client.ClientSelectedRole.Type.ALL;
import static io.trino.client.ClientSelectedRole.Type.NONE;
import static io.trino.jdbc.AbstractConnectionProperty.checkedPredicate;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

final class ConnectionProperties
{
    enum SslVerificationMode
    {
        FULL, CA, NONE
    }

    public static final ConnectionProperty<String> USER = new User();
    public static final ConnectionProperty<String> PASSWORD = new Password();
    public static final ConnectionProperty<String> SESSION_USER = new SessionUser();
    public static final ConnectionProperty<Map<String, ClientSelectedRole>> ROLES = new Roles();
    public static final ConnectionProperty<HostAndPort> SOCKS_PROXY = new SocksProxy();
    public static final ConnectionProperty<HostAndPort> HTTP_PROXY = new HttpProxy();
    public static final ConnectionProperty<String> APPLICATION_NAME_PREFIX = new ApplicationNamePrefix();
    public static final ConnectionProperty<Boolean> DISABLE_COMPRESSION = new DisableCompression();
    public static final ConnectionProperty<Boolean> ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS = new AssumeLiteralNamesInMetadataCallsForNonConformingClients();
    public static final ConnectionProperty<Boolean> SSL = new Ssl();
    public static final ConnectionProperty<SslVerificationMode> SSL_VERIFICATION = new SslVerification();
    public static final ConnectionProperty<String> SSL_KEY_STORE_PATH = new SslKeyStorePath();
    public static final ConnectionProperty<String> SSL_KEY_STORE_PASSWORD = new SslKeyStorePassword();
    public static final ConnectionProperty<String> SSL_KEY_STORE_TYPE = new SslKeyStoreType();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PATH = new SslTrustStorePath();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_PASSWORD = new SslTrustStorePassword();
    public static final ConnectionProperty<String> SSL_TRUST_STORE_TYPE = new SslTrustStoreType();
    public static final ConnectionProperty<String> KERBEROS_SERVICE_PRINCIPAL_PATTERN = new KerberosServicePrincipalPattern();
    public static final ConnectionProperty<String> KERBEROS_REMOTE_SERVICE_NAME = new KerberosRemoteServiceName();
    public static final ConnectionProperty<Boolean> KERBEROS_USE_CANONICAL_HOSTNAME = new KerberosUseCanonicalHostname();
    public static final ConnectionProperty<String> KERBEROS_PRINCIPAL = new KerberosPrincipal();
    public static final ConnectionProperty<File> KERBEROS_CONFIG_PATH = new KerberosConfigPath();
    public static final ConnectionProperty<File> KERBEROS_KEYTAB_PATH = new KerberosKeytabPath();
    public static final ConnectionProperty<File> KERBEROS_CREDENTIAL_CACHE_PATH = new KerberosCredentialCachePath();
    public static final ConnectionProperty<Boolean> KERBEROS_DELEGATION = new KerberosDelegation();
    public static final ConnectionProperty<String> ACCESS_TOKEN = new AccessToken();
    public static final ConnectionProperty<Boolean> EXTERNAL_AUTHENTICATION = new ExternalAuthentication();
    public static final ConnectionProperty<Duration> EXTERNAL_AUTHENTICATION_TIMEOUT = new ExternalAuthenticationTimeout();
    public static final ConnectionProperty<List<ExternalRedirectStrategy>> EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS = new ExternalAuthenticationRedirectHandlers();
    public static final ConnectionProperty<KnownTokenCache> EXTERNAL_AUTHENTICATION_TOKEN_CACHE = new ExternalAuthenticationTokenCache();
    public static final ConnectionProperty<Map<String, String>> EXTRA_CREDENTIALS = new ExtraCredentials();
    public static final ConnectionProperty<String> CLIENT_INFO = new ClientInfo();
    public static final ConnectionProperty<String> CLIENT_TAGS = new ClientTags();
    public static final ConnectionProperty<String> TRACE_TOKEN = new TraceToken();
    public static final ConnectionProperty<Map<String, String>> SESSION_PROPERTIES = new SessionProperties();
    public static final ConnectionProperty<String> SOURCE = new Source();

    private static final Set<ConnectionProperty<?>> ALL_PROPERTIES = ImmutableSet.<ConnectionProperty<?>>builder()
            .add(USER)
            .add(PASSWORD)
            .add(SESSION_USER)
            .add(ROLES)
            .add(SOCKS_PROXY)
            .add(HTTP_PROXY)
            .add(APPLICATION_NAME_PREFIX)
            .add(DISABLE_COMPRESSION)
            .add(ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS)
            .add(SSL)
            .add(SSL_VERIFICATION)
            .add(SSL_KEY_STORE_PATH)
            .add(SSL_KEY_STORE_PASSWORD)
            .add(SSL_KEY_STORE_TYPE)
            .add(SSL_TRUST_STORE_PATH)
            .add(SSL_TRUST_STORE_PASSWORD)
            .add(SSL_TRUST_STORE_TYPE)
            .add(KERBEROS_REMOTE_SERVICE_NAME)
            .add(KERBEROS_SERVICE_PRINCIPAL_PATTERN)
            .add(KERBEROS_USE_CANONICAL_HOSTNAME)
            .add(KERBEROS_PRINCIPAL)
            .add(KERBEROS_CONFIG_PATH)
            .add(KERBEROS_KEYTAB_PATH)
            .add(KERBEROS_CREDENTIAL_CACHE_PATH)
            .add(KERBEROS_DELEGATION)
            .add(ACCESS_TOKEN)
            .add(EXTRA_CREDENTIALS)
            .add(CLIENT_INFO)
            .add(CLIENT_TAGS)
            .add(TRACE_TOKEN)
            .add(SESSION_PROPERTIES)
            .add(SOURCE)
            .add(EXTERNAL_AUTHENTICATION)
            .add(EXTERNAL_AUTHENTICATION_TIMEOUT)
            .add(EXTERNAL_AUTHENTICATION_TOKEN_CACHE)
            .add(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS)
            .build();

    private static final Map<String, ConnectionProperty<?>> KEY_LOOKUP = unmodifiableMap(ALL_PROPERTIES.stream()
            .collect(toMap(ConnectionProperty::getKey, identity())));

    private static final Map<String, String> DEFAULTS;

    static {
        ImmutableMap.Builder<String, String> defaults = ImmutableMap.builder();
        for (ConnectionProperty<?> property : ALL_PROPERTIES) {
            property.getDefault().ifPresent(value -> defaults.put(property.getKey(), value));
        }
        DEFAULTS = defaults.buildOrThrow();
    }

    private ConnectionProperties() {}

    public static ConnectionProperty<?> forKey(String propertiesKey)
    {
        return KEY_LOOKUP.get(propertiesKey);
    }

    public static Set<ConnectionProperty<?>> allProperties()
    {
        return ALL_PROPERTIES;
    }

    public static Map<String, String> getDefaults()
    {
        return DEFAULTS;
    }

    private static class User
            extends AbstractConnectionProperty<String>
    {
        public User()
        {
            super("user", REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Password
            extends AbstractConnectionProperty<String>
    {
        public Password()
        {
            super("password", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class SessionUser
            extends AbstractConnectionProperty<String>
    {
        protected SessionUser()
        {
            super("sessionUser", NOT_REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Roles
            extends AbstractConnectionProperty<Map<String, ClientSelectedRole>>
    {
        public Roles()
        {
            super("roles", NOT_REQUIRED, ALLOWED, Roles::parseRoles);
        }

        // Roles consists of a list of catalog role pairs.
        // E.g., `jdbc:trino://example.net:8080/?roles=catalog1:none;catalog2:all;catalog3:role` will set following roles:
        //  - `none` in `catalog1`
        //  - `all` in `catalog2`
        //  - `role` in `catalog3`
        public static Map<String, ClientSelectedRole> parseRoles(String roles)
        {
            return new MapPropertyParser("roles").parse(roles).entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> mapToClientSelectedRole(entry.getValue())));
        }

        private static ClientSelectedRole mapToClientSelectedRole(String role)
        {
            checkArgument(!role.contains("\""), "Role must not contain double quotes: %s", role);
            if (ALL.name().equalsIgnoreCase(role)) {
                return new ClientSelectedRole(ALL, Optional.empty());
            }
            if (NONE.name().equalsIgnoreCase(role)) {
                return new ClientSelectedRole(NONE, Optional.empty());
            }
            return new ClientSelectedRole(ClientSelectedRole.Type.ROLE, Optional.of(role));
        }
    }

    private static class SocksProxy
            extends AbstractConnectionProperty<HostAndPort>
    {
        private static final Predicate<Properties> NO_HTTP_PROXY =
                checkedPredicate(properties -> !HTTP_PROXY.getValue(properties).isPresent());

        public SocksProxy()
        {
            super("socksProxy", NOT_REQUIRED, NO_HTTP_PROXY, HostAndPort::fromString);
        }
    }

    private static class HttpProxy
            extends AbstractConnectionProperty<HostAndPort>
    {
        private static final Predicate<Properties> NO_SOCKS_PROXY =
                checkedPredicate(properties -> !SOCKS_PROXY.getValue(properties).isPresent());

        public HttpProxy()
        {
            super("httpProxy", NOT_REQUIRED, NO_SOCKS_PROXY, HostAndPort::fromString);
        }
    }

    private static class ApplicationNamePrefix
            extends AbstractConnectionProperty<String>
    {
        public ApplicationNamePrefix()
        {
            super("applicationNamePrefix", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ClientInfo
            extends AbstractConnectionProperty<String>
    {
        public ClientInfo()
        {
            super("clientInfo", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ClientTags
            extends AbstractConnectionProperty<String>
    {
        public ClientTags()
        {
            super("clientTags", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class TraceToken
            extends AbstractConnectionProperty<String>
    {
        public TraceToken()
        {
            super("traceToken", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class DisableCompression
            extends AbstractConnectionProperty<Boolean>
    {
        public DisableCompression()
        {
            super("disableCompression", NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class AssumeLiteralNamesInMetadataCallsForNonConformingClients
            extends AbstractConnectionProperty<Boolean>
    {
        public AssumeLiteralNamesInMetadataCallsForNonConformingClients()
        {
            super("assumeLiteralNamesInMetadataCallsForNonConformingClients", NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class Ssl
            extends AbstractConnectionProperty<Boolean>
    {
        public Ssl()
        {
            super("SSL", NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class SslVerification
            extends AbstractConnectionProperty<SslVerificationMode>
    {
        private static final Predicate<Properties> IF_SSL_ENABLED =
                checkedPredicate(properties -> SSL.getValue(properties).orElse(false));

        static final Predicate<Properties> IF_SSL_VERIFICATION_ENABLED =
                IF_SSL_ENABLED.and(checkedPredicate(properties -> !SSL_VERIFICATION.getValue(properties).orElse(SslVerificationMode.FULL).equals(SslVerificationMode.NONE)));

        public SslVerification()
        {
            super("SSLVerification", NOT_REQUIRED, IF_SSL_ENABLED, SslVerificationMode::valueOf);
        }
    }

    private static class SslKeyStorePath
            extends AbstractConnectionProperty<String>
    {
        public SslKeyStorePath()
        {
            super("SSLKeyStorePath", NOT_REQUIRED, SslVerification.IF_SSL_VERIFICATION_ENABLED, STRING_CONVERTER);
        }
    }

    private static class SslKeyStorePassword
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_KEY_STORE =
                checkedPredicate(properties -> SSL_KEY_STORE_PATH.getValue(properties).isPresent());

        public SslKeyStorePassword()
        {
            super("SSLKeyStorePassword", NOT_REQUIRED, IF_KEY_STORE.and(SslVerification.IF_SSL_VERIFICATION_ENABLED), STRING_CONVERTER);
        }
    }

    private static class SslKeyStoreType
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_KEY_STORE =
                checkedPredicate(properties -> SSL_KEY_STORE_PATH.getValue(properties).isPresent());

        public SslKeyStoreType()
        {
            super("SSLKeyStoreType", NOT_REQUIRED, IF_KEY_STORE.and(SslVerification.IF_SSL_VERIFICATION_ENABLED), STRING_CONVERTER);
        }
    }

    private static class SslTrustStorePath
            extends AbstractConnectionProperty<String>
    {
        public SslTrustStorePath()
        {
            super("SSLTrustStorePath", NOT_REQUIRED, SslVerification.IF_SSL_VERIFICATION_ENABLED, STRING_CONVERTER);
        }
    }

    private static class SslTrustStorePassword
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_TRUST_STORE =
                checkedPredicate(properties -> SSL_TRUST_STORE_PATH.getValue(properties).isPresent());

        public SslTrustStorePassword()
        {
            super("SSLTrustStorePassword", NOT_REQUIRED, IF_TRUST_STORE.and(SslVerification.IF_SSL_VERIFICATION_ENABLED), STRING_CONVERTER);
        }
    }

    private static class SslTrustStoreType
            extends AbstractConnectionProperty<String>
    {
        private static final Predicate<Properties> IF_TRUST_STORE =
                checkedPredicate(properties -> SSL_TRUST_STORE_PATH.getValue(properties).isPresent());

        public SslTrustStoreType()
        {
            super("SSLTrustStoreType", NOT_REQUIRED, IF_TRUST_STORE.and(SslVerification.IF_SSL_VERIFICATION_ENABLED), STRING_CONVERTER);
        }
    }

    private static class KerberosRemoteServiceName
            extends AbstractConnectionProperty<String>
    {
        public KerberosRemoteServiceName()
        {
            super("KerberosRemoteServiceName", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static Predicate<Properties> isKerberosEnabled()
    {
        return checkedPredicate(properties -> KERBEROS_REMOTE_SERVICE_NAME.getValue(properties).isPresent());
    }

    private static Predicate<Properties> isKerberosWithoutDelegation()
    {
        return isKerberosEnabled().and(checkedPredicate(properties -> !KERBEROS_DELEGATION.getValue(properties).orElse(false)));
    }

    private static class KerberosServicePrincipalPattern
            extends AbstractConnectionProperty<String>
    {
        public KerberosServicePrincipalPattern()
        {
            super("KerberosServicePrincipalPattern", Optional.of("${SERVICE}@${HOST}"), isKerberosEnabled(), ALLOWED, STRING_CONVERTER);
        }
    }

    private static class KerberosPrincipal
            extends AbstractConnectionProperty<String>
    {
        public KerberosPrincipal()
        {
            super("KerberosPrincipal", NOT_REQUIRED, isKerberosWithoutDelegation(), STRING_CONVERTER);
        }
    }

    private static class KerberosUseCanonicalHostname
            extends AbstractConnectionProperty<Boolean>
    {
        public KerberosUseCanonicalHostname()
        {
            super("KerberosUseCanonicalHostname", Optional.of("true"), isKerberosEnabled(), ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class KerberosConfigPath
            extends AbstractConnectionProperty<File>
    {
        public KerberosConfigPath()
        {
            super("KerberosConfigPath", NOT_REQUIRED, isKerberosWithoutDelegation(), FILE_CONVERTER);
        }
    }

    private static class KerberosKeytabPath
            extends AbstractConnectionProperty<File>
    {
        public KerberosKeytabPath()
        {
            super("KerberosKeytabPath", NOT_REQUIRED, isKerberosWithoutDelegation(), FILE_CONVERTER);
        }
    }

    private static class KerberosCredentialCachePath
            extends AbstractConnectionProperty<File>
    {
        public KerberosCredentialCachePath()
        {
            super("KerberosCredentialCachePath", NOT_REQUIRED, isKerberosWithoutDelegation(), FILE_CONVERTER);
        }
    }

    private static class KerberosDelegation
            extends AbstractConnectionProperty<Boolean>
    {
        public KerberosDelegation()
        {
            super("KerberosDelegation", Optional.of("false"), isKerberosEnabled(), ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class AccessToken
            extends AbstractConnectionProperty<String>
    {
        public AccessToken()
        {
            super("accessToken", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ExternalAuthentication
            extends AbstractConnectionProperty<Boolean>
    {
        public ExternalAuthentication()
        {
            super("externalAuthentication", Optional.of("false"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class ExternalAuthenticationRedirectHandlers
            extends AbstractConnectionProperty<List<ExternalRedirectStrategy>>
    {
        private static final Splitter ENUM_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

        public ExternalAuthenticationRedirectHandlers()
        {
            super("externalAuthenticationRedirectHandlers", Optional.of("OPEN"), NOT_REQUIRED, ALLOWED, ExternalAuthenticationRedirectHandlers::parse);
        }

        public static List<ExternalRedirectStrategy> parse(String value)
        {
            return StreamSupport.stream(ENUM_SPLITTER.split(value).spliterator(), false)
                    .map(ExternalRedirectStrategy::valueOf)
                    .collect(toImmutableList());
        }
    }

    private static class ExternalAuthenticationTimeout
            extends AbstractConnectionProperty<Duration>
    {
        private static final Predicate<Properties> IF_EXTERNAL_AUTHENTICATION_ENABLED =
                checkedPredicate(properties -> EXTERNAL_AUTHENTICATION.getValue(properties).orElse(false));

        public ExternalAuthenticationTimeout()
        {
            super("externalAuthenticationTimeout", NOT_REQUIRED, IF_EXTERNAL_AUTHENTICATION_ENABLED, Duration::valueOf);
        }
    }

    private static class ExternalAuthenticationTokenCache
            extends AbstractConnectionProperty<KnownTokenCache>
    {
        public ExternalAuthenticationTokenCache()
        {
            super("externalAuthenticationTokenCache", Optional.of(KnownTokenCache.NONE.name()), NOT_REQUIRED, ALLOWED, KnownTokenCache::valueOf);
        }
    }

    private static class ExtraCredentials
            extends AbstractConnectionProperty<Map<String, String>>
    {
        public ExtraCredentials()
        {
            super("extraCredentials", NOT_REQUIRED, ALLOWED, ExtraCredentials::parseExtraCredentials);
        }

        // Extra credentials consists of a list of credential name value pairs.
        // E.g., `jdbc:trino://example.net:8080/?extraCredentials=abc:xyz;foo:bar` will create credentials `abc=xyz` and `foo=bar`
        public static Map<String, String> parseExtraCredentials(String extraCredentialString)
        {
            return new MapPropertyParser("extraCredentials").parse(extraCredentialString);
        }
    }

    private static class SessionProperties
            extends AbstractConnectionProperty<Map<String, String>>
    {
        private static final Splitter NAME_PARTS_SPLITTER = Splitter.on('.');

        public SessionProperties()
        {
            super("sessionProperties", NOT_REQUIRED, ALLOWED, SessionProperties::parseSessionProperties);
        }

        // Session properties consists of a list of session property name value pairs.
        // E.g., `jdbc:trino://example.net:8080/?sessionProperties=abc:xyz;catalog.foo:bar` will create session properties `abc=xyz` and `catalog.foo=bar`
        public static Map<String, String> parseSessionProperties(String sessionPropertiesString)
        {
            Map<String, String> sessionProperties = new MapPropertyParser("sessionProperties").parse(sessionPropertiesString);
            for (String sessionPropertyName : sessionProperties.keySet()) {
                checkArgument(NAME_PARTS_SPLITTER.splitToList(sessionPropertyName).size() <= 2, "Malformed session property name: %s", sessionPropertyName);
            }
            return sessionProperties;
        }
    }

    private static class Source
            extends AbstractConnectionProperty<String>
    {
        public Source()
        {
            super("source", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class MapPropertyParser
    {
        private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E);
        private static final Splitter MAP_ENTRIES_SPLITTER = Splitter.on(';');
        private static final Splitter MAP_ENTRY_SPLITTER = Splitter.on(':');

        private final String mapName;

        private MapPropertyParser(String mapName)
        {
            this.mapName = requireNonNull(mapName, "mapName is null");
        }

        /**
         * Parses map in a form: key1:value1;key2:value2
         */
        public Map<String, String> parse(String map)
        {
            return MAP_ENTRIES_SPLITTER.splitToList(map).stream()
                    .map(this::parseEntry)
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private Map.Entry<String, String> parseEntry(String credential)
        {
            List<String> keyValue = MAP_ENTRY_SPLITTER.limit(2).splitToList(credential);
            checkArgument(keyValue.size() == 2, "Malformed %s: %s", mapName, credential);
            String key = keyValue.get(0);
            String value = keyValue.get(1);
            checkArgument(!key.isEmpty(), "%s key is empty", mapName);
            checkArgument(!value.isEmpty(), "%s key is empty", mapName);

            checkArgument(PRINTABLE_ASCII.matchesAllOf(key), "%s key '%s' contains spaces or is not printable ASCII", mapName, key);
            // do not log value as it may contain sensitive information
            checkArgument(PRINTABLE_ASCII.matchesAllOf(value), "%s value for key '%s' contains spaces or is not printable ASCII", mapName, key);
            return immutableEntry(key, value);
        }
    }
}
