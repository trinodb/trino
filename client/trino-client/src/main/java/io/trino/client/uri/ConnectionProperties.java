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

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.client.ClientSelectedRole;
import io.trino.client.DnsResolver;
import io.trino.client.auth.external.ExternalRedirectStrategy;
import io.trino.client.spooling.encoding.QueryDataDecoders;
import org.ietf.jgss.GSSCredential;

import java.io.File;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.Streams.stream;
import static io.trino.client.ClientSelectedRole.Type.ALL;
import static io.trino.client.ClientSelectedRole.Type.NONE;
import static io.trino.client.uri.AbstractConnectionProperty.Validator;
import static io.trino.client.uri.AbstractConnectionProperty.validator;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.FULL;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
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

    public static final ConnectionProperty<String, String> USER = new User();
    public static final ConnectionProperty<String, String> PASSWORD = new Password();
    public static final ConnectionProperty<String, String> SESSION_USER = new SessionUser();
    public static final ConnectionProperty<String, Map<String, ClientSelectedRole>> ROLES = new Roles();
    public static final ConnectionProperty<String, HostAndPort> SOCKS_PROXY = new SocksProxy();
    public static final ConnectionProperty<String, HostAndPort> HTTP_PROXY = new HttpProxy();
    public static final ConnectionProperty<String, String> APPLICATION_NAME_PREFIX = new ApplicationNamePrefix();
    public static final ConnectionProperty<String, Boolean> DISABLE_COMPRESSION = new DisableCompression();
    public static final ConnectionProperty<String, String> ENCODING = new Encoding();
    public static final ConnectionProperty<String, Boolean> ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS = new AssumeLiteralNamesInMetadataCallsForNonConformingClients();
    public static final ConnectionProperty<String, Boolean> ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS = new AssumeLiteralUnderscoreInMetadataCallsForNonConformingClients();
    public static final ConnectionProperty<String, Boolean> SSL = new Ssl();
    public static final ConnectionProperty<String, SslVerificationMode> SSL_VERIFICATION = new SslVerification();
    public static final ConnectionProperty<String, String> SSL_KEY_STORE_PATH = new SslKeyStorePath();
    public static final ConnectionProperty<String, String> SSL_KEY_STORE_PASSWORD = new SslKeyStorePassword();
    public static final ConnectionProperty<String, String> SSL_KEY_STORE_TYPE = new SslKeyStoreType();
    public static final ConnectionProperty<String, Boolean> SSL_USE_SYSTEM_KEY_STORE = new SslUseSystemKeyStore();
    public static final ConnectionProperty<String, String> SSL_TRUST_STORE_PATH = new SslTrustStorePath();
    public static final ConnectionProperty<String, String> SSL_TRUST_STORE_PASSWORD = new SslTrustStorePassword();
    public static final ConnectionProperty<String, String> SSL_TRUST_STORE_TYPE = new SslTrustStoreType();
    public static final ConnectionProperty<String, Boolean> SSL_USE_SYSTEM_TRUST_STORE = new SslUseSystemTrustStore();
    public static final ConnectionProperty<String, String> KERBEROS_SERVICE_PRINCIPAL_PATTERN = new KerberosServicePrincipalPattern();
    public static final ConnectionProperty<String, String> KERBEROS_REMOTE_SERVICE_NAME = new KerberosRemoteServiceName();
    public static final ConnectionProperty<String, Boolean> KERBEROS_USE_CANONICAL_HOSTNAME = new KerberosUseCanonicalHostname();
    public static final ConnectionProperty<String, String> KERBEROS_PRINCIPAL = new KerberosPrincipal();
    public static final ConnectionProperty<String, File> KERBEROS_CONFIG_PATH = new KerberosConfigPath();
    public static final ConnectionProperty<String, File> KERBEROS_KEYTAB_PATH = new KerberosKeytabPath();
    public static final ConnectionProperty<String, File> KERBEROS_CREDENTIAL_CACHE_PATH = new KerberosCredentialCachePath();
    public static final ConnectionProperty<String, Boolean> KERBEROS_DELEGATION = new KerberosDelegation();
    public static final ConnectionProperty<GSSCredential, GSSCredential> KERBEROS_CONSTRAINED_DELEGATION = new KerberosConstrainedDelegation();
    public static final ConnectionProperty<String, String> ACCESS_TOKEN = new AccessToken();
    public static final ConnectionProperty<String, Boolean> EXTERNAL_AUTHENTICATION = new ExternalAuthentication();
    public static final ConnectionProperty<String, Duration> EXTERNAL_AUTHENTICATION_TIMEOUT = new ExternalAuthenticationTimeout();
    public static final ConnectionProperty<String, List<ExternalRedirectStrategy>> EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS = new ExternalAuthenticationRedirectHandlers();
    public static final ConnectionProperty<String, KnownTokenCache> EXTERNAL_AUTHENTICATION_TOKEN_CACHE = new ExternalAuthenticationTokenCache();
    public static final ConnectionProperty<String, Map<String, String>> EXTRA_CREDENTIALS = new ExtraCredentials();
    public static final ConnectionProperty<String, String> CLIENT_INFO = new ClientInfo();
    public static final ConnectionProperty<String, Set<String>> CLIENT_TAGS = new ClientTags();
    public static final ConnectionProperty<String, String> TRACE_TOKEN = new TraceToken();
    public static final ConnectionProperty<String, Map<String, String>> SESSION_PROPERTIES = new SessionProperties();
    public static final ConnectionProperty<String, String> SOURCE = new Source();
    public static final ConnectionProperty<String, String> CATALOG = new Catalog();
    public static final ConnectionProperty<String, String> SCHEMA = new Schema();
    public static final ConnectionProperty<String, Class<? extends DnsResolver>> DNS_RESOLVER = new Resolver();
    public static final ConnectionProperty<String, String> DNS_RESOLVER_CONTEXT = new ResolverContext();
    public static final ConnectionProperty<String, String> HOSTNAME_IN_CERTIFICATE = new HostnameInCertificate();
    public static final ConnectionProperty<String, ZoneId> TIMEZONE = new TimeZone();
    public static final ConnectionProperty<String, Boolean> EXPLICIT_PREPARE = new ExplicitPrepare();
    public static final ConnectionProperty<String, Boolean> ASSUME_NULL_CATALOG_MEANS_CURRENT_CATALOG = new AssumeNullCatalogMeansCurrentCatalog();
    public static final ConnectionProperty<String, Locale> LOCALE = new UserLocale();
    public static final ConnectionProperty<String, Duration> TIMEOUT = new Timeout();
    public static final ConnectionProperty<String, LoggingLevel> HTTP_LOGGING_LEVEL = new HttpLoggingLevel();
    public static final ConnectionProperty<String, Map<String, String>> RESOURCE_ESTIMATES = new ResourceEstimates();
    public static final ConnectionProperty<String, List<String>> SQL_PATH = new SqlPath();
    public static final ConnectionProperty<String, Boolean> VALIDATE_CONNECTION = new ValidateConnection();

    private static final Set<ConnectionProperty<?, ?>> ALL_PROPERTIES = ImmutableSet.<ConnectionProperty<?, ?>>builder()
            // Keep sorted
            .add(ACCESS_TOKEN)
            .add(APPLICATION_NAME_PREFIX)
            .add(ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS)
            .add(ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS)
            .add(ASSUME_NULL_CATALOG_MEANS_CURRENT_CATALOG)
            .add(CATALOG)
            .add(CLIENT_INFO)
            .add(CLIENT_TAGS)
            .add(DISABLE_COMPRESSION)
            .add(DNS_RESOLVER)
            .add(DNS_RESOLVER_CONTEXT)
            .add(ENCODING)
            .add(EXPLICIT_PREPARE)
            .add(EXTERNAL_AUTHENTICATION)
            .add(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS)
            .add(EXTERNAL_AUTHENTICATION_TIMEOUT)
            .add(EXTERNAL_AUTHENTICATION_TOKEN_CACHE)
            .add(EXTRA_CREDENTIALS)
            .add(HOSTNAME_IN_CERTIFICATE)
            .add(HTTP_LOGGING_LEVEL)
            .add(HTTP_PROXY)
            .add(KERBEROS_CONFIG_PATH)
            .add(KERBEROS_CONSTRAINED_DELEGATION)
            .add(KERBEROS_CREDENTIAL_CACHE_PATH)
            .add(KERBEROS_DELEGATION)
            .add(KERBEROS_KEYTAB_PATH)
            .add(KERBEROS_PRINCIPAL)
            .add(KERBEROS_REMOTE_SERVICE_NAME)
            .add(KERBEROS_SERVICE_PRINCIPAL_PATTERN)
            .add(KERBEROS_USE_CANONICAL_HOSTNAME)
            .add(LOCALE)
            .add(PASSWORD)
            .add(RESOURCE_ESTIMATES)
            .add(ROLES)
            .add(SCHEMA)
            .add(SESSION_PROPERTIES)
            .add(SESSION_USER)
            .add(SOCKS_PROXY)
            .add(SOURCE)
            .add(SQL_PATH)
            .add(SSL)
            .add(SSL_KEY_STORE_PASSWORD)
            .add(SSL_KEY_STORE_PATH)
            .add(SSL_KEY_STORE_TYPE)
            .add(SSL_USE_SYSTEM_KEY_STORE)
            .add(SSL_TRUST_STORE_PASSWORD)
            .add(SSL_TRUST_STORE_PATH)
            .add(SSL_TRUST_STORE_TYPE)
            .add(SSL_USE_SYSTEM_TRUST_STORE)
            .add(SSL_VERIFICATION)
            .add(TIMEOUT)
            .add(TIMEZONE)
            .add(TRACE_TOKEN)
            .add(USER)
            .add(VALIDATE_CONNECTION)
            .build();

    private static final Map<String, ConnectionProperty<?, ?>> KEY_LOOKUP = unmodifiableMap(ALL_PROPERTIES.stream()
            .collect(toMap(ConnectionProperty::getKey, identity())));

    private ConnectionProperties() {}

    public static ConnectionProperty<?, ?> forKey(String propertiesKey)
    {
        return KEY_LOOKUP.get(propertiesKey);
    }

    public static Set<ConnectionProperty<?, ?>> allProperties()
    {
        return ALL_PROPERTIES;
    }

    private static class User
            extends AbstractConnectionProperty<String, String>
    {
        public User()
        {
            super(PropertyName.USER, NOT_REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Password
            extends AbstractConnectionProperty<String, String>
    {
        public Password()
        {
            super(PropertyName.PASSWORD, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class SessionUser
            extends AbstractConnectionProperty<String, String>
    {
        protected SessionUser()
        {
            super(PropertyName.SESSION_USER, NOT_REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Roles
            extends AbstractConnectionProperty<String, Map<String, ClientSelectedRole>>
    {
        public Roles()
        {
            super(PropertyName.ROLES, NOT_REQUIRED, ALLOWED, converter(Roles::parseRoles, Roles::rolesToString));
        }

        // Roles consists of a list of catalog role pairs.
        // E.g., `jdbc:trino://example.net:8080/?roles=catalog1:none;catalog2:all;catalog3:role` will set following roles:
        //  - `none` in `catalog1`
        //  - `all` in `catalog2`
        //  - `role` in `catalog3`
        public static Map<String, ClientSelectedRole> parseRoles(String roles)
        {
            return new MapPropertyParser(PropertyName.ROLES.toString()).parse(roles).entrySet().stream()
                    .collect(toImmutableMap(Map.Entry::getKey, entry -> mapToClientSelectedRole(entry.getValue())));
        }

        public static String rolesToString(Map<String, ClientSelectedRole> roles)
        {
            return roles.entrySet().stream()
                    .map(entry -> entry.getKey() + ":" + roleToString(entry.getValue()))
                    .collect(Collectors.joining(";"));
        }

        private static String roleToString(ClientSelectedRole value)
        {
            switch (value.getType()) {
                case ALL:
                    return "all";
                case NONE:
                    return "none";
                case ROLE:
                    return value.getRole().orElse("role");
                default:
                    throw new IllegalArgumentException("Unrecognized role type " + value.getType());
            }
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

    private static class ResourceEstimates
            extends AbstractConnectionProperty<String, Map<String, String>>
    {
        private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E);

        public ResourceEstimates()
        {
            super(PropertyName.RESOURCE_ESTIMATES, NOT_REQUIRED, ALLOWED, converter(ResourceEstimates::parseResourceEstimates, ResourceEstimates::toString));
        }

        public static Map<String, String> parseResourceEstimates(String resourceEstimateString)
        {
            Map<String, String> resourceEstimates = new MapPropertyParser(PropertyName.RESOURCE_ESTIMATES.toString()).parse(resourceEstimateString);
            for (String resourceName : resourceEstimates.keySet()) {
                checkArgument(PRINTABLE_ASCII.matchesAllOf(resourceName), "Resource contains spaces or is not ASCII: %s", resourceName);
                checkArgument(resourceName.indexOf('=') < 0, "Resource must not contain '=': %s", resourceName);
                checkArgument(PRINTABLE_ASCII.matchesAllOf(resourceEstimates.get(resourceName)), "Resource estimate contains spaces or is not ASCII: %s", resourceName);
            }
            return resourceEstimates;
        }

        public static String toString(Map<String, String> values)
        {
            return values.entrySet().stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(";"));
        }
    }

    private static class SocksProxy
            extends AbstractConnectionProperty<String, HostAndPort>
    {
        private static final Validator<Properties> NO_HTTP_PROXY = validator(
                properties -> !HTTP_PROXY.getValue(properties).isPresent(),
                format("Connection property %s cannot be used when %s is set", PropertyName.SOCKS_PROXY, PropertyName.HTTP_PROXY));

        public SocksProxy()
        {
            super(PropertyName.SOCKS_PROXY, NOT_REQUIRED, NO_HTTP_PROXY, converter(HostAndPort::fromString, HostAndPort::toString));
        }
    }

    private static class HttpProxy
            extends AbstractConnectionProperty<String, HostAndPort>
    {
        private static final Validator<Properties> NO_SOCKS_PROXY = validator(
                properties -> !SOCKS_PROXY.getValue(properties).isPresent(),
                format("Connection property %s cannot be used when %s is set", PropertyName.HTTP_PROXY, PropertyName.SOCKS_PROXY));

        public HttpProxy()
        {
            super(PropertyName.HTTP_PROXY, NOT_REQUIRED, NO_SOCKS_PROXY, converter(HostAndPort::fromString, HostAndPort::toString));
        }
    }

    private static class ApplicationNamePrefix
            extends AbstractConnectionProperty<String, String>
    {
        public ApplicationNamePrefix()
        {
            super(PropertyName.APPLICATION_NAME_PREFIX, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ClientInfo
            extends AbstractConnectionProperty<String, String>
    {
        public ClientInfo()
        {
            super(PropertyName.CLIENT_INFO, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ClientTags
            extends AbstractConnectionProperty<String, Set<String>>
    {
        public ClientTags()
        {
            super(PropertyName.CLIENT_TAGS, NOT_REQUIRED, ALLOWED, converter(ClientTags::parseClientTags, ClientTags::toString));
        }

        private static Set<String> parseClientTags(String clientTagsString)
        {
            Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
            return ImmutableSet.copyOf(splitter.split(nullToEmpty(clientTagsString)));
        }

        private static String toString(Set<String> clientTags)
        {
            return Joiner.on(",").join(clientTags);
        }
    }

    private static class TraceToken
            extends AbstractConnectionProperty<String, String>
    {
        public TraceToken()
        {
            super(PropertyName.TRACE_TOKEN, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class DisableCompression
            extends AbstractConnectionProperty<String, Boolean>
    {
        public DisableCompression()
        {
            super(PropertyName.DISABLE_COMPRESSION, NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class Encoding
            extends AbstractConnectionProperty<String, String>
    {
        public Encoding()
        {
            super(PropertyName.ENCODING, NOT_REQUIRED, Encoding::areEncodingsValid, STRING_CONVERTER);
        }

        public static Optional<String> areEncodingsValid(Properties properties)
        {
            List<String> supportedEncodings = Splitter.on(",").trimResults().omitEmptyStrings()
                    .splitToList(ENCODING.getRequiredValue(properties));

            for (String encoding : supportedEncodings) {
                if (!QueryDataDecoders.exists(encoding)) {
                    return Optional.of("Unknown encoding: " + encoding);
                }
            }
            return Optional.empty();
        }
    }

    /**
     * @deprecated use {@link AssumeLiteralUnderscoreInMetadataCallsForNonConformingClients}
     */
    @Deprecated
    private static class AssumeLiteralNamesInMetadataCallsForNonConformingClients
            extends AbstractConnectionProperty<String, Boolean>
    {
        private static final Predicate<Properties> IS_NOT_ENABLED = properties -> !ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.getValueOrDefault(properties, false);

        public AssumeLiteralNamesInMetadataCallsForNonConformingClients()
        {
            super(
                    PropertyName.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS,
                    NOT_REQUIRED,
                    validator(
                            AssumeLiteralUnderscoreInMetadataCallsForNonConformingClients.IS_NOT_ENABLED.or(IS_NOT_ENABLED),
                            format(
                                    "Connection property %s cannot be set if %s is enabled",
                                    PropertyName.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS,
                                    PropertyName.ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS)),
                    BOOLEAN_CONVERTER);
        }
    }

    private static class AssumeLiteralUnderscoreInMetadataCallsForNonConformingClients
            extends AbstractConnectionProperty<String, Boolean>
    {
        private static final Predicate<Properties> IS_NOT_ENABLED = properties -> !ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS.getValueOrDefault(properties, false);

        public AssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()
        {
            super(
                    PropertyName.ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS,
                    NOT_REQUIRED,
                    validator(
                            AssumeLiteralNamesInMetadataCallsForNonConformingClients.IS_NOT_ENABLED.or(IS_NOT_ENABLED),
                            format(
                                    "Connection property %s cannot be set if %s is enabled",
                                    PropertyName.ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS,
                                    PropertyName.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS)),
                    BOOLEAN_CONVERTER);
        }
    }

    private static class Ssl
            extends AbstractConnectionProperty<String, Boolean>
    {
        public Ssl()
        {
            super(PropertyName.SSL, Optional.of(false), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class SslVerification
            extends AbstractConnectionProperty<String, SslVerificationMode>
    {
        private static final Predicate<Properties> IF_SSL_ENABLED = properties -> SSL.getValueOrDefault(properties, false);

        static Validator<Properties> validateEnabled(PropertyName propertyName)
        {
            return validator(
                    IF_SSL_ENABLED.and(properties -> !SSL_VERIFICATION.getValueOrDefault(properties, FULL).equals(SslVerificationMode.NONE)),
                    format("Connection property %s cannot be set if %s is set to %s", propertyName, PropertyName.SSL_VERIFICATION, SslVerificationMode.NONE));
        }

        static Validator<Properties> validateFull(PropertyName propertyName)
        {
            return validator(
                    IF_SSL_ENABLED.and(properties -> SSL_VERIFICATION.getValueOrDefault(properties, FULL).equals(FULL)),
                    format("Connection property %s requires %s to be set to %s", propertyName, PropertyName.SSL_VERIFICATION, FULL));
        }

        public SslVerification()
        {
            super(
                    PropertyName.SSL_VERIFICATION,
                    Optional.of(FULL),
                    NOT_REQUIRED,
                    validator(IF_SSL_ENABLED, format("Connection property %s requires TLS/SSL to be enabled", PropertyName.SSL_VERIFICATION)),
                    converter(SslVerificationMode::valueOf, SslVerificationMode::name));
        }
    }

    private static class SslKeyStorePath
            extends AbstractConnectionProperty<String, String>
    {
        private static final Validator<Properties> VALIDATE_SYSTEM_KEY_STORE_NOT_ENABLED = validator(
                properties -> !SSL_USE_SYSTEM_KEY_STORE.getValue(properties).orElse(false),
                format("Connection property %s cannot be set if %s is enabled", PropertyName.SSL_KEY_STORE_PATH, PropertyName.SSL_USE_SYSTEM_KEY_STORE));

        public SslKeyStorePath()
        {
            super(PropertyName.SSL_KEY_STORE_PATH, NOT_REQUIRED, VALIDATE_SYSTEM_KEY_STORE_NOT_ENABLED.and(SslVerification.validateEnabled(PropertyName.SSL_KEY_STORE_PATH)), STRING_CONVERTER);
        }
    }

    private static class SslKeyStorePassword
            extends AbstractConnectionProperty<String, String>
    {
        private static final Validator<Properties> VALID_KEY_STORE = validator(
                properties -> SSL_KEY_STORE_PATH.getValue(properties).isPresent(),
                format("Connection property %s requires %s to be set", PropertyName.SSL_KEY_STORE_PASSWORD, PropertyName.SSL_KEY_STORE_PATH));

        public SslKeyStorePassword()
        {
            super(PropertyName.SSL_KEY_STORE_PASSWORD, NOT_REQUIRED, VALID_KEY_STORE.and(SslVerification.validateEnabled(PropertyName.SSL_KEY_STORE_PASSWORD)), STRING_CONVERTER);
        }
    }

    private static class SslKeyStoreType
            extends AbstractConnectionProperty<String, String>
    {
        private static final Validator<Properties> VALID_KEY_STORE = validator(
                properties -> SSL_KEY_STORE_PATH.getValue(properties).isPresent() || SSL_USE_SYSTEM_KEY_STORE.getValue(properties).orElse(false),
                format("Connection property %s requires %s to be set or %s to be enabled", PropertyName.SSL_KEY_STORE_TYPE, PropertyName.SSL_KEY_STORE_PATH, PropertyName.SSL_USE_SYSTEM_KEY_STORE));

        public SslKeyStoreType()
        {
            super(PropertyName.SSL_KEY_STORE_TYPE, NOT_REQUIRED, VALID_KEY_STORE.and(SslVerification.validateEnabled(PropertyName.SSL_KEY_STORE_TYPE)), STRING_CONVERTER);
        }
    }

    private static class SslUseSystemKeyStore
            extends AbstractConnectionProperty<String, Boolean>
    {
        public SslUseSystemKeyStore()
        {
            super(PropertyName.SSL_USE_SYSTEM_KEY_STORE, NOT_REQUIRED, SslVerification.validateEnabled(PropertyName.SSL_USE_SYSTEM_KEY_STORE), BOOLEAN_CONVERTER);
        }
    }

    private static class SslTrustStorePath
            extends AbstractConnectionProperty<String, String>
    {
        private static final Validator<Properties> VALIDATE_SYSTEM_TRUST_STORE_NOT_ENABLED = validator(
                properties -> !SSL_USE_SYSTEM_TRUST_STORE.getValueOrDefault(properties, false),
                format("Connection property %s cannot be set if %s is enabled", PropertyName.SSL_TRUST_STORE_PATH, PropertyName.SSL_USE_SYSTEM_TRUST_STORE));

        public SslTrustStorePath()
        {
            super(PropertyName.SSL_TRUST_STORE_PATH, NOT_REQUIRED, VALIDATE_SYSTEM_TRUST_STORE_NOT_ENABLED.and(SslVerification.validateEnabled(PropertyName.SSL_TRUST_STORE_PATH)), STRING_CONVERTER);
        }
    }

    private static class SslTrustStorePassword
            extends AbstractConnectionProperty<String, String>
    {
        private static final Validator<Properties> VALIDATE_TRUST_STORE = validator(
                properties -> SSL_TRUST_STORE_PATH.getValue(properties).isPresent(),
                format("Connection property %s requires %s to be set", PropertyName.SSL_TRUST_STORE_PASSWORD, PropertyName.SSL_TRUST_STORE_PATH));

        public SslTrustStorePassword()
        {
            super(PropertyName.SSL_TRUST_STORE_PASSWORD, NOT_REQUIRED, VALIDATE_TRUST_STORE.and(SslVerification.validateEnabled(PropertyName.SSL_TRUST_STORE_PASSWORD)), STRING_CONVERTER);
        }
    }

    private static class SslTrustStoreType
            extends AbstractConnectionProperty<String, String>
    {
        private static final Validator<Properties> VALIDATE_TRUST_STORE = validator(
                properties -> SSL_TRUST_STORE_PATH.getValue(properties).isPresent() || SSL_USE_SYSTEM_TRUST_STORE.getValueOrDefault(properties, false),
                format("Connection property %s requires %s to be set or %s to be enabled", PropertyName.SSL_TRUST_STORE_TYPE, PropertyName.SSL_TRUST_STORE_PATH, PropertyName.SSL_USE_SYSTEM_TRUST_STORE));

        public SslTrustStoreType()
        {
            super(PropertyName.SSL_TRUST_STORE_TYPE, NOT_REQUIRED, VALIDATE_TRUST_STORE.and(SslVerification.validateEnabled(PropertyName.SSL_TRUST_STORE_TYPE)), STRING_CONVERTER);
        }
    }

    private static class SslUseSystemTrustStore
            extends AbstractConnectionProperty<String, Boolean>
    {
        public SslUseSystemTrustStore()
        {
            super(PropertyName.SSL_USE_SYSTEM_TRUST_STORE, NOT_REQUIRED, SslVerification.validateEnabled(PropertyName.SSL_USE_SYSTEM_TRUST_STORE), BOOLEAN_CONVERTER);
        }
    }

    private static class KerberosRemoteServiceName
            extends AbstractConnectionProperty<String, String>
    {
        public KerberosRemoteServiceName()
        {
            super(PropertyName.KERBEROS_REMOTE_SERVICE_NAME, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ValidateConnection
            extends AbstractConnectionProperty<String, Boolean>
    {
        public ValidateConnection()
        {
            super(PropertyName.VALIDATE_CONNECTION, NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static Predicate<Properties> isKerberosEnabled()
    {
        return properties -> KERBEROS_REMOTE_SERVICE_NAME.getValue(properties).isPresent();
    }

    private static Validator<Properties> validateKerberosWithoutDelegation(PropertyName propertyName)
    {
        return validator(isKerberosEnabled(), format("Connection property %s requires %s to be set", propertyName, PropertyName.KERBEROS_REMOTE_SERVICE_NAME))
                .and(validator(
                        properties -> !KERBEROS_DELEGATION.getValueOrDefault(properties, false),
                        format("Connection property %s cannot be set if %s is enabled", propertyName, PropertyName.KERBEROS_DELEGATION)));
    }

    private static Validator<Properties> validateKerberosWithDelegation(PropertyName propertyName)
    {
        return validator(isKerberosEnabled(), format("Connection property %s requires %s to be set", propertyName, PropertyName.KERBEROS_REMOTE_SERVICE_NAME))
                .and(validator(
                        properties -> KERBEROS_DELEGATION.getValueOrDefault(properties, false),
                        format("Connection property %s requires %s to be enabled", propertyName, PropertyName.KERBEROS_DELEGATION)));
    }

    private static class KerberosServicePrincipalPattern
            extends AbstractConnectionProperty<String, String>
    {
        public KerberosServicePrincipalPattern()
        {
            super(PropertyName.KERBEROS_SERVICE_PRINCIPAL_PATTERN, Optional.of("${SERVICE}@${HOST}"), isKerberosEnabled(), ALLOWED, STRING_CONVERTER);
        }
    }

    private static class KerberosPrincipal
            extends AbstractConnectionProperty<String, String>
    {
        public KerberosPrincipal()
        {
            super(PropertyName.KERBEROS_PRINCIPAL, NOT_REQUIRED, validateKerberosWithoutDelegation(PropertyName.KERBEROS_PRINCIPAL), STRING_CONVERTER);
        }
    }

    private static class KerberosUseCanonicalHostname
            extends AbstractConnectionProperty<String, Boolean>
    {
        public KerberosUseCanonicalHostname()
        {
            super(PropertyName.KERBEROS_USE_CANONICAL_HOSTNAME, Optional.of(true), isKerberosEnabled(), ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class KerberosConfigPath
            extends AbstractConnectionProperty<String, File>
    {
        public KerberosConfigPath()
        {
            super(PropertyName.KERBEROS_CONFIG_PATH, NOT_REQUIRED, validateKerberosWithoutDelegation(PropertyName.KERBEROS_CONFIG_PATH), FILE_CONVERTER);
        }
    }

    private static class KerberosKeytabPath
            extends AbstractConnectionProperty<String, File>
    {
        public KerberosKeytabPath()
        {
            super(PropertyName.KERBEROS_KEYTAB_PATH, NOT_REQUIRED, validateKerberosWithoutDelegation(PropertyName.KERBEROS_KEYTAB_PATH), FILE_CONVERTER);
        }
    }

    private static class KerberosCredentialCachePath
            extends AbstractConnectionProperty<String, File>
    {
        public KerberosCredentialCachePath()
        {
            super(PropertyName.KERBEROS_CREDENTIAL_CACHE_PATH, NOT_REQUIRED, validateKerberosWithoutDelegation(PropertyName.KERBEROS_CREDENTIAL_CACHE_PATH), FILE_CONVERTER);
        }
    }

    private static class KerberosDelegation
            extends AbstractConnectionProperty<String, Boolean>
    {
        public KerberosDelegation()
        {
            super(PropertyName.KERBEROS_DELEGATION, Optional.of(false), isKerberosEnabled(), ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class KerberosConstrainedDelegation
            extends AbstractConnectionProperty<GSSCredential, GSSCredential>
    {
        public KerberosConstrainedDelegation()
        {
            super(PropertyName.KERBEROS_CONSTRAINED_DELEGATION, Optional.empty(), NOT_REQUIRED, validateKerberosWithDelegation(PropertyName.KERBEROS_CONSTRAINED_DELEGATION), converter(GSSCredential.class::cast, identity()));
        }
    }

    private static class AccessToken
            extends AbstractConnectionProperty<String, String>
    {
        public AccessToken()
        {
            super(PropertyName.ACCESS_TOKEN, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class ExternalAuthentication
            extends AbstractConnectionProperty<String, Boolean>
    {
        public ExternalAuthentication()
        {
            super(PropertyName.EXTERNAL_AUTHENTICATION, Optional.of(false), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class ExternalAuthenticationRedirectHandlers
            extends AbstractConnectionProperty<String, List<ExternalRedirectStrategy>>
    {
        private static final Splitter ENUM_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

        public ExternalAuthenticationRedirectHandlers()
        {
            super(
                    PropertyName.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS,
                    Optional.of(singletonList(ExternalRedirectStrategy.OPEN)),
                    NOT_REQUIRED,
                    ALLOWED,
                    converter(ExternalAuthenticationRedirectHandlers::parse, ExternalAuthenticationRedirectHandlers::toString));
        }

        public static List<ExternalRedirectStrategy> parse(String value)
        {
            return stream(ENUM_SPLITTER.split(value))
                    .map(ExternalRedirectStrategy::valueOf)
                    .collect(toImmutableList());
        }

        public static String toString(List<ExternalRedirectStrategy> values)
        {
            return values.stream()
                    .map(ExternalRedirectStrategy::toString)
                    .collect(Collectors.joining(","));
        }
    }

    private static class ExternalAuthenticationTimeout
            extends AbstractConnectionProperty<String, Duration>
    {
        private static final Validator<Properties> VALIDATE_EXTERNAL_AUTHENTICATION_ENABLED = validator(
                properties -> EXTERNAL_AUTHENTICATION.getValueOrDefault(properties, false),
                format("Connection property %s requires %s to be enabled", PropertyName.EXTERNAL_AUTHENTICATION_TIMEOUT, PropertyName.EXTERNAL_AUTHENTICATION));

        public ExternalAuthenticationTimeout()
        {
            super(PropertyName.EXTERNAL_AUTHENTICATION_TIMEOUT, NOT_REQUIRED, VALIDATE_EXTERNAL_AUTHENTICATION_ENABLED, converter(Duration::valueOf, Duration::toString));
        }
    }

    private static class ExternalAuthenticationTokenCache
            extends AbstractConnectionProperty<String, KnownTokenCache>
    {
        public ExternalAuthenticationTokenCache()
        {
            super(PropertyName.EXTERNAL_AUTHENTICATION_TOKEN_CACHE, Optional.of(KnownTokenCache.NONE), NOT_REQUIRED, ALLOWED, converter(KnownTokenCache::valueOf, KnownTokenCache::name));
        }
    }

    private static class ExtraCredentials
            extends AbstractConnectionProperty<String, Map<String, String>>
    {
        public ExtraCredentials()
        {
            super(PropertyName.EXTRA_CREDENTIALS, NOT_REQUIRED, ALLOWED, converter(ExtraCredentials::parseExtraCredentials, ExtraCredentials::toString));
        }

        // Extra credentials consists of a list of credential name value pairs.
        // E.g., `jdbc:trino://example.net:8080/?extraCredentials=abc:xyz;foo:bar` will create credentials `abc=xyz` and `foo=bar`
        public static Map<String, String> parseExtraCredentials(String extraCredentialString)
        {
            return new MapPropertyParser(PropertyName.EXTRA_CREDENTIALS.toString()).parse(extraCredentialString);
        }

        public static String toString(Map<String, String> values)
        {
            return values.entrySet().stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(";"));
        }
    }

    private static class SessionProperties
            extends AbstractConnectionProperty<String, Map<String, String>>
    {
        private static final Splitter NAME_PARTS_SPLITTER = Splitter.on('.');

        public SessionProperties()
        {
            super(PropertyName.SESSION_PROPERTIES, NOT_REQUIRED, ALLOWED, converter(SessionProperties::parseSessionProperties, SessionProperties::toString));
        }

        // Session properties consists of a list of session property name value pairs.
        // E.g., `jdbc:trino://example.net:8080/?sessionProperties=abc:xyz;catalog.foo:bar` will create session properties `abc=xyz` and `catalog.foo=bar`
        public static Map<String, String> parseSessionProperties(String sessionPropertiesString)
        {
            Map<String, String> sessionProperties = new MapPropertyParser(PropertyName.SESSION_PROPERTIES.toString()).parse(sessionPropertiesString);
            for (String sessionPropertyName : sessionProperties.keySet()) {
                checkArgument(NAME_PARTS_SPLITTER.splitToList(sessionPropertyName).size() <= 2, "Malformed session property name: %s", sessionPropertyName);
            }
            return sessionProperties;
        }

        public static String toString(Map<String, String> values)
        {
            return values.entrySet().stream()
                    .map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.joining(";"));
        }
    }

    private static class Source
            extends AbstractConnectionProperty<String, String>
    {
        public Source()
        {
            super(PropertyName.SOURCE, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class Schema
            extends AbstractConnectionProperty<String, String>
    {
        public Schema()
        {
            super(PropertyName.SCHEMA, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class Catalog
            extends AbstractConnectionProperty<String, String>
    {
        public Catalog()
        {
            super(PropertyName.CATALOG, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class UserLocale
            extends AbstractConnectionProperty<String, Locale>
    {
        public UserLocale()
        {
            super(PropertyName.LOCALE, NOT_REQUIRED, ALLOWED, converter(Locale::new, Locale::toString));
        }
    }

    private static class Timeout
            extends AbstractConnectionProperty<String, Duration>
    {
        protected Timeout()
        {
            super(PropertyName.TIMEOUT, NOT_REQUIRED, ALLOWED, converter(Duration::valueOf, Duration::toString));
        }
    }

    private static class SqlPath
            extends AbstractConnectionProperty<String, List<String>>
    {
        protected SqlPath()
        {
            super(PropertyName.SQL_PATH, NOT_REQUIRED, SqlPath::isValidSqlPath, converter(Splitter.on(",")::splitToList, Joiner.on(",")::join));
        }

        private static Optional<String> isValidSqlPath(Properties properties)
        {
            String paths = properties.getProperty(SQL_PATH.getKey());
            if (paths == null) {
                return Optional.empty();
            }

            for (String path : Splitter.on(',').split(paths)) {
                if (Splitter.on('.').splitToList(path).size() > 2) {
                    return Optional.of(format("Connection property '%s' has invalid syntax, should be [catalog].[schema] or [schema]", SQL_PATH.getKey()));
                }
            }
            return Optional.empty();
        }
    }

    private static class HttpLoggingLevel
            extends AbstractConnectionProperty<String, LoggingLevel>
    {
        protected HttpLoggingLevel()
        {
            super(PropertyName.HTTP_LOGGING_LEVEL, NOT_REQUIRED, ALLOWED, converter(LoggingLevel::valueOf, LoggingLevel::toString));
        }
    }

    private static class Resolver
            extends AbstractConnectionProperty<String, Class<? extends DnsResolver>>
    {
        public Resolver()
        {
            super(PropertyName.DNS_RESOLVER, NOT_REQUIRED, ALLOWED, converter(Resolver::findByName, Class::getName));
        }

        public static Class<? extends DnsResolver> findByName(String name)
        {
            try {
                return Class.forName(name).asSubclass(DnsResolver.class);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException("DNS resolver class not found: " + name, e);
            }
        }
    }

    private static class ResolverContext
            extends AbstractConnectionProperty<String, String>
    {
        public ResolverContext()
        {
            super(PropertyName.DNS_RESOLVER_CONTEXT, NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class HostnameInCertificate
            extends AbstractConnectionProperty<String, String>
    {
        public HostnameInCertificate()
        {
            super(PropertyName.HOSTNAME_IN_CERTIFICATE, NOT_REQUIRED, SslVerification.validateFull(PropertyName.HOSTNAME_IN_CERTIFICATE), STRING_CONVERTER);
        }
    }

    private static class TimeZone
            extends AbstractConnectionProperty<String, ZoneId>
    {
        public TimeZone()
        {
            super(PropertyName.TIMEZONE, NOT_REQUIRED, ALLOWED, converter(ZoneId::of, ZoneId::getId));
        }
    }

    private static class ExplicitPrepare
            extends AbstractConnectionProperty<String, Boolean>
    {
        public ExplicitPrepare()
        {
            super(PropertyName.EXPLICIT_PREPARE, NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class AssumeNullCatalogMeansCurrentCatalog
            extends AbstractConnectionProperty<String, Boolean>
    {
        public AssumeNullCatalogMeansCurrentCatalog()
        {
            super(PropertyName.ASSUME_NULL_CATALOG_MEANS_CURRENT_CATALOG, NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
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
