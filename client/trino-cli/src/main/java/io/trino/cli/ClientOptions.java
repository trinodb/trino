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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.auth.external.ExternalRedirectStrategy;
import io.trino.client.uri.PropertyName;
import io.trino.client.uri.RestrictedPropertyException;
import io.trino.client.uri.TrinoUri;
import okhttp3.logging.HttpLoggingInterceptor;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;

import java.lang.annotation.Retention;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.client.KerberosUtil.defaultCredentialCachePath;
import static io.trino.client.uri.PropertyName.ACCESS_TOKEN;
import static io.trino.client.uri.PropertyName.CATALOG;
import static io.trino.client.uri.PropertyName.CLIENT_INFO;
import static io.trino.client.uri.PropertyName.CLIENT_TAGS;
import static io.trino.client.uri.PropertyName.EXTERNAL_AUTHENTICATION;
import static io.trino.client.uri.PropertyName.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS;
import static io.trino.client.uri.PropertyName.EXTRA_CREDENTIALS;
import static io.trino.client.uri.PropertyName.HTTP_PROXY;
import static io.trino.client.uri.PropertyName.KERBEROS_CONFIG_PATH;
import static io.trino.client.uri.PropertyName.KERBEROS_CREDENTIAL_CACHE_PATH;
import static io.trino.client.uri.PropertyName.KERBEROS_KEYTAB_PATH;
import static io.trino.client.uri.PropertyName.KERBEROS_PRINCIPAL;
import static io.trino.client.uri.PropertyName.KERBEROS_REMOTE_SERVICE_NAME;
import static io.trino.client.uri.PropertyName.KERBEROS_SERVICE_PRINCIPAL_PATTERN;
import static io.trino.client.uri.PropertyName.KERBEROS_USE_CANONICAL_HOSTNAME;
import static io.trino.client.uri.PropertyName.PASSWORD;
import static io.trino.client.uri.PropertyName.SCHEMA;
import static io.trino.client.uri.PropertyName.SESSION_PROPERTIES;
import static io.trino.client.uri.PropertyName.SESSION_USER;
import static io.trino.client.uri.PropertyName.SOCKS_PROXY;
import static io.trino.client.uri.PropertyName.SOURCE;
import static io.trino.client.uri.PropertyName.SSL_KEY_STORE_PASSWORD;
import static io.trino.client.uri.PropertyName.SSL_KEY_STORE_PATH;
import static io.trino.client.uri.PropertyName.SSL_KEY_STORE_TYPE;
import static io.trino.client.uri.PropertyName.SSL_TRUST_STORE_PASSWORD;
import static io.trino.client.uri.PropertyName.SSL_TRUST_STORE_PATH;
import static io.trino.client.uri.PropertyName.SSL_TRUST_STORE_TYPE;
import static io.trino.client.uri.PropertyName.SSL_USE_SYSTEM_TRUST_STORE;
import static io.trino.client.uri.PropertyName.SSL_VERIFICATION;
import static io.trino.client.uri.PropertyName.TRACE_TOKEN;
import static io.trino.client.uri.PropertyName.USER;
import static java.lang.String.format;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.Option;
import static picocli.CommandLine.Parameters;

public class ClientOptions
{
    private static final Splitter NAME_VALUE_SPLITTER = Splitter.on('=').limit(2);
    private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E); // spaces are not allowed
    private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";
    private static final String SERVER_DEFAULT = "localhost:8080";
    private static final String SOURCE_DEFAULT = "trino-cli";
    static final String DEBUG_OPTION_NAME = "--debug";

    @Parameters(paramLabel = "URL", description = "Trino server URL", arity = "0..1")
    public Optional<String> url;

    @Option(names = "--server", paramLabel = "<server>", description = "Trino server location (default: " + SERVER_DEFAULT + ")")
    public Optional<String> server;

    @PropertyMapping(KERBEROS_SERVICE_PRINCIPAL_PATTERN)
    @Option(names = "--krb5-service-principal-pattern", paramLabel = "<pattern>", defaultValue = "$${SERVICE}@$${HOST}", description = "Remote kerberos service principal pattern " + DEFAULT_VALUE)
    public Optional<String> krb5ServicePrincipalPattern;

    @PropertyMapping(KERBEROS_REMOTE_SERVICE_NAME)
    @Option(names = "--krb5-remote-service-name", paramLabel = "<name>", description = "Remote peer's kerberos service name")
    public Optional<String> krb5RemoteServiceName;

    @PropertyMapping(KERBEROS_CONFIG_PATH)
    @Option(names = "--krb5-config-path", paramLabel = "<path>", defaultValue = "/etc/krb5.conf", description = "Kerberos config file path " + DEFAULT_VALUE)
    public Optional<String> krb5ConfigPath;

    @PropertyMapping(KERBEROS_KEYTAB_PATH)
    @Option(names = "--krb5-keytab-path", paramLabel = "<path>", defaultValue = "/etc/krb5.keytab", description = "Kerberos key table path " + DEFAULT_VALUE)
    public Optional<String> krb5KeytabPath;

    @PropertyMapping(KERBEROS_CREDENTIAL_CACHE_PATH)
    @Option(names = "--krb5-credential-cache-path", paramLabel = "<path>", description = "Kerberos credential cache path")
    public Optional<String> krb5CredentialCachePath = defaultCredentialCachePath();

    @PropertyMapping(KERBEROS_PRINCIPAL)
    @Option(names = "--krb5-principal", paramLabel = "<principal>", description = "Kerberos principal to be used")
    public Optional<String> krb5Principal;

    @PropertyMapping(KERBEROS_USE_CANONICAL_HOSTNAME)
    @Option(names = "--krb5-disable-remote-service-hostname-canonicalization", description = "Disable service hostname canonicalization using the DNS reverse lookup")
    public boolean krb5DisableRemoteServiceHostnameCanonicalization;

    @PropertyMapping(SSL_KEY_STORE_PATH)
    @Option(names = "--keystore-path", paramLabel = "<path>", description = "Keystore path")
    public Optional<String> keystorePath;

    @PropertyMapping(SSL_KEY_STORE_PASSWORD)
    @Option(names = "--keystore-password", paramLabel = "<password>", description = "Keystore password")
    public Optional<String> keystorePassword;

    @PropertyMapping(SSL_KEY_STORE_TYPE)
    @Option(names = "--keystore-type", paramLabel = "<type>", description = "Keystore type")
    public Optional<String> keystoreType;

    @PropertyMapping(SSL_TRUST_STORE_PATH)
    @Option(names = "--truststore-path", paramLabel = "<path>", description = "Truststore path")
    public Optional<String> truststorePath;

    @PropertyMapping(SSL_TRUST_STORE_PASSWORD)
    @Option(names = "--truststore-password", paramLabel = "<password>", description = "Truststore password")
    public Optional<String> truststorePassword;

    @PropertyMapping(SSL_TRUST_STORE_TYPE)
    @Option(names = "--truststore-type", paramLabel = "<type>", description = "Truststore type")
    public Optional<String> truststoreType;

    @PropertyMapping(SSL_USE_SYSTEM_TRUST_STORE)
    @Option(names = "--use-system-truststore", description = "Use default system (OS) truststore")
    public boolean useSystemTruststore;

    @PropertyMapping(SSL_VERIFICATION)
    @Option(names = "--insecure", description = "Skip validation of HTTP server certificates (should only be used for debugging)")
    public boolean insecure;

    @PropertyMapping(ACCESS_TOKEN)
    @Option(names = "--access-token", paramLabel = "<token>", description = "Access token")
    public Optional<String> accessToken;

    @PropertyMapping(USER)
    @Option(names = "--user", paramLabel = "<user>", defaultValue = "${sys:user.name}", description = "Username " + DEFAULT_VALUE)
    public Optional<String> user;

    @PropertyMapping(PASSWORD)
    @Option(names = "--password", paramLabel = "<password>", description = "Prompt for password")
    public boolean password;

    @PropertyMapping(EXTERNAL_AUTHENTICATION)
    @Option(names = "--external-authentication", paramLabel = "<externalAuthentication>", description = "Enable external authentication")
    public boolean externalAuthentication;

    @PropertyMapping(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS)
    @Option(names = "--external-authentication-redirect-handler", paramLabel = "<externalAuthenticationRedirectHandler>", description = "External authentication redirect handlers: ${COMPLETION-CANDIDATES} " + DEFAULT_VALUE, defaultValue = "ALL")
    public List<ExternalRedirectStrategy> externalAuthenticationRedirectHandler = new ArrayList<>();

    @PropertyMapping(SOURCE)
    @Option(names = "--source", paramLabel = "<source>", description = "Name of the client to use as source that submits the query (default: " + SOURCE_DEFAULT + ")")
    public Optional<String> source;

    @PropertyMapping(CLIENT_INFO)
    @Option(names = "--client-info", paramLabel = "<info>", description = "Extra information about client making query")
    public Optional<String> clientInfo;

    @PropertyMapping(CLIENT_TAGS)
    @Option(names = "--client-tags", paramLabel = "<tags>", description = "Client tags")
    public Optional<String> clientTags;

    @PropertyMapping(TRACE_TOKEN)
    @Option(names = "--trace-token", paramLabel = "<token>", description = "Trace token")
    public Optional<String> traceToken;

    @PropertyMapping(CATALOG)
    @Option(names = "--catalog", paramLabel = "<catalog>", description = "Default catalog")
    public Optional<String> catalog;

    @PropertyMapping(SCHEMA)
    @Option(names = "--schema", paramLabel = "<schema>", description = "Default schema")
    public Optional<String> schema;

    @Option(names = {"-f", "--file"}, paramLabel = "<file>", description = "Execute statements from file and exit")
    public String file;

    @Option(names = DEBUG_OPTION_NAME, paramLabel = "<debug>", description = "Enable debug information")
    public boolean debug;

    @Option(names = "--history-file", paramLabel = "<historyFile>", defaultValue = "${env:TRINO_HISTORY_FILE:-${sys:user.home}/.trino_history}", description = "Path to the history file " + DEFAULT_VALUE)
    public String historyFile;

    @Option(names = "--network-logging", paramLabel = "<level>", defaultValue = "NONE", description = "Network logging level [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public HttpLoggingInterceptor.Level networkLogging;

    @Option(names = "--progress", paramLabel = "<progress>", description = "Show query progress", negatable = true)
    public Optional<Boolean> progress;

    @Option(names = "--execute", paramLabel = "<execute>", description = "Execute specified statements and exit")
    public String execute;

    @Option(names = "--output-format", paramLabel = "<format>", defaultValue = "CSV", description = "Output format for batch mode [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public OutputFormat outputFormat;

    @Option(names = "--output-format-interactive", paramLabel = "<format>", defaultValue = "ALIGNED", description = "Output format for interactive mode [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public OutputFormat outputFormatInteractive;

    @Option(names = "--pager", paramLabel = "<pager>", defaultValue = "${env:TRINO_PAGER}", description = "Path to the pager program used to display the query results")
    public Optional<String> pager;

    @Option(names = "--resource-estimate", paramLabel = "<estimate>", description = "Resource estimate (property can be used multiple times; format is key=value)")
    public final List<ClientResourceEstimate> resourceEstimates = new ArrayList<>();

    @PropertyMapping(SESSION_PROPERTIES)
    @Option(names = "--session", paramLabel = "<session>", description = "Session property (property can be used multiple times; format is key=value; use 'SHOW SESSION' to see available properties)")
    public final List<ClientSessionProperty> sessionProperties = new ArrayList<>();

    @PropertyMapping(SESSION_USER)
    @Option(names = "--session-user", paramLabel = "<user>", description = "Username to impersonate")
    public Optional<String> sessionUser;

    @PropertyMapping(EXTRA_CREDENTIALS)
    @Option(names = "--extra-credential", paramLabel = "<credential>", description = "Extra credentials (property can be used multiple times; format is key=value)")
    public final List<ClientExtraCredential> extraCredentials = new ArrayList<>();

    @PropertyMapping(SOCKS_PROXY)
    @Option(names = "--socks-proxy", paramLabel = "<proxy>", description = "SOCKS proxy to use for server connections")
    public Optional<HostAndPort> socksProxy;

    @PropertyMapping(HTTP_PROXY)
    @Option(names = "--http-proxy", paramLabel = "<proxy>", description = "HTTP proxy to use for server connections")
    public Optional<HostAndPort> httpProxy;

    @Option(names = "--client-request-timeout", paramLabel = "<timeout>", defaultValue = "2m", description = "Client request timeout " + DEFAULT_VALUE)
    public Duration clientRequestTimeout;

    @Option(names = "--ignore-errors", description = "Continue processing in batch mode when an error occurs (default is to exit immediately)")
    public boolean ignoreErrors;

    @Option(names = "--timezone", paramLabel = "<timezone>", description = "Session time zone " + DEFAULT_VALUE)
    public ZoneId timeZone = ZoneId.systemDefault();

    @Option(names = "--disable-compression", description = "Disable compression of query results")
    public boolean disableCompression;

    @Option(names = "--editing-mode", paramLabel = "<editing-mode>", defaultValue = "EMACS", description = "Editing mode [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public EditingMode editingMode;

    @Option(names = "--disable-auto-suggestion", description = "Disable auto suggestion")
    public boolean disableAutoSuggestion;

    public enum OutputFormat
    {
        AUTO,
        ALIGNED,
        VERTICAL,
        TSV,
        TSV_HEADER,
        CSV,
        CSV_HEADER,
        CSV_UNQUOTED,
        CSV_HEADER_UNQUOTED,
        JSON,
        MARKDOWN,
        NULL
    }

    @Retention(RUNTIME)
    @interface PropertyMapping
    {
        PropertyName value();
    }

    public enum EditingMode
    {
        EMACS(LineReader.EMACS),
        VI(LineReader.VIINS);

        private final String keyMap;

        EditingMode(String keyMap)
        {
            this.keyMap = keyMap;
        }

        public String getKeyMap()
        {
            return keyMap;
        }
    }

    public ClientSession toClientSession(TrinoUri uri)
    {
        return ClientSession.builder()
                .server(uri.getHttpUri())
                .principal(user)
                .user(sessionUser)
                .source(source.orElse("trino-cli"))
                .traceToken(traceToken)
                .clientTags(parseClientTags(clientTags.orElse("")))
                .clientInfo(clientInfo.orElse(null))
                .catalog(uri.getCatalog().orElse(catalog.orElse(null)))
                .schema(uri.getSchema().orElse(schema.orElse(null)))
                .timeZone(timeZone)
                .locale(Locale.getDefault())
                .resourceEstimates(toResourceEstimates(resourceEstimates))
                .properties(toProperties(sessionProperties))
                .credentials(toExtraCredentials(extraCredentials))
                .transactionId(null)
                .clientRequestTimeout(clientRequestTimeout)
                .compressionDisabled(disableCompression)
                .build();
    }

    public TrinoUri getTrinoUri()
    {
        return getTrinoUri(ImmutableMap.of());
    }

    public TrinoUri getTrinoUri(Map<PropertyName, String> restrictedProperties)
    {
        URI uri;
        if (url.isPresent()) {
            if (server.isPresent()) {
                throw new IllegalArgumentException("Using both the URL parameter and the --server option is not allowed");
            }
            uri = parseServer(url.get());
        }
        else {
            uri = parseServer(server.orElse(SERVER_DEFAULT));
        }
        List<PropertyName> bannedProperties = ImmutableList.<PropertyName>builder()
                .addAll(restrictedProperties.keySet())
                .add(PASSWORD)
                .build();
        TrinoUri.Builder builder = TrinoUri.builder()
                .setUri(uri)
                .setRestrictedProperties(bannedProperties);
        catalog.ifPresent(builder::setCatalog);
        schema.ifPresent(builder::setSchema);
        user.ifPresent(builder::setUser);
        sessionUser.ifPresent(builder::setSessionUser);
        if (password) {
            builder.setPassword(getPassword());
        }
        krb5RemoteServiceName.ifPresent(builder::setKerberosRemoveServiceName);
        krb5ServicePrincipalPattern.ifPresent(builder::setKerberosServicePrincipalPattern);
        if (krb5RemoteServiceName.isPresent()) {
            krb5ConfigPath.ifPresent(builder::setKerberosConfigPath);
            krb5KeytabPath.ifPresent(builder::setKerberosKeytabPath);
        }
        krb5CredentialCachePath.ifPresent(builder::setKerberosCredentialCachePath);
        krb5Principal.ifPresent(builder::setKerberosPrincipal);
        if (krb5DisableRemoteServiceHostnameCanonicalization) {
            builder.setKerberosUseCanonicalHostname(false);
        }
        boolean useSecureConnection = uri.getScheme().equals("https") || (uri.getScheme().equals("trino") && uri.getPort() == 443);
        if (useSecureConnection) {
            builder.setSsl(true);
        }
        if (insecure) {
            builder.setSslVerificationNone();
        }
        keystorePath.ifPresent(builder::setSslKeyStorePath);
        keystorePassword.ifPresent(builder::setSslKeyStorePassword);
        keystoreType.ifPresent(builder::setSslKeyStoreType);
        truststorePath.ifPresent(builder::setSslTrustStorePath);
        truststorePassword.ifPresent(builder::setSslTrustStorePassword);
        truststoreType.ifPresent(builder::setSslTrustStoreType);
        if (useSystemTruststore) {
            builder.setSslUseSystemTrustStore(true);
        }
        accessToken.ifPresent(builder::setAccessToken);
        if (!extraCredentials.isEmpty()) {
            builder.setExtraCredentials(toExtraCredentials(extraCredentials));
        }
        if (!sessionProperties.isEmpty()) {
            builder.setSessionProperties(toProperties(sessionProperties));
        }
        builder.setExternalAuthentication(externalAuthentication);
        builder.setExternalRedirectStrategies(externalAuthenticationRedirectHandler);
        source.ifPresent(builder::setSource);
        clientInfo.ifPresent(builder::setClientInfo);
        clientTags.ifPresent(builder::setClientTags);
        traceToken.ifPresent(builder::setTraceToken);
        socksProxy.ifPresent(builder::setSocksProxy);
        httpProxy.ifPresent(builder::setHttpProxy);
        builder.setDisableCompression(disableCompression);
        TrinoUri trinoUri;

        try {
            trinoUri = builder.build();
        }
        catch (RestrictedPropertyException e) {
            if (e.getPropertyName() == PropertyName.PASSWORD) {
                throw new IllegalArgumentException(
                        "Setting the password in the URL parameter is not allowed, " +
                                "use the `--password` option or the `TRINO_PASSWORD` environment variable");
            }
            throw new IllegalArgumentException(format(
                    "Connection property '%s' cannot be set in the URL when option '%s' is set",
                    e.getPropertyName(),
                    restrictedProperties.get(e.getPropertyName())), e);
        }
        catch (SQLException e) {
            throw new IllegalArgumentException(e);
        }
        return trinoUri;
    }

    private String getPassword()
    {
        checkState(user.isPresent() && !user.get().isEmpty(), "Both username and password must be specified");
        String defaultPassword = System.getenv("TRINO_PASSWORD");
        if (defaultPassword != null) {
            return defaultPassword;
        }

        java.io.Console console = System.console();
        if (console != null) {
            char[] password = console.readPassword("Password: ");
            if (password != null) {
                return new String(password);
            }
            return "";
        }

        LineReader reader = LineReaderBuilder.builder().terminal(getTerminal()).build();
        return reader.readLine("Password: ", (char) 0);
    }

    public static URI parseServer(String server)
    {
        String lowerServer = server.toLowerCase(ENGLISH);
        if (lowerServer.startsWith("http://") || lowerServer.startsWith("https://") || lowerServer.startsWith("trino://")) {
            return URI.create(server);
        }

        HostAndPort host = HostAndPort.fromString(server);
        try {
            int port = host.getPortOrDefault(80);
            String scheme = port == 443 ? "https" : "http";
            return new URI(scheme, null, host.getHost(), port, null, null, null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Set<String> parseClientTags(String clientTagsString)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        return ImmutableSet.copyOf(splitter.split(nullToEmpty(clientTagsString)));
    }

    public static Map<String, String> toProperties(List<ClientSessionProperty> sessionProperties)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ClientSessionProperty sessionProperty : sessionProperties) {
            String name = sessionProperty.getName();
            if (sessionProperty.getCatalog().isPresent()) {
                name = sessionProperty.getCatalog().get() + "." + name;
            }
            builder.put(name, sessionProperty.getValue());
        }
        return builder.buildOrThrow();
    }

    public static Map<String, String> toResourceEstimates(List<ClientResourceEstimate> estimates)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ClientResourceEstimate estimate : estimates) {
            builder.put(estimate.getResource(), estimate.getEstimate());
        }
        return builder.buildOrThrow();
    }

    public static Map<String, String> toExtraCredentials(List<ClientExtraCredential> extraCredentials)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ClientExtraCredential credential : extraCredentials) {
            builder.put(credential.getName(), credential.getValue());
        }
        return builder.buildOrThrow();
    }

    public static final class ClientResourceEstimate
    {
        private final String resource;
        private final String estimate;

        public ClientResourceEstimate(String resourceEstimate)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(resourceEstimate);
            checkArgument(nameValue.size() == 2, "Resource estimate: %s", resourceEstimate);

            this.resource = nameValue.get(0);
            this.estimate = nameValue.get(1);
            checkArgument(!resource.isEmpty(), "Resource name is empty");
            checkArgument(!estimate.isEmpty(), "Resource estimate is empty");
            checkArgument(PRINTABLE_ASCII.matchesAllOf(resource), "Resource contains spaces or is not ASCII: %s", resource);
            checkArgument(resource.indexOf('=') < 0, "Resource must not contain '=': %s", resource);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(estimate), "Resource estimate contains spaces or is not ASCII: %s", resource);
        }

        @VisibleForTesting
        public ClientResourceEstimate(String resource, String estimate)
        {
            this.resource = requireNonNull(resource, "resource is null");
            this.estimate = estimate;
        }

        public String getResource()
        {
            return resource;
        }

        public String getEstimate()
        {
            return estimate;
        }

        @Override
        public String toString()
        {
            return resource + '=' + estimate;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClientResourceEstimate other = (ClientResourceEstimate) o;
            return Objects.equals(resource, other.resource) &&
                    Objects.equals(estimate, other.estimate);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(resource, estimate);
        }
    }

    public static final class ClientSessionProperty
    {
        private static final Splitter NAME_SPLITTER = Splitter.on('.');
        private final Optional<String> catalog;
        private final String name;
        private final String value;

        public ClientSessionProperty(String property)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(property);
            checkArgument(nameValue.size() == 2, "Session property: %s", property);

            List<String> nameParts = NAME_SPLITTER.splitToList(nameValue.get(0));
            checkArgument(nameParts.size() == 1 || nameParts.size() == 2, "Invalid session property: %s", property);
            if (nameParts.size() == 1) {
                catalog = Optional.empty();
                name = nameParts.get(0);
            }
            else {
                catalog = Optional.of(nameParts.get(0));
                name = nameParts.get(1);
            }
            value = nameValue.get(1);

            verifyProperty(catalog, name, value);
        }

        public ClientSessionProperty(Optional<String> catalog, String name, String value)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            this.name = requireNonNull(name, "name is null");
            this.value = requireNonNull(value, "value is null");

            verifyProperty(catalog, name, value);
        }

        private static void verifyProperty(Optional<String> catalog, String name, String value)
        {
            checkArgument(!catalog.isPresent() || !catalog.get().isEmpty(), "Invalid session property: %s.%s:%s", catalog, name, value);
            checkArgument(!name.isEmpty(), "Session property name is empty");
            checkArgument(catalog.orElse("").indexOf('=') < 0, "Session property catalog must not contain '=': %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(catalog.orElse("")), "Session property catalog contains spaces or is not ASCII: %s", name);
            checkArgument(name.indexOf('=') < 0, "Session property name must not contain '=': %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(name), "Session property name contains spaces or is not ASCII: %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(value), "Session property value contains spaces or is not ASCII: %s", value);
        }

        public Optional<String> getCatalog()
        {
            return catalog;
        }

        public String getName()
        {
            return name;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return (catalog.isPresent() ? catalog.get() + '.' : "") + name + '=' + value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(catalog, name, value);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ClientSessionProperty other = (ClientSessionProperty) obj;
            return Objects.equals(this.catalog, other.catalog) &&
                    Objects.equals(this.name, other.name) &&
                    Objects.equals(this.value, other.value);
        }
    }

    public static final class ClientExtraCredential
    {
        private final String name;
        private final String value;

        public ClientExtraCredential(String extraCredential)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(extraCredential);
            checkArgument(nameValue.size() == 2, "Extra credential: %s", extraCredential);

            this.name = nameValue.get(0);
            this.value = nameValue.get(1);
            checkArgument(!name.isEmpty(), "Credential name is empty");
            checkArgument(!value.isEmpty(), "Credential value is empty");
            checkArgument(PRINTABLE_ASCII.matchesAllOf(name), "Credential name contains spaces or is not ASCII: %s", name);
            checkArgument(name.indexOf('=') < 0, "Credential name must not contain '=': %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(value), "Credential value contains space or is not ASCII: %s", name);
        }

        public ClientExtraCredential(String name, String value)
        {
            this.name = requireNonNull(name, "name is null");
            this.value = value;
        }

        public String getName()
        {
            return name;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return name + '=' + value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ClientExtraCredential other = (ClientExtraCredential) o;
            return Objects.equals(name, other.name) && Objects.equals(value, other.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, value);
        }
    }
}
