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
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.Console;
import java.lang.annotation.Retention;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.cli.TrinoConnectionProperties.ACCESS_TOKEN;
import static io.trino.cli.TrinoConnectionProperties.CATALOG;
import static io.trino.cli.TrinoConnectionProperties.CLIENT_INFO;
import static io.trino.cli.TrinoConnectionProperties.CLIENT_TAGS;
import static io.trino.cli.TrinoConnectionProperties.DISABLE_COMPRESSION;
import static io.trino.cli.TrinoConnectionProperties.ENCODING;
import static io.trino.cli.TrinoConnectionProperties.EXTERNAL_AUTHENTICATION;
import static io.trino.cli.TrinoConnectionProperties.EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS;
import static io.trino.cli.TrinoConnectionProperties.EXTERNAL_AUTHENTICATION_TOKEN_CACHE;
import static io.trino.cli.TrinoConnectionProperties.EXTRA_CREDENTIALS;
import static io.trino.cli.TrinoConnectionProperties.EXTRA_HEADERS;
import static io.trino.cli.TrinoConnectionProperties.HTTP_LOGGING_LEVEL;
import static io.trino.cli.TrinoConnectionProperties.HTTP_PROXY;
import static io.trino.cli.TrinoConnectionProperties.KERBEROS_CONFIG_PATH;
import static io.trino.cli.TrinoConnectionProperties.KERBEROS_CREDENTIAL_CACHE_PATH;
import static io.trino.cli.TrinoConnectionProperties.KERBEROS_KEYTAB_PATH;
import static io.trino.cli.TrinoConnectionProperties.KERBEROS_PRINCIPAL;
import static io.trino.cli.TrinoConnectionProperties.KERBEROS_REMOTE_SERVICE_NAME;
import static io.trino.cli.TrinoConnectionProperties.KERBEROS_SERVICE_PRINCIPAL_PATTERN;
import static io.trino.cli.TrinoConnectionProperties.KERBEROS_USE_CANONICAL_HOSTNAME;
import static io.trino.cli.TrinoConnectionProperties.OAUTH2_CLIENT_ID;
import static io.trino.cli.TrinoConnectionProperties.OAUTH2_CLIENT_SECRET;
import static io.trino.cli.TrinoConnectionProperties.OAUTH2_SCOPE;
import static io.trino.cli.TrinoConnectionProperties.OAUTH2_TOKEN_ENDPOINT;
import static io.trino.cli.TrinoConnectionProperties.PASSWORD;
import static io.trino.cli.TrinoConnectionProperties.RESOURCE_ESTIMATES;
import static io.trino.cli.TrinoConnectionProperties.SCHEMA;
import static io.trino.cli.TrinoConnectionProperties.SESSION_PROPERTIES;
import static io.trino.cli.TrinoConnectionProperties.SESSION_USER;
import static io.trino.cli.TrinoConnectionProperties.SOCKS_PROXY;
import static io.trino.cli.TrinoConnectionProperties.SOURCE;
import static io.trino.cli.TrinoConnectionProperties.SQL_PATH;
import static io.trino.cli.TrinoConnectionProperties.SSL;
import static io.trino.cli.TrinoConnectionProperties.SSL_KEY_STORE_PASSWORD;
import static io.trino.cli.TrinoConnectionProperties.SSL_KEY_STORE_PATH;
import static io.trino.cli.TrinoConnectionProperties.SSL_KEY_STORE_TYPE;
import static io.trino.cli.TrinoConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static io.trino.cli.TrinoConnectionProperties.SSL_TRUST_STORE_PATH;
import static io.trino.cli.TrinoConnectionProperties.SSL_TRUST_STORE_TYPE;
import static io.trino.cli.TrinoConnectionProperties.SSL_USE_SYSTEM_KEY_STORE;
import static io.trino.cli.TrinoConnectionProperties.SSL_USE_SYSTEM_TRUST_STORE;
import static io.trino.cli.TrinoConnectionProperties.SSL_VERIFICATION;
import static io.trino.cli.TrinoConnectionProperties.TIMEOUT;
import static io.trino.cli.TrinoConnectionProperties.TIMEZONE;
import static io.trino.cli.TrinoConnectionProperties.TRACE_TOKEN;
import static io.trino.cli.TrinoConnectionProperties.USER;
import static java.lang.String.format;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ClientOptions
{
    private static final Splitter NAME_VALUE_SPLITTER = Splitter.on('=').limit(2);
    private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E); // spaces are not allowed
    private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";
    private static final String SERVER_DEFAULT = "localhost:8080";
    private static final String SOURCE_DEFAULT = "trino-cli";
    private static final String JDBC_URL_PREFIX = "jdbc:";
    private static final String TRINO_JDBC_URL_PREFIX = "jdbc:trino:";
    private static final Set<String> GENERIC_DRIVER_PROPERTIES = ImmutableSet.of(USER, PASSWORD, CATALOG, SCHEMA);
    private static final Splitter URL_PARAMETERS_SPLITTER = Splitter.on('&').omitEmptyStrings();
    static final String DEBUG_OPTION_NAME = "--debug";

    @Parameters(paramLabel = "URL", description = "Trino server URL, or a JDBC URL for a driver available on the class path", arity = "0..1")
    public Optional<String> url;

    @Option(names = "--server", paramLabel = "<server>", description = "Trino server location, or a JDBC URL for a driver available on the class path (default: " + SERVER_DEFAULT + ")")
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
    public Optional<String> krb5CredentialCachePath = defaultKerberosCredentialCachePath();

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

    @PropertyMapping(SSL_USE_SYSTEM_KEY_STORE)
    @Option(names = "--use-system-keystore", description = "Use default operating system keystore")
    public boolean useSystemKeystore;

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
    public List<ExternalAuthenticationRedirectHandler> externalAuthenticationRedirectHandler = new ArrayList<>();

    @PropertyMapping(OAUTH2_CLIENT_ID)
    @Option(names = "--oauth2-client-id", paramLabel = "<clientId>", description = "OAuth2 client ID for client credentials authentication")
    public Optional<String> oauth2ClientId;

    @PropertyMapping(OAUTH2_CLIENT_SECRET)
    @Option(names = "--oauth2-client-secret", description = "Prompt for OAuth2 client secret for client credentials authentication")
    public boolean oauth2ClientSecret;

    @PropertyMapping(OAUTH2_TOKEN_ENDPOINT)
    @Option(names = "--oauth2-token-endpoint", paramLabel = "<tokenEndpoint>", description = "OAuth2 token endpoint URL for client credentials authentication")
    public Optional<String> oauth2TokenEndpoint;

    @PropertyMapping(OAUTH2_SCOPE)
    @Option(names = "--oauth2-scope", paramLabel = "<scope>", description = "OAuth2 scope for client credentials authentication (overrides the scope from the server challenge)")
    public Optional<String> oauth2Scope;

    @PropertyMapping(SOURCE)
    @Option(names = "--source", paramLabel = "<source>", description = "Name of the client to use as source that submits the query (default: " + SOURCE_DEFAULT + ")")
    public Optional<String> source;

    @PropertyMapping(CLIENT_INFO)
    @Option(names = "--client-info", paramLabel = "<info>", description = "Extra information about client making query")
    public Optional<String> clientInfo;

    @PropertyMapping(CLIENT_TAGS)
    @Option(names = "--client-tags", paramLabel = "<tags>", description = "Client tags", converter = ClientTagsConverter.class)
    public Optional<Set<String>> clientTags;

    @PropertyMapping(EXTRA_HEADERS)
    @Option(names = "--extra-header", paramLabel = "<header>", description = "Additional HTTP header to add to HTTP requests (property can be used multiple times; format is key=value)")
    public List<ExtraHeader> extraHeaders = new ArrayList<>();

    @PropertyMapping(TRACE_TOKEN)
    @Option(names = "--trace-token", paramLabel = "<token>", description = "Trace token")
    public Optional<String> traceToken;

    @PropertyMapping(CATALOG)
    @Option(names = "--catalog", paramLabel = "<catalog>", description = "Default catalog")
    public Optional<String> catalog;

    @PropertyMapping(SCHEMA)
    @Option(names = "--schema", paramLabel = "<schema>", description = "Default schema")
    public Optional<String> schema;

    @PropertyMapping(SQL_PATH)
    @Option(names = "--path", paramLabel = "<catalog.schema>", description = "Default SQL path", arity = "0..*")
    public List<String> path = ImmutableList.of();

    @Option(names = {"-f", "--file"}, paramLabel = "<file>", description = "Execute statements from file and exit")
    public String file;

    @Option(names = DEBUG_OPTION_NAME, paramLabel = "<debug>", description = "Enable debug information")
    public boolean debug;

    @Option(names = "--history-file", paramLabel = "<historyFile>", defaultValue = "${env:TRINO_HISTORY_FILE:-${sys:user.home}/.trino_history}", description = "Path to the history file " + DEFAULT_VALUE)
    public String historyFile;

    @PropertyMapping(HTTP_LOGGING_LEVEL)
    @Option(names = "--network-logging", paramLabel = "<level>", defaultValue = "NONE", description = "Network logging level [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public Optional<NetworkLogging> networkLogging;

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

    @PropertyMapping(RESOURCE_ESTIMATES)
    @Option(names = "--resource-estimate", paramLabel = "<estimate>", description = "Resource estimate (property can be used multiple times; format is key=value)")
    public List<ClientResourceEstimate> resourceEstimates = new ArrayList<>();

    @PropertyMapping(SESSION_PROPERTIES)
    @Option(names = "--session", paramLabel = "<session>", description = "Session property (property can be used multiple times; format is key=value; use 'SHOW SESSION' to see available properties)")
    public List<ClientSessionProperty> sessionProperties = new ArrayList<>();

    @PropertyMapping(SESSION_USER)
    @Option(names = "--session-user", paramLabel = "<user>", description = "Username to impersonate")
    public Optional<String> sessionUser;

    @PropertyMapping(EXTRA_CREDENTIALS)
    @Option(names = "--extra-credential", paramLabel = "<credential>", description = "Extra credentials (property can be used multiple times; format is key=value)")
    public List<ClientExtraCredential> extraCredentials = new ArrayList<>();

    @PropertyMapping(SOCKS_PROXY)
    @Option(names = "--socks-proxy", paramLabel = "<proxy>", description = "SOCKS proxy to use for server connections")
    public Optional<HostAndPort> socksProxy;

    @PropertyMapping(HTTP_PROXY)
    @Option(names = "--http-proxy", paramLabel = "<proxy>", description = "HTTP proxy to use for server connections")
    public Optional<HostAndPort> httpProxy;

    @PropertyMapping(TIMEOUT)
    @Option(names = "--client-request-timeout", paramLabel = "<timeout>", defaultValue = "2m", description = "Client request timeout " + DEFAULT_VALUE)
    public Duration clientRequestTimeout;

    @Option(names = "--ignore-errors", description = "Continue processing in batch mode when an error occurs (default is to exit immediately)")
    public boolean ignoreErrors;

    @PropertyMapping(TIMEZONE)
    @Option(names = "--timezone", paramLabel = "<timezone>", description = "Session time zone " + DEFAULT_VALUE)
    public ZoneId timeZone = ZoneId.systemDefault();

    @PropertyMapping(DISABLE_COMPRESSION)
    @Option(names = "--disable-compression", description = "Disable compression of query results")
    public boolean disableCompression;

    @PropertyMapping(ENCODING)
    @Option(names = "--encoding", paramLabel = "<encoding>", description = "Spooled encoding [available: ${ENCODINGS}] " + DEFAULT_VALUE, defaultValue = "${PREFERRED_ENCODINGS}")
    public Optional<String> encoding = Optional.empty();

    @Option(names = "--editing-mode", paramLabel = "<editing-mode>", defaultValue = "EMACS", description = "Editing mode [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public EditingMode editingMode;

    @Option(names = "--disable-auto-suggestion", description = "Disable auto suggestion")
    public boolean disableAutoSuggestion;

    @Option(names = "--decimal-data-size", description = "Show data size and rate in base 10 rather than base 2")
    public boolean decimalDataSize;

    @Option(names = "--theme", paramLabel = "<theme>", defaultValue = "AUTO", description = "Color theme [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public Theme theme;

    @Option(names = "--max-buffered-rows", paramLabel = "<maxBufferedRows>", description = "Maximum number of rows to buffer in memory before writing to output (default: ${DEFAULT-VALUE})")
    public int maxBufferedRows = 10_000;

    @Option(names = "--max-queued-rows", paramLabel = "<maxQueuedRows>", description = "Maximum number of rows to queue before blocking the query (default: ${DEFAULT-VALUE})")
    public int maxQueuedRows = 50_000;

    @Option(names = "--driver-path", paramLabel = "<path>", description = "JAR file, or directory with JAR files, of the JDBC driver for a URL of a database other than Trino (option can be used multiple times)")
    public List<Path> driverPaths = new ArrayList<>();

    @Option(names = "--driver-class", paramLabel = "<class>", description = "Class name of the JDBC driver, for drivers that are not registered as a service")
    public Optional<String> driverClass;

    @Option(names = "--connection-property", paramLabel = "<property>", description = "JDBC driver connection property (option can be used multiple times; format is key=value)")
    public Map<String, String> connectionProperties = new LinkedHashMap<>();

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
        NULL,
    }

    @Retention(RUNTIME)
    @interface PropertyMapping
    {
        /**
         * Name of the connection property of the Trino JDBC driver
         */
        String value();
    }

    public enum ExternalAuthenticationRedirectHandler
    {
        DESKTOP_OPEN,
        SYSTEM_OPEN,
        PRINT,
        OPEN,
        ALL,
    }

    public enum NetworkLogging
    {
        NONE,
        BASIC,
        HEADERS,
        BODY,
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

    public ConnectionFactory createConnectionFactory(Map<String, String> restrictedProperties)
    {
        Optional<String> jdbcUrl = url.or(() -> server)
                .filter(ClientOptions::isNonTrinoJdbcUrl);
        if (jdbcUrl.isPresent()) {
            if (url.isPresent() && server.isPresent()) {
                throw new IllegalArgumentException("Using both the URL parameter and the --server option is not allowed");
            }
            return createDriverConnectionFactory(jdbcUrl.get(), restrictedProperties);
        }

        if (!driverPaths.isEmpty() || driverClass.isPresent()) {
            throw new IllegalArgumentException("Options '--driver-path' and '--driver-class' can only be used with JDBC URLs of databases other than Trino");
        }
        return new TrinoConnectionFactory(toTrinoJdbcUrl(restrictedProperties), toTrinoConnectionProperties(restrictedProperties), clientRequestTimeout);
    }

    /**
     * Returns the Trino JDBC driver URL, after verifying it does not set connection properties that are also set by options.
     */
    public String toTrinoJdbcUrl(Map<String, String> restrictedProperties)
    {
        URI uri = getServerUri();
        Map<String, String> urlProperties = getUrlProperties(uri);
        for (String property : urlProperties.keySet()) {
            if (property.equals(PASSWORD)) {
                throw new IllegalArgumentException(
                        "Setting the password in the URL parameter is not allowed, " +
                                "use the `--password` option or the `TRINO_PASSWORD` environment variable");
            }
            if (property.equals(OAUTH2_CLIENT_SECRET)) {
                throw new IllegalArgumentException(
                        "Setting the OAuth2 client secret in the URL parameter is not allowed, " +
                                "use the `--oauth2-client-secret` option or the `TRINO_OAUTH2_CLIENT_SECRET` environment variable");
            }
            if (restrictedProperties.containsKey(property)) {
                throw new IllegalArgumentException(format(
                        "Connection property '%s' cannot be set in the URL when option '%s' is set",
                        property,
                        restrictedProperties.get(property)));
            }
        }
        return TRINO_JDBC_URL_PREFIX + uri.getRawSchemeSpecificPart();
    }

    /**
     * Returns the Trino JDBC driver connection properties set by options, in the format of the driver.
     */
    public Properties toTrinoConnectionProperties(Map<String, String> restrictedProperties)
    {
        Map<String, String> urlProperties = getUrlProperties(getServerUri());
        Properties properties = new Properties();
        catalog.ifPresent(value -> properties.setProperty(CATALOG, value));
        schema.ifPresent(value -> properties.setProperty(SCHEMA, value));
        user.ifPresent(value -> properties.setProperty(USER, value));
        sessionUser.ifPresent(value -> properties.setProperty(SESSION_USER, value));
        if (!path.isEmpty()) {
            properties.setProperty(SQL_PATH, String.join(",", path));
        }
        if (password) {
            properties.setProperty(PASSWORD, getPassword());
        }
        krb5RemoteServiceName.ifPresent(value -> properties.setProperty(KERBEROS_REMOTE_SERVICE_NAME, value));
        krb5ServicePrincipalPattern.ifPresent(value -> properties.setProperty(KERBEROS_SERVICE_PRINCIPAL_PATTERN, value));
        if (krb5RemoteServiceName.isPresent()) {
            krb5ConfigPath.ifPresent(value -> properties.setProperty(KERBEROS_CONFIG_PATH, value));
            krb5KeytabPath.ifPresent(value -> properties.setProperty(KERBEROS_KEYTAB_PATH, value));
            krb5CredentialCachePath.ifPresent(value -> properties.setProperty(KERBEROS_CREDENTIAL_CACHE_PATH, value));
        }
        krb5Principal.ifPresent(value -> properties.setProperty(KERBEROS_PRINCIPAL, value));
        if (krb5DisableRemoteServiceHostnameCanonicalization) {
            properties.setProperty(KERBEROS_USE_CANONICAL_HOSTNAME, "false");
        }
        if (insecure) {
            properties.setProperty(SSL_VERIFICATION, "NONE");
        }
        keystorePath.ifPresent(value -> properties.setProperty(SSL_KEY_STORE_PATH, value));
        keystorePassword.ifPresent(value -> properties.setProperty(SSL_KEY_STORE_PASSWORD, value));
        keystoreType.ifPresent(value -> properties.setProperty(SSL_KEY_STORE_TYPE, value));
        if (useSystemKeystore) {
            properties.setProperty(SSL_USE_SYSTEM_KEY_STORE, "true");
        }
        truststorePath.ifPresent(value -> properties.setProperty(SSL_TRUST_STORE_PATH, value));
        truststorePassword.ifPresent(value -> properties.setProperty(SSL_TRUST_STORE_PASSWORD, value));
        truststoreType.ifPresent(value -> properties.setProperty(SSL_TRUST_STORE_TYPE, value));
        if (useSystemTruststore) {
            properties.setProperty(SSL_USE_SYSTEM_TRUST_STORE, "true");
        }
        accessToken.ifPresent(value -> properties.setProperty(ACCESS_TOKEN, value));
        if (!extraCredentials.isEmpty()) {
            properties.setProperty(EXTRA_CREDENTIALS, toMapProperty(toExtraCredentials(extraCredentials)));
        }
        if (!sessionProperties.isEmpty()) {
            properties.setProperty(SESSION_PROPERTIES, toMapProperty(toProperties(sessionProperties)));
        }
        if (!extraHeaders.isEmpty()) {
            properties.setProperty(EXTRA_HEADERS, toMapProperty(toExtraHeaders(extraHeaders)));
        }
        if (!resourceEstimates.isEmpty()) {
            properties.setProperty(RESOURCE_ESTIMATES, toMapProperty(toResourceEstimates(resourceEstimates)));
        }
        properties.setProperty(EXTERNAL_AUTHENTICATION, String.valueOf(externalAuthentication));
        if (!externalAuthenticationRedirectHandler.isEmpty()) {
            properties.setProperty(EXTERNAL_AUTHENTICATION_REDIRECT_HANDLERS, externalAuthenticationRedirectHandler.stream()
                    .map(Enum::name)
                    .collect(joining(",")));
        }
        if (externalAuthentication && !urlProperties.containsKey(EXTERNAL_AUTHENTICATION_TOKEN_CACHE)) {
            // share the token between the query and the auto-completion connections, to authenticate only once
            properties.setProperty(EXTERNAL_AUTHENTICATION_TOKEN_CACHE, "MEMORY");
        }
        oauth2ClientId.ifPresent(value -> properties.setProperty(OAUTH2_CLIENT_ID, value));
        if (oauth2ClientSecret) {
            properties.setProperty(OAUTH2_CLIENT_SECRET, getClientSecret());
        }
        oauth2TokenEndpoint.ifPresent(value -> properties.setProperty(OAUTH2_TOKEN_ENDPOINT, value));
        oauth2Scope.ifPresent(value -> properties.setProperty(OAUTH2_SCOPE, value));
        if (source.isPresent()) {
            properties.setProperty(SOURCE, source.get());
        }
        else if (!urlProperties.containsKey(SOURCE)) {
            properties.setProperty(SOURCE, SOURCE_DEFAULT);
        }
        clientInfo.ifPresent(value -> properties.setProperty(CLIENT_INFO, value));
        clientTags.ifPresent(value -> properties.setProperty(CLIENT_TAGS, String.join(",", value)));
        traceToken.ifPresent(value -> properties.setProperty(TRACE_TOKEN, value));
        socksProxy.ifPresent(value -> properties.setProperty(SOCKS_PROXY, value.toString()));
        httpProxy.ifPresent(value -> properties.setProperty(HTTP_PROXY, value.toString()));
        properties.setProperty(TIMEZONE, timeZone.getId());
        properties.setProperty(DISABLE_COMPRESSION, String.valueOf(disableCompression));
        properties.setProperty(TIMEOUT, clientRequestTimeout.toString());
        networkLogging.ifPresent(value -> properties.setProperty(HTTP_LOGGING_LEVEL, value.name()));
        encoding.ifPresent(value -> properties.setProperty(ENCODING, value));
        if (!urlProperties.containsKey(SSL)) {
            isSecureScheme(getServerUri()).ifPresent(secure -> properties.setProperty(SSL, String.valueOf(secure)));
        }
        // explicit properties override the defaults set above
        properties.putAll(getConnectionProperties(restrictedProperties));
        return properties;
    }

    private ConnectionFactory createDriverConnectionFactory(String jdbcUrl, Map<String, String> restrictedProperties)
    {
        Set<String> unsupportedProperties = Sets.difference(restrictedProperties.keySet(), GENERIC_DRIVER_PROPERTIES);
        if (!unsupportedProperties.isEmpty()) {
            throw new IllegalArgumentException(format(
                    "Option '%s' is only supported when connecting to Trino",
                    restrictedProperties.get(unsupportedProperties.iterator().next())));
        }
        Properties properties = new Properties();
        properties.putAll(getConnectionProperties(restrictedProperties));
        if (restrictedProperties.containsKey(USER)) {
            user.ifPresent(value -> properties.setProperty(USER, value));
        }
        if (password) {
            properties.setProperty(PASSWORD, getPassword());
        }
        try {
            return DriverConnectionFactory.builder(jdbcUrl)
                    .setProperties(properties)
                    .setCatalog(catalog)
                    .setSchema(schema)
                    .setDriverPaths(driverPaths)
                    .setDriverClass(driverClass)
                    .build();
        }
        catch (SQLException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private Map<String, String> getConnectionProperties(Map<String, String> restrictedProperties)
    {
        for (String property : connectionProperties.keySet()) {
            if (property.equals(PASSWORD) || property.equals(OAUTH2_CLIENT_SECRET)) {
                throw new IllegalArgumentException(format(
                        "Connection property '%s' cannot be set with the --connection-property option, use the --password or --oauth2-client-secret option instead",
                        property));
            }
            if (restrictedProperties.containsKey(property)) {
                throw new IllegalArgumentException(format(
                        "Connection property '%s' cannot be set with the --connection-property option when option '%s' is set",
                        property,
                        restrictedProperties.get(property)));
            }
        }
        return connectionProperties;
    }

    private static boolean isNonTrinoJdbcUrl(String url)
    {
        String lowerUrl = url.toLowerCase(ENGLISH);
        return lowerUrl.startsWith(JDBC_URL_PREFIX) && !lowerUrl.startsWith(TRINO_JDBC_URL_PREFIX);
    }

    private URI getServerUri()
    {
        if (url.isPresent()) {
            if (server.isPresent()) {
                throw new IllegalArgumentException("Using both the URL parameter and the --server option is not allowed");
            }
            return parseServer(url.get());
        }
        return parseServer(server.orElse(SERVER_DEFAULT));
    }

    /**
     * Returns connection properties set in the URL path and parameters, the same way the driver parses them.
     */
    private static Map<String, String> getUrlProperties(URI uri)
    {
        Map<String, String> properties = new HashMap<>();
        List<String> pathSegments = Splitter.on('/').omitEmptyStrings().splitToList(nullToEmpty(uri.getPath()));
        if (!pathSegments.isEmpty()) {
            properties.put(CATALOG, pathSegments.get(0));
        }
        if (pathSegments.size() > 1) {
            properties.put(SCHEMA, pathSegments.get(1));
        }
        for (String parameter : URL_PARAMETERS_SPLITTER.split(nullToEmpty(uri.getQuery()))) {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(parameter);
            properties.put(nameValue.get(0), nameValue.size() > 1 ? nameValue.get(1) : "");
        }
        return properties;
    }

    // The driver URL has no scheme, so TLS must be configured explicitly for HTTP URLs
    private static Optional<Boolean> isSecureScheme(URI uri)
    {
        switch (uri.getScheme().toLowerCase(ENGLISH)) {
            case "https":
                return Optional.of(true);
            case "http":
                return Optional.of(false);
        }
        return Optional.empty();
    }

    private static String toMapProperty(Map<String, String> values)
    {
        return values.entrySet().stream()
                .map(entry -> entry.getKey() + ":" + entry.getValue())
                .collect(joining(";"));
    }

    private static Optional<String> defaultKerberosCredentialCachePath()
    {
        String value = nullToEmpty(System.getenv("KRB5CCNAME"));
        if (value.startsWith("FILE:")) {
            value = value.substring("FILE:".length());
        }
        return Optional.ofNullable(emptyToNull(value));
    }

    private String getClientSecret()
    {
        checkState(oauth2ClientId.isPresent() && !oauth2ClientId.get().isEmpty(), "Both client ID and client secret must be specified");
        String defaultSecret = System.getenv("TRINO_OAUTH2_CLIENT_SECRET");
        if (defaultSecret != null) {
            return defaultSecret;
        }

        Console console = System.console();
        if (console != null) {
            char[] secret = console.readPassword("OAuth2 client secret: ");
            if (secret == null) {
                throw new IllegalArgumentException("OAuth2 client secret not provided");
            }
            return new String(secret);
        }

        LineReader reader = LineReaderBuilder.builder().terminal(getTerminal()).build();
        return reader.readLine("OAuth2 client secret: ", (char) 0);
    }

    private String getPassword()
    {
        checkState(user.isPresent() && !user.get().isEmpty(), "Both username and password must be specified");
        String defaultPassword = System.getenv("TRINO_PASSWORD");
        if (defaultPassword != null) {
            return defaultPassword;
        }

        Console console = System.console();
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
        if (lowerServer.startsWith(TRINO_JDBC_URL_PREFIX)) {
            return URI.create(server.substring(JDBC_URL_PREFIX.length()));
        }
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

    public static Map<String, String> toExtraHeaders(List<ExtraHeader> extraHeaders)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (ExtraHeader extraHeader : extraHeaders) {
            builder.put(extraHeader.getHeader(), extraHeader.getValue());
        }
        return builder.buildOrThrow();
    }

    private static Map<String, String> toProperties(List<ClientSessionProperty> sessionProperties)
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

    private static class ClientTagsConverter
            implements CommandLine.ITypeConverter<Set<String>>
    {
        @Override
        public Set<String> convert(String clientTagsString)
        {
            Splitter splitter = Splitter.on(',')
                    .trimResults()
                    .omitEmptyStrings();
            return ImmutableSet.copyOf(splitter.split(nullToEmpty(clientTagsString)));
        }
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

    public static final class ExtraHeader
    {
        private final String header;
        private final String value;

        public ExtraHeader(String headerAndValue)
        {
            List<String> nameValue = NAME_VALUE_SPLITTER.splitToList(headerAndValue);
            checkArgument(nameValue.size() == 2, "Header and value: %s", headerAndValue);
            this.header = nameValue.get(0);
            this.value = nameValue.get(1);

            checkArgument(!header.isEmpty(), "Header name is empty");
            checkArgument(!value.isEmpty(), "Header value is empty");
        }

        public ExtraHeader(String header, String value)
        {
            this.header = header;
            this.value = value;
        }

        public String getHeader()
        {
            return header;
        }

        public String getValue()
        {
            return value;
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
            ExtraHeader other = (ExtraHeader) o;
            return Objects.equals(header, other.header) && Objects.equals(value, other.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(header, value);
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
            checkArgument(catalog.isEmpty() || !catalog.get().isEmpty(), "Invalid session property: %s.%s:%s", catalog, name, value);
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
            return catalog.map(catalogName -> catalogName + '.').orElse("") + name + '=' + value;
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
