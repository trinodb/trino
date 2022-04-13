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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.client.ClientSession;
import io.trino.client.auth.external.ExternalRedirectStrategy;
import okhttp3.logging.HttpLoggingInterceptor;
import org.jline.reader.LineReader;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.client.KerberosUtil.defaultCredentialCachePath;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.Option;

public class ClientOptions
{
    private static final Splitter NAME_VALUE_SPLITTER = Splitter.on('=').limit(2);
    private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E); // spaces are not allowed
    private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

    @Option(names = "--server", paramLabel = "<server>", defaultValue = "localhost:8080", description = "Trino server location " + DEFAULT_VALUE)
    public String server;

    @Option(names = "--krb5-service-principal-pattern", paramLabel = "<pattern>", defaultValue = "$${SERVICE}@$${HOST}", description = "Remote kerberos service principal pattern " + DEFAULT_VALUE)
    public Optional<String> krb5ServicePrincipalPattern;

    @Option(names = "--krb5-remote-service-name", paramLabel = "<name>", description = "Remote peer's kerberos service name")
    public Optional<String> krb5RemoteServiceName;

    @Option(names = "--krb5-config-path", paramLabel = "<path>", defaultValue = "/etc/krb5.conf", description = "Kerberos config file path " + DEFAULT_VALUE)
    public Optional<String> krb5ConfigPath;

    @Option(names = "--krb5-keytab-path", paramLabel = "<path>", defaultValue = "/etc/krb5.keytab", description = "Kerberos key table path " + DEFAULT_VALUE)
    public Optional<String> krb5KeytabPath;

    @Option(names = "--krb5-credential-cache-path", paramLabel = "<path>", description = "Kerberos credential cache path")
    public Optional<String> krb5CredentialCachePath = defaultCredentialCachePath();

    @Option(names = "--krb5-principal", paramLabel = "<principal>", description = "Kerberos principal to be used")
    public Optional<String> krb5Principal;

    @Option(names = "--krb5-disable-remote-service-hostname-canonicalization", description = "Disable service hostname canonicalization using the DNS reverse lookup")
    public boolean krb5DisableRemoteServiceHostnameCanonicalization;

    @Option(names = "--keystore-path", paramLabel = "<path>", description = "Keystore path")
    public Optional<String> keystorePath;

    @Option(names = "--keystore-password", paramLabel = "<password>", description = "Keystore password")
    public Optional<String> keystorePassword;

    @Option(names = "--keystore-type", paramLabel = "<type>", description = "Keystore type")
    public Optional<String> keystoreType;

    @Option(names = "--truststore-path", paramLabel = "<path>", description = "Truststore path")
    public Optional<String> truststorePath;

    @Option(names = "--truststore-password", paramLabel = "<password>", description = "Truststore password")
    public Optional<String> truststorePassword;

    @Option(names = "--truststore-type", paramLabel = "<type>", description = "Truststore type")
    public Optional<String> truststoreType;

    @Option(names = "--use-system-truststore", description = "Use default system (OS) truststore")
    public boolean useSystemTruststore;

    @Option(names = "--insecure", description = "Skip validation of HTTP server certificates (should only be used for debugging)")
    public boolean insecure;

    @Option(names = "--access-token", paramLabel = "<token>", description = "Access token")
    public Optional<String> accessToken;

    @Option(names = "--user", paramLabel = "<user>", defaultValue = "${sys:user.name}", description = "Username " + DEFAULT_VALUE)
    public Optional<String> user;

    @Option(names = "--password", paramLabel = "<password>", description = "Prompt for password")
    public boolean password;

    @Option(names = "--external-authentication", paramLabel = "<externalAuthentication>", description = "Enable external authentication")
    public boolean externalAuthentication;

    @Option(names = "--external-authentication-redirect-handler", paramLabel = "<externalAuthenticationRedirectHandler>", description = "External authentication redirect handlers: ${COMPLETION-CANDIDATES} " + DEFAULT_VALUE, defaultValue = "ALL")
    public List<ExternalRedirectStrategy> externalAuthenticationRedirectHandler = new ArrayList<>();

    @Option(names = "--source", paramLabel = "<source>", defaultValue = "trino-cli", description = "Name of source making query " + DEFAULT_VALUE)
    public String source;

    @Option(names = "--client-info", paramLabel = "<info>", description = "Extra information about client making query")
    public String clientInfo;

    @Option(names = "--client-tags", paramLabel = "<tags>", description = "Client tags")
    public String clientTags;

    @Option(names = "--trace-token", paramLabel = "<token>", description = "Trace token")
    public String traceToken;

    @Option(names = "--catalog", paramLabel = "<catalog>", description = "Default catalog")
    public String catalog;

    @Option(names = "--schema", paramLabel = "<schema>", description = "Default schema")
    public String schema;

    @Option(names = {"-f", "--file"}, paramLabel = "<file>", description = "Execute statements from file and exit")
    public String file;

    @Option(names = "--debug", paramLabel = "<debug>", description = "Enable debug information")
    public boolean debug;

    @Option(names = "--network-logging", paramLabel = "<level>", defaultValue = "NONE", description = "Network logging level [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public HttpLoggingInterceptor.Level networkLogging;

    @Option(names = "--progress", paramLabel = "<progress>", description = "Show query progress in batch mode")
    public boolean progress;

    @Option(names = "--execute", paramLabel = "<execute>", description = "Execute specified statements and exit")
    public String execute;

    @Option(names = "--output-format", paramLabel = "<format>", defaultValue = "CSV", description = "Output format for batch mode [${COMPLETION-CANDIDATES}] " + DEFAULT_VALUE)
    public OutputFormat outputFormat;

    @Option(names = "--resource-estimate", paramLabel = "<estimate>", description = "Resource estimate (property can be used multiple times; format is key=value)")
    public final List<ClientResourceEstimate> resourceEstimates = new ArrayList<>();

    @Option(names = "--session", paramLabel = "<session>", description = "Session property (property can be used multiple times; format is key=value; use 'SHOW SESSION' to see available properties)")
    public final List<ClientSessionProperty> sessionProperties = new ArrayList<>();

    @Option(names = "--session-user", paramLabel = "<user>", description = "Username to impersonate")
    public Optional<String> sessionUser;

    @Option(names = "--extra-credential", paramLabel = "<credential>", description = "Extra credentials (property can be used multiple times; format is key=value)")
    public final List<ClientExtraCredential> extraCredentials = new ArrayList<>();

    @Option(names = "--socks-proxy", paramLabel = "<proxy>", description = "SOCKS proxy to use for server connections")
    public Optional<HostAndPort> socksProxy;

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

    public enum OutputFormat
    {
        ALIGNED,
        VERTICAL,
        TSV,
        TSV_HEADER,
        CSV,
        CSV_HEADER,
        CSV_UNQUOTED,
        CSV_HEADER_UNQUOTED,
        JSON,
        NULL
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

    public ClientSession toClientSession()
    {
        return new ClientSession(
                parseServer(server),
                user,
                sessionUser,
                source,
                Optional.ofNullable(traceToken),
                parseClientTags(nullToEmpty(clientTags)),
                clientInfo,
                catalog,
                schema,
                null,
                timeZone,
                Locale.getDefault(),
                toResourceEstimates(resourceEstimates),
                toProperties(sessionProperties),
                emptyMap(),
                emptyMap(),
                toExtraCredentials(extraCredentials),
                null,
                clientRequestTimeout,
                disableCompression);
    }

    public static URI parseServer(String server)
    {
        server = server.toLowerCase(ENGLISH);
        if (server.startsWith("http://") || server.startsWith("https://")) {
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
