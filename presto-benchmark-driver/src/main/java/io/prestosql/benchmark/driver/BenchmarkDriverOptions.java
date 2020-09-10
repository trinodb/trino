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
package io.prestosql.benchmark.driver;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.prestosql.client.ClientSession;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.CharsetEncoder;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.Option;

public class BenchmarkDriverOptions
{
    private static final Splitter NAME_VALUE_SPLITTER = Splitter.on('=').limit(2);
    private static final CharMatcher PRINTABLE_ASCII = CharMatcher.inRange((char) 0x21, (char) 0x7E);

    private static final String DEFAULT_VALUE = "(default: ${DEFAULT-VALUE})";

    @Option(names = "--server", paramLabel = "<server>", defaultValue = "localhost:8080", description = "Presto server location " + DEFAULT_VALUE)
    public String server;

    @Option(names = "--user", paramLabel = "<user>", description = "Username " + DEFAULT_VALUE)
    public String user = System.getProperty("user.name");

    @Option(names = "--catalog", paramLabel = "<catalog>", description = "Default catalog")
    public String catalog;

    @Option(names = "--schema", paramLabel = "<schema>", description = "Default schema")
    public String schema;

    @Option(names = "--suite", paramLabel = "<suite>", description = "Suite to execute")
    public List<String> suites = new ArrayList<>();

    @Option(names = "--suite-config", paramLabel = "<file>", defaultValue = "suite.json", description = "Suites configuration file " + DEFAULT_VALUE)
    public String suiteConfigFile;

    @Option(names = "--sql", paramLabel = "<path>", defaultValue = "sql", description = "Directory containing sql files " + DEFAULT_VALUE)
    public String sqlTemplateDir;

    @Option(names = "--query", paramLabel = "<query>", description = "Queries to execute")
    public List<String> queries = new ArrayList<>();

    @Option(names = "--debug", description = "Enable debug information")
    public boolean debug;

    @Option(names = "--session", paramLabel = "<session>", description = "Session property (property can be used multiple times; format is key=value)")
    public final List<ClientSessionProperty> sessionProperties = new ArrayList<>();

    @Option(names = "--extra-credential", paramLabel = "<credential>", description = "Extra credentials (property can be used multiple times; format is key=value)")
    public final List<ClientExtraCredential> extraCredentials = new ArrayList<>();

    @Option(names = "--runs", paramLabel = "<runs>", defaultValue = "3", description = "Number of times to run each query " + DEFAULT_VALUE)
    public int runs;

    @Option(names = "--warm", paramLabel = "<warm>", defaultValue = "1", description = "Number of times to run each query for a warm-up " + DEFAULT_VALUE)
    public int warm;

    @Option(names = "--max-failures", paramLabel = "<count>", defaultValue = "10", description = "Max number of consecutive failures before benchmark fails " + DEFAULT_VALUE)
    public int maxFailures;

    @Option(names = "--socks", paramLabel = "<proxy>", description = "Socks proxy to use")
    public HostAndPort socksProxy;

    @Option(names = "--client-request-timeout", paramLabel = "<timeout>", defaultValue = "2m", description = "Client request timeout " + DEFAULT_VALUE)
    public Duration clientRequestTimeout;

    public ClientSession getClientSession()
    {
        return new ClientSession(
                parseServer(server),
                user,
                "presto-benchmark",
                Optional.empty(),
                ImmutableSet.of(),
                null,
                catalog,
                schema,
                null,
                ZoneId.systemDefault(),
                false,
                Locale.getDefault(),
                ImmutableMap.of(),
                toProperties(this.sessionProperties),
                ImmutableMap.of(),
                ImmutableMap.of(),
                extraCredentials.stream()
                        .collect(toImmutableMap(ClientExtraCredential::getName, ClientExtraCredential::getValue)),
                null,
                clientRequestTimeout);
    }

    private static URI parseServer(String server)
    {
        server = server.toLowerCase(ENGLISH);
        if (server.startsWith("http://") || server.startsWith("https://")) {
            return URI.create(server);
        }

        HostAndPort host = HostAndPort.fromString(server);
        try {
            return new URI("http", null, host.getHost(), host.getPortOrDefault(80), null, null, null);
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
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
        return builder.build();
    }

    public static final class ClientSessionProperty
    {
        private static final Splitter NAME_VALUE_SPLITTER = Splitter.on('=').limit(2);
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

            CharsetEncoder charsetEncoder = US_ASCII.newEncoder();
            checkArgument(catalog.orElse("").indexOf('=') < 0, "Session property catalog must not contain '=': %s", name);
            checkArgument(charsetEncoder.canEncode(catalog.orElse("")), "Session property catalog is not US_ASCII: %s", name);
            checkArgument(name.indexOf('=') < 0, "Session property name must not contain '=': %s", name);
            checkArgument(charsetEncoder.canEncode(name), "Session property name is not US_ASCII: %s", name);
            checkArgument(charsetEncoder.canEncode(value), "Session property value is not US_ASCII: %s", value);
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
            checkArgument(PRINTABLE_ASCII.matchesAllOf(name), "Credential name contains spaces or is not US_ASCII: %s", name);
            checkArgument(name.indexOf('=') < 0, "Credential name must not contain '=': %s", name);
            checkArgument(PRINTABLE_ASCII.matchesAllOf(value), "Credential value contains space or is not US_ASCII: %s", name);
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
