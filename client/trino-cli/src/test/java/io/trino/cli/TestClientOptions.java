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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.cli.ClientOptions.ClientResourceEstimate;
import io.trino.cli.ClientOptions.ClientSessionProperty;
import io.trino.cli.ClientOptions.OutputFormat;
import io.trino.client.ClientSession;
import io.trino.client.uri.TrinoUri;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.cli.Trino.createCommandLine;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestClientOptions
{
    @Test
    public void testDefaults()
    {
        Console console = createConsole();
        ClientOptions options = console.clientOptions;
        assertThat(options.krb5ServicePrincipalPattern).isEqualTo(Optional.of("${SERVICE}@${HOST}"));
        ClientSession session = toClientSession(options);
        assertThat(session.getServer().toString()).isEqualTo("http://localhost:8080");
        assertThat(session.getSource()).isEqualTo("trino-cli");
        assertThat(session.getTimeZone()).isEqualTo(ZoneId.systemDefault());
    }

    @Test
    public void testSource()
    {
        Console console = createConsole("--source=test");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getSource()).isEqualTo("test");
    }

    @Test
    public void testTraceToken()
    {
        Console console = createConsole("--trace-token", "test token");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getTraceToken()).isEqualTo(Optional.of("test token"));
    }

    @Test
    public void testServerHostOnly()
    {
        Console console = createConsole("--server=test");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
    }

    @Test
    public void testServerHostPort()
    {
        Console console = createConsole("--server=test:8888");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://test:8888");
    }

    @Test
    public void testServerHttpUri()
    {
        Console console = createConsole("--server=http://test/foo");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("foo"));
    }

    @Test
    public void testServerTrinoUri()
    {
        Console console = createConsole("--server=trino://test/foo");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("foo"));
    }

    @Test
    public void testServerHttpsUri()
    {
        Console console = createConsole("--server=https://test/foo");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("https://test:443");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("foo"));
    }

    @Test
    public void testServer443Port()
    {
        Console console = createConsole("--server=test:443");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("https://test:443");
    }

    @Test
    public void testServerHttpsHostPort()
    {
        Console console = createConsole("--server=https://test:443");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("https://test:443");
    }

    @Test
    public void testServerHttpWithPort443()
    {
        Console console = createConsole("--server=http://test:443");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://test:443");
    }

    @Test
    public void testInvalidServer()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("--server=x:y");
            toClientSession(console.clientOptions);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unparseable port number: x:y");
    }

    @Test
    public void testServerAndURL()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("--server=trino://server.example:80", "trino://server.example:80");
            toClientSession(console.clientOptions);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Using both the URL parameter and the --server option is not allowed");
    }

    @Test
    public void testPath()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("--path=name.name.name");
            toClientSession(console.clientOptions);
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Connection property 'path' has invalid syntax, should be [catalog].[schema] or [schema]");

        Console console = createConsole("--path=catalog.schema");
        assertThat(toTrinoUri(console.clientOptions).getPath()).hasValue(ImmutableList.of("catalog.schema"));
        assertThat(toClientSession(console.clientOptions).getPath()).isEqualTo(ImmutableList.of("catalog.schema"));
    }

    @Test
    public void testURLHostOnly()
    {
        Console console = createConsole("test");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
    }

    @Test
    public void testURLParams()
    {
        Console console = createConsole("trino://server.example:8080/my-catalog/my-schema?source=my-client");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://server.example:8080");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("my-catalog"));
        assertThat(session.getSchema()).isEqualTo(Optional.of("my-schema"));
        assertThat(session.getSource()).isEqualTo("my-client");
    }

    @Test
    public void testTrinoJdbcUrl()
    {
        Console console = createConsole("jdbc:trino://server.example:8080/my-catalog/my-schema?source=my-client");
        ClientSession session = toClientSession(console.clientOptions);
        assertThat(session.getServer().toString()).isEqualTo("http://server.example:8080");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("my-catalog"));
        assertThat(session.getSchema()).isEqualTo(Optional.of("my-schema"));
        assertThat(session.getSource()).isEqualTo("my-client");
        assertThat(console.clientOptions.createConnectionFactory(ImmutableMap.of())).isInstanceOf(TrinoConnectionFactory.class);
    }

    @Test
    public void testNonTrinoJdbcUrl()
    {
        Console console = createConsole("--catalog=test", "jdbc:h2:mem:test");
        assertThat(console.clientOptions.createConnectionFactory(ImmutableMap.of("catalog", "--catalog")))
                .isInstanceOf(DriverConnectionFactory.class);

        assertThatThrownBy(() -> createConsole("--session=a=b", "jdbc:h2:mem:test").clientOptions.createConnectionFactory(ImmutableMap.of("sessionProperties", "--session")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Option '--session' is only supported when connecting to Trino");

        assertThatThrownBy(() -> createConsole("--server=jdbc:h2:mem:test", "jdbc:h2:mem:test").clientOptions.createConnectionFactory(ImmutableMap.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Using both the URL parameter and the --server option is not allowed");
    }

    @Test
    public void testExplicitConnectionProperties()
    {
        ClientSession session = toClientSession(createConsole("--connection-property", "source=other", "--connection-property", "clientInfo=info").clientOptions);
        assertThat(session.getSource()).isEqualTo("other");
        assertThat(session.getClientInfo()).isEqualTo("info");

        ClientOptions options = createConsole("--source=mine", "--connection-property", "source=other").clientOptions;
        assertThatThrownBy(() -> options.toTrinoConnectionProperties(ImmutableMap.of("source", "--source")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Connection property 'source' cannot be set with the --connection-property option when option '--source' is set");

        assertThatThrownBy(() -> createConsole("--connection-property", "password=secret").clientOptions.toTrinoConnectionProperties(ImmutableMap.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Connection property 'password' cannot be set with the --connection-property option");
    }

    @Test
    public void testDriverOptions()
    {
        assertThat(createConsole("--driver-class=org.h2.Driver", "jdbc:h2:mem:test").clientOptions.createConnectionFactory(ImmutableMap.of()))
                .isInstanceOf(DriverConnectionFactory.class);

        assertThatThrownBy(() -> createConsole("--driver-class=org.example.Missing", "jdbc:h2:mem:test").clientOptions.createConnectionFactory(ImmutableMap.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Cannot load JDBC driver class org.example.Missing");

        assertThatThrownBy(() -> createConsole("--driver-path=/no/such/driver.jar", "jdbc:h2:mem:test").clientOptions.createConnectionFactory(ImmutableMap.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Driver path does not exist: /no/such/driver.jar");

        assertThatThrownBy(() -> createConsole("jdbc:unknown://localhost").clientOptions.createConnectionFactory(ImmutableMap.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No JDBC driver found for URL jdbc:unknown://localhost, use the --driver-path option to load it");

        assertThatThrownBy(() -> createConsole("--driver-class=org.h2.Driver", "trino://localhost:8080").clientOptions.createConnectionFactory(ImmutableMap.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Options '--driver-path' and '--driver-class' can only be used with JDBC URLs of databases other than Trino");
    }

    @Test
    public void testConnectionProperties()
    {
        ClientOptions options = createConsole("--server=http://test:443", "--external-authentication").clientOptions;
        Properties properties = options.toTrinoConnectionProperties(ImmutableMap.of());
        assertThat(properties.getProperty("SSL")).isEqualTo("false");
        assertThat(properties.getProperty("source")).isEqualTo("trino-cli");
        assertThat(properties.getProperty("externalAuthenticationTokenCache")).isEqualTo("MEMORY");
    }

    @Test
    public void testURLPropertiesSetByOptions()
    {
        ClientOptions options = createConsole("--catalog=other", "trino://server.example:8080/my-catalog").clientOptions;
        assertThatThrownBy(() -> options.toTrinoJdbcUrl(ImmutableMap.of("catalog", "--catalog")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Connection property 'catalog' cannot be set in the URL when option '--catalog' is set");

        ClientOptions sourceOptions = createConsole("--source=other", "trino://server.example:8080?source=my-client").clientOptions;
        assertThatThrownBy(() -> sourceOptions.toTrinoJdbcUrl(ImmutableMap.of("source", "--source")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Connection property 'source' cannot be set in the URL when option '--source' is set");
    }

    @Test
    public void testURLPassword()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("trino://server.example:80?password=invalid");
            toClientSession(console.clientOptions);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Setting the password in the URL parameter is not allowed.*");
    }

    @Test
    public void testOutputFormat()
    {
        Console console = createConsole("--output-format=JSON");
        ClientOptions options = console.clientOptions;
        assertThat(options.outputFormat).isEqualTo(OutputFormat.JSON);
    }

    @Test
    public void testSocksProxy()
    {
        Console console = createConsole("--socks-proxy=abc:123");
        ClientOptions options = console.clientOptions;
        assertThat(options.socksProxy).isEqualTo(Optional.of(HostAndPort.fromParts("abc", 123)));
    }

    @Test
    public void testClientRequestTimeout()
    {
        Console console = createConsole("--client-request-timeout=7s");
        ClientOptions options = console.clientOptions;
        assertThat(options.clientRequestTimeout).isEqualTo(new Duration(7, SECONDS));
    }

    @Test
    public void testResourceEstimates()
    {
        Console console = createConsole("--resource-estimate", "resource1=1B", "--resource-estimate", "resource2=2.2h");
        ClientOptions options = console.clientOptions;
        assertThat(options.resourceEstimates).isEqualTo(ImmutableList.of(
                new ClientResourceEstimate("resource1", "1B"),
                new ClientResourceEstimate("resource2", "2.2h")));
        assertThat(toClientSession(options).getResourceEstimates()).isEqualTo(ImmutableMap.of("resource1", "1B", "resource2", "2.2h"));
    }

    @Test
    public void testExtraCredentials()
    {
        Console console = createConsole("--extra-credential", "test.token.foo=foo", "--extra-credential", "test.token.bar=bar");
        ClientOptions options = console.clientOptions;
        assertThat(options.extraCredentials).isEqualTo(ImmutableList.of(
                new ClientOptions.ClientExtraCredential("test.token.foo", "foo"),
                new ClientOptions.ClientExtraCredential("test.token.bar", "bar")));
    }

    @Test
    public void testExtraHeaders()
    {
        Console console = createConsole("--extra-header", "X-Trino-Routing-Group=foo", "--extra-header", "x-foo=bar");
        ClientOptions options = console.clientOptions;
        assertThat(options.extraHeaders).isEqualTo(ImmutableList.of(
                new ClientOptions.ExtraHeader("X-Trino-Routing-Group", "foo"),
                new ClientOptions.ExtraHeader("x-foo", "bar")));

        assertThatThrownBy(() -> toClientSession(createConsole("--extra-header", "X-Trino-User=Forbidden").clientOptions))
                .hasMessageContaining("Connection property extraHeaders cannot override any of the Trino protocol headers");
    }

    @Test
    public void testSessionProperties()
    {
        Console console = createConsole("--session", "system=system-value", "--session", "catalog.name=catalog-property");

        ClientOptions options = console.clientOptions;
        assertThat(options.sessionProperties).isEqualTo(ImmutableList.of(
                new ClientSessionProperty(Optional.empty(), "system", "system-value"),
                new ClientSessionProperty(Optional.of("catalog"), "name", "catalog-property")));

        // special characters are allowed in the value
        assertThat(new ClientSessionProperty("foo=bar:=baz")).isEqualTo(new ClientSessionProperty(Optional.empty(), "foo", "bar:=baz"));

        // empty values are allowed
        assertThat(new ClientSessionProperty("foo=")).isEqualTo(new ClientSessionProperty(Optional.empty(), "foo", ""));
    }

    @Test
    public void testTimeZone()
    {
        Console console = createConsole("--timezone=Europe/Vilnius");

        ClientOptions options = console.clientOptions;
        assertThat(options.timeZone).isEqualTo(ZoneId.of("Europe/Vilnius"));

        ClientSession session = toClientSession(options);
        assertThat(session.getTimeZone()).isEqualTo(ZoneId.of("Europe/Vilnius"));
    }

    @Test
    public void testTimeout()
    {
        Console console = createConsole("--client-request-timeout=17s");

        ClientOptions options = console.clientOptions;
        assertThat(options.clientRequestTimeout).isEqualTo(Duration.succinctDuration(17, SECONDS));

        ClientSession session = toClientSession(options);
        assertThat(session.getClientRequestTimeout()).isEqualTo(Duration.succinctDuration(17, SECONDS));

        assertThatThrownBy(() -> toClientSession(createConsole("--client-request-timeout=17s", "trino://localhost:8080?timeout=30s").clientOptions))
                .hasMessageContaining("Connection property timeout is passed both by URL and properties");
    }

    @Test
    public void testDisableCompression()
    {
        Console console = createConsole("--disable-compression");

        ClientOptions options = console.clientOptions;
        assertThat(options.disableCompression).isTrue();

        ClientSession session = toClientSession(options);
        assertThat(session.isCompressionDisabled()).isTrue();
    }

    @Test
    public void testThreePartPropertyName()
    {
        assertThatThrownBy(() -> new ClientSessionProperty("foo.bar.baz=value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid session property: foo.bar.baz=value");
    }

    @Test
    public void testEmptyPropertyName()
    {
        assertThatThrownBy(() -> new ClientSessionProperty("=value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Session property name is empty");
    }

    @Test
    public void testInvalidCharsetPropertyName()
    {
        assertThatThrownBy(() -> new ClientSessionProperty("\u2603=value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Session property name contains spaces or is not ASCII: ☃");
    }

    @Test
    public void testInvalidCharsetPropertyValue()
    {
        assertThatThrownBy(() -> new ClientSessionProperty("name=\u2603"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Session property value contains spaces or is not ASCII: ☃");
    }

    @Test
    public void testEqualSignNoAllowedInPropertyCatalog()
    {
        assertThatThrownBy(() -> new ClientSessionProperty(Optional.of("cat=alog"), "name", "value"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Session property catalog must not contain '=': name");
    }

    @Test
    public void testDuplicateExtraCredentialKey()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("--extra-credential", "test.token.foo=foo", "--extra-credential", "test.token.foo=bar");
            toClientSession(console.clientOptions);
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Multiple entries with same key: test.token.foo=bar and test.token.foo=foo");
    }

    @Test
    public void testAllClientOptionsHaveMappingToAConnectionProperty()
    {
        Set<String> fieldsWithoutMapping = Arrays.stream(ClientOptions.class.getDeclaredFields())
                .filter(field -> Modifier.isPublic(field.getModifiers()))
                .filter(field -> field.getAnnotation(ClientOptions.PropertyMapping.class) == null)
                .map(Field::getName)
                .filter(value -> !isCliSpecificOptions(value))
                .collect(toImmutableSet());

        assertThat(fieldsWithoutMapping).isEmpty();
    }

    private boolean isCliSpecificOptions(String name)
    {
        switch (name) {
            case "url":
            case "server":
            case "file":
            case "debug":
            case "historyFile":
            case "progress":
            case "execute":
            case "outputFormat":
            case "outputFormatInteractive":
            case "pager":
            case "ignoreErrors":
            case "editingMode":
            case "disableAutoSuggestion":
            case "decimalDataSize":
            case "theme":
            case "maxBufferedRows":
            case "maxQueuedRows":
            case "driverPaths":
            case "driverClass":
            case "connectionProperties":
                return true;
        }

        return false;
    }

    private static ClientSession toClientSession(ClientOptions options)
    {
        return toTrinoUri(options)
                .toClientSessionBuilder()
                .build();
    }

    // interprets the driver URL and properties the same way the Trino JDBC driver does
    private static TrinoUri toTrinoUri(ClientOptions options)
    {
        String url = options.toTrinoJdbcUrl(ImmutableMap.of());
        return TrinoUri.create(url.substring("jdbc:".length()), options.toTrinoConnectionProperties(ImmutableMap.of()));
    }

    private static Console createConsole(String... args)
    {
        Console console = new Console();
        createCommandLine(console).setDefaultValueProvider(null).parseArgs(args);
        return console;
    }
}
