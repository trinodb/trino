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
        ClientSession session = options.toClientSession(options.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("http://localhost:8080");
        assertThat(session.getSource()).isEqualTo("trino-cli");
        assertThat(session.getTimeZone()).isEqualTo(ZoneId.systemDefault());
    }

    @Test
    public void testSource()
    {
        Console console = createConsole("--source=test");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getSource()).isEqualTo("test");
    }

    @Test
    public void testTraceToken()
    {
        Console console = createConsole("--trace-token", "test token");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getTraceToken()).isEqualTo(Optional.of("test token"));
    }

    @Test
    public void testServerHostOnly()
    {
        Console console = createConsole("--server=test");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
    }

    @Test
    public void testServerHostPort()
    {
        Console console = createConsole("--server=test:8888");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("http://test:8888");
    }

    @Test
    public void testServerHttpUri()
    {
        Console console = createConsole("--server=http://test/foo");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("foo"));
    }

    @Test
    public void testServerTrinoUri()
    {
        Console console = createConsole("--server=trino://test/foo");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("foo"));
    }

    @Test
    public void testServerHttpsUri()
    {
        Console console = createConsole("--server=https://test/foo");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("https://test:443");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("foo"));
    }

    @Test
    public void testServer443Port()
    {
        Console console = createConsole("--server=test:443");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("https://test:443");
    }

    @Test
    public void testServerHttpsHostPort()
    {
        Console console = createConsole("--server=https://test:443");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("https://test:443");
    }

    @Test
    public void testServerHttpWithPort443()
    {
        Console console = createConsole("--server=http://test:443");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("http://test:443");
    }

    @Test
    public void testInvalidServer()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("--server=x:y");
            console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unparseable port number: x:y");
    }

    @Test
    public void testServerAndURL()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("--server=trino://server.example:80", "trino://server.example:80");
            console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Using both the URL parameter and the --server option is not allowed");
    }

    @Test
    public void testPath()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("--path=name.name.name");
            console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        })
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Connection property 'path' has invalid syntax, should be [catalog].[schema] or [schema]");

        Console console = createConsole("--path=catalog.schema");
        TrinoUri trinoUri = console.clientOptions.getTrinoUri();
        assertThat(trinoUri.getPath()).hasValue(ImmutableList.of("catalog.schema"));
        assertThat(console.clientOptions.toClientSession(trinoUri).getPath()).isEqualTo(ImmutableList.of("catalog.schema"));
    }

    @Test
    public void testURLHostOnly()
    {
        Console console = createConsole("test");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertThat(session.getServer().toString()).isEqualTo("http://test:80");
    }

    @Test
    public void testURLParams()
    {
        Console console = createConsole("trino://server.example:8080/my-catalog/my-schema?source=my-client");
        TrinoUri uri = console.clientOptions.getTrinoUri();
        ClientSession session = console.clientOptions.toClientSession(uri);
        assertThat(session.getServer().toString()).isEqualTo("http://server.example:8080");
        assertThat(session.getCatalog()).isEqualTo(Optional.of("my-catalog"));
        assertThat(session.getSchema()).isEqualTo(Optional.of("my-schema"));
        assertThat(uri.getSource()).isEqualTo(Optional.of("my-client"));
    }

    @Test
    public void testURLPassword()
    {
        assertThatThrownBy(() -> {
            Console console = createConsole("trino://server.example:80?password=invalid");
            console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
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

        ClientSession session = options.toClientSession(options.getTrinoUri());
        assertThat(session.getTimeZone()).isEqualTo(ZoneId.of("Europe/Vilnius"));
    }

    @Test
    public void testTimeout()
    {
        Console console = createConsole("--client-request-timeout=17s");

        ClientOptions options = console.clientOptions;
        assertThat(options.clientRequestTimeout).isEqualTo(Duration.succinctDuration(17, SECONDS));

        ClientSession session = options.toClientSession(options.getTrinoUri());
        assertThat(session.getClientRequestTimeout()).isEqualTo(Duration.succinctDuration(17, SECONDS));

        assertThatThrownBy(() -> createConsole("--client-request-timeout=17s", "trino://localhost:8080?timeout=30s").clientOptions.getTrinoUri())
                .hasMessageContaining("Connection property timeout is passed both by URL and properties");
    }

    @Test
    public void testDisableCompression()
    {
        Console console = createConsole("--disable-compression");

        ClientOptions options = console.clientOptions;
        assertThat(options.disableCompression).isTrue();

        ClientSession session = options.toClientSession(options.getTrinoUri());
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
            console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
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
                return true;
        }

        return false;
    }

    private static Console createConsole(String... args)
    {
        Console console = new Console();
        createCommandLine(console).setDefaultValueProvider(null).parseArgs(args);
        return console;
    }
}
