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
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.Optional;

import static io.trino.cli.Trino.createCommandLine;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestClientOptions
{
    @Test
    public void testDefaults()
    {
        Console console = createConsole();
        ClientOptions options = console.clientOptions;
        assertEquals(options.krb5ServicePrincipalPattern, Optional.of("${SERVICE}@${HOST}"));
        ClientSession session = options.toClientSession(options.getTrinoUri());
        assertEquals(session.getServer().toString(), "http://localhost:8080");
        assertEquals(session.getSource(), "trino-cli");
        assertEquals(session.getTimeZone(), ZoneId.systemDefault());
    }

    @Test
    public void testSource()
    {
        Console console = createConsole("--source=test");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getSource(), "test");
    }

    @Test
    public void testTraceToken()
    {
        Console console = createConsole("--trace-token", "test token");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getTraceToken(), Optional.of("test token"));
    }

    @Test
    public void testServerHostOnly()
    {
        Console console = createConsole("--server=test");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "http://test:80");
    }

    @Test
    public void testServerHostPort()
    {
        Console console = createConsole("--server=test:8888");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "http://test:8888");
    }

    @Test
    public void testServerHttpUri()
    {
        Console console = createConsole("--server=http://test/foo");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "http://test:80");
        assertEquals(session.getCatalog(), Optional.of("foo"));
    }

    @Test
    public void testServerTrinoUri()
    {
        Console console = createConsole("--server=trino://test/foo");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "http://test:80");
        assertEquals(session.getCatalog(), Optional.of("foo"));
    }

    @Test
    public void testServerHttpsUri()
    {
        Console console = createConsole("--server=https://test/foo");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "https://test:443");
        assertEquals(session.getCatalog(), Optional.of("foo"));
    }

    @Test
    public void testServer443Port()
    {
        Console console = createConsole("--server=test:443");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "https://test:443");
    }

    @Test
    public void testServerHttpsHostPort()
    {
        Console console = createConsole("--server=https://test:443");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "https://test:443");
    }

    @Test
    public void testServerHttpWithPort443()
    {
        Console console = createConsole("--server=http://test:443");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "http://test:443");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unparseable port number: x:y")
    public void testInvalidServer()
    {
        Console console = createConsole("--server=x:y");
        console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Using both the URL parameter and the --server option is not allowed")
    public void testServerAndURL()
    {
        Console console = createConsole("--server=trino://server.example:80", "trino://server.example:80");
        console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
    }

    @Test
    public void testURLHostOnly()
    {
        Console console = createConsole("test");
        ClientSession session = console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
        assertEquals(session.getServer().toString(), "http://test:80");
    }

    @Test
    public void testURLParams()
            throws SQLException
    {
        Console console = createConsole("trino://server.example:8080/my-catalog/my-schema?source=my-client");
        TrinoUri uri = console.clientOptions.getTrinoUri();
        ClientSession session = console.clientOptions.toClientSession(uri);
        assertEquals(session.getServer().toString(), "http://server.example:8080");
        assertEquals(session.getCatalog(), Optional.of("my-catalog"));
        assertEquals(session.getSchema(), Optional.of("my-schema"));
        assertEquals(uri.getSource(), Optional.of("my-client"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Setting the password in the URL parameter is not allowed.*")
    public void testURLPassword()
    {
        Console console = createConsole("trino://server.example:80?password=invalid");
        console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
    }

    @Test
    public void testOutputFormat()
    {
        Console console = createConsole("--output-format=JSON");
        ClientOptions options = console.clientOptions;
        assertEquals(options.outputFormat, OutputFormat.JSON);
    }

    @Test
    public void testSocksProxy()
    {
        Console console = createConsole("--socks-proxy=abc:123");
        ClientOptions options = console.clientOptions;
        assertEquals(options.socksProxy, Optional.of(HostAndPort.fromParts("abc", 123)));
    }

    @Test
    public void testClientRequestTimeout()
    {
        Console console = createConsole("--client-request-timeout=7s");
        ClientOptions options = console.clientOptions;
        assertEquals(options.clientRequestTimeout, new Duration(7, SECONDS));
    }

    @Test
    public void testResourceEstimates()
    {
        Console console = createConsole("--resource-estimate", "resource1=1B", "--resource-estimate", "resource2=2.2h");
        ClientOptions options = console.clientOptions;
        assertEquals(options.resourceEstimates, ImmutableList.of(
                new ClientResourceEstimate("resource1", "1B"),
                new ClientResourceEstimate("resource2", "2.2h")));
    }

    @Test
    public void testExtraCredentials()
    {
        Console console = createConsole("--extra-credential", "test.token.foo=foo", "--extra-credential", "test.token.bar=bar");
        ClientOptions options = console.clientOptions;
        assertEquals(options.extraCredentials, ImmutableList.of(
                new ClientOptions.ClientExtraCredential("test.token.foo", "foo"),
                new ClientOptions.ClientExtraCredential("test.token.bar", "bar")));
    }

    @Test
    public void testSessionProperties()
    {
        Console console = createConsole("--session", "system=system-value", "--session", "catalog.name=catalog-property");

        ClientOptions options = console.clientOptions;
        assertEquals(options.sessionProperties, ImmutableList.of(
                new ClientSessionProperty(Optional.empty(), "system", "system-value"),
                new ClientSessionProperty(Optional.of("catalog"), "name", "catalog-property")));

        // special characters are allowed in the value
        assertEquals(new ClientSessionProperty("foo=bar:=baz"), new ClientSessionProperty(Optional.empty(), "foo", "bar:=baz"));

        // empty values are allowed
        assertEquals(new ClientSessionProperty("foo="), new ClientSessionProperty(Optional.empty(), "foo", ""));
    }

    @Test
    public void testTimeZone()
    {
        Console console = createConsole("--timezone=Europe/Vilnius");

        ClientOptions options = console.clientOptions;
        assertEquals(options.timeZone, ZoneId.of("Europe/Vilnius"));

        ClientSession session = options.toClientSession(options.getTrinoUri());
        assertEquals(session.getTimeZone(), ZoneId.of("Europe/Vilnius"));
    }

    @Test
    public void testDisableCompression()
    {
        Console console = createConsole("--disable-compression");

        ClientOptions options = console.clientOptions;
        assertTrue(options.disableCompression);

        ClientSession session = options.toClientSession(options.getTrinoUri());
        assertTrue(session.isCompressionDisabled());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QInvalid session property: foo.bar.baz=value\\E")
    public void testThreePartPropertyName()
    {
        new ClientSessionProperty("foo.bar.baz=value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QSession property name is empty\\E")
    public void testEmptyPropertyName()
    {
        new ClientSessionProperty("=value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QSession property name contains spaces or is not ASCII: ☃\\E")
    public void testInvalidCharsetPropertyName()
    {
        new ClientSessionProperty("\u2603=value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QSession property value contains spaces or is not ASCII: ☃\\E")
    public void testInvalidCharsetPropertyValue()
    {
        new ClientSessionProperty("name=\u2603");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QSession property catalog must not contain '=': name\\E")
    public void testEqualSignNoAllowedInPropertyCatalog()
    {
        new ClientSessionProperty(Optional.of("cat=alog"), "name", "value");
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QMultiple entries with same key: test.token.foo=bar and test.token.foo=foo\\E")
    public void testDuplicateExtraCredentialKey()
    {
        Console console = createConsole("--extra-credential", "test.token.foo=foo", "--extra-credential", "test.token.foo=bar");
        console.clientOptions.toClientSession(console.clientOptions.getTrinoUri());
    }

    private static Console createConsole(String... args)
    {
        Console console = new Console();
        createCommandLine(console).setDefaultValueProvider(null).parseArgs(args);
        return console;
    }
}
