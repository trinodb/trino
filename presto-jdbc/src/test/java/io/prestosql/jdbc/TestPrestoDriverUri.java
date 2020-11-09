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
package io.prestosql.jdbc;

import org.testng.annotations.Test;

import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;

import static io.prestosql.jdbc.ConnectionProperties.CLIENT_TAGS;
import static io.prestosql.jdbc.ConnectionProperties.EXTRA_CREDENTIALS;
import static io.prestosql.jdbc.ConnectionProperties.HTTP_PROXY;
import static io.prestosql.jdbc.ConnectionProperties.SOCKS_PROXY;
import static io.prestosql.jdbc.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static io.prestosql.jdbc.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static io.prestosql.jdbc.ConnectionProperties.SSL_VERIFICATION;
import static io.prestosql.jdbc.ConnectionProperties.SslVerificationMode.CA;
import static io.prestosql.jdbc.ConnectionProperties.SslVerificationMode.FULL;
import static io.prestosql.jdbc.ConnectionProperties.SslVerificationMode.NONE;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class TestPrestoDriverUri
{
    @Test
    public void testInvalidUrls()
    {
        // missing port
        assertInvalid("jdbc:presto://localhost/", "No port number specified:");

        // extra path segments
        assertInvalid("jdbc:presto://localhost:8080/hive/default/abc", "Invalid path segments in URL:");

        // extra slash
        assertInvalid("jdbc:presto://localhost:8080//", "Catalog name is empty:");

        // has schema but is missing catalog
        assertInvalid("jdbc:presto://localhost:8080//default", "Catalog name is empty:");

        // has catalog but schema is missing
        assertInvalid("jdbc:presto://localhost:8080/a//", "Schema name is empty:");

        // unrecognized property
        assertInvalid("jdbc:presto://localhost:8080/hive/default?ShoeSize=13", "Unrecognized connection property 'ShoeSize'");

        // empty property
        assertInvalid("jdbc:presto://localhost:8080/hive/default?SSL=", "Connection property 'SSL' value is empty");

        // empty ssl verification property
        assertInvalid("jdbc:presto://localhost:8080/hive/default?SSL=true&SSLVerification=", "Connection property 'SSLVerification' value is empty");

        // property in url multiple times
        assertInvalid("presto://localhost:8080/blackhole?password=a&password=b", "Connection property 'password' is in URL multiple times");

        // property in both url and arguments
        assertInvalid("presto://localhost:8080/blackhole?user=test123", "Connection property 'user' is both in the URL and an argument");

        // setting both socks and http proxy
        assertInvalid("presto://localhost:8080?socksProxy=localhost:1080&httpProxy=localhost:8888", "Connection property 'socksProxy' is not allowed");
        assertInvalid("presto://localhost:8080?httpProxy=localhost:8888&socksProxy=localhost:1080", "Connection property 'socksProxy' is not allowed");

        // invalid ssl flag
        assertInvalid("jdbc:presto://localhost:8080?SSL=0", "Connection property 'SSL' value is invalid: 0");
        assertInvalid("jdbc:presto://localhost:8080?SSL=1", "Connection property 'SSL' value is invalid: 1");
        assertInvalid("jdbc:presto://localhost:8080?SSL=2", "Connection property 'SSL' value is invalid: 2");
        assertInvalid("jdbc:presto://localhost:8080?SSL=abc", "Connection property 'SSL' value is invalid: abc");

        //invalid ssl verification mode
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLVerification=0", "Connection property 'SSLVerification' value is invalid: 0");
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLVerification=abc", "Connection property 'SSLVerification' value is invalid: abc");

        // ssl verification without ssl
        assertInvalid("jdbc:presto://localhost:8080?SSLVerification=FULL", "Connection property 'SSLVerification' is not allowed");

        // ssl verification using port 443 without ssl
        assertInvalid("jdbc:presto://localhost:443?SSLVerification=FULL", "Connection property 'SSLVerification' is not allowed");

        // ssl key store password without path
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLKeyStorePassword=password", "Connection property 'SSLKeyStorePassword' is not allowed");

        // ssl key store type without path
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLKeyStoreType=type", "Connection property 'SSLKeyStoreType' is not allowed");

        // ssl trust store password without path
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLTrustStorePassword=password", "Connection property 'SSLTrustStorePassword' is not allowed");

        // ssl trust store type without path
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLTrustStoreType=type", "Connection property 'SSLTrustStoreType' is not allowed");

        // key store path without ssl
        assertInvalid("jdbc:presto://localhost:8080?SSLKeyStorePath=keystore.jks", "Connection property 'SSLKeyStorePath' is not allowed");

        // key store path using port 443 without ssl
        assertInvalid("jdbc:presto://localhost:443?SSLKeyStorePath=keystore.jks", "Connection property 'SSLKeyStorePath' is not allowed");

        // trust store path without ssl
        assertInvalid("jdbc:presto://localhost:8080?SSLTrustStorePath=truststore.jks", "Connection property 'SSLTrustStorePath' is not allowed");

        // trust store path using port 443 without ssl
        assertInvalid("jdbc:presto://localhost:443?SSLTrustStorePath=truststore.jks", "Connection property 'SSLTrustStorePath' is not allowed");

        // key store password without ssl
        assertInvalid("jdbc:presto://localhost:8080?SSLKeyStorePassword=password", "Connection property 'SSLKeyStorePassword' is not allowed");

        // trust store password without ssl
        assertInvalid("jdbc:presto://localhost:8080?SSLTrustStorePassword=password", "Connection property 'SSLTrustStorePassword' is not allowed");

        // key store path with ssl verification mode NONE
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStorePath=keystore.jks", "Connection property 'SSLKeyStorePath' is not allowed");

        // ssl key store password with ssl verification mode NONE
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStorePassword=password", "Connection property 'SSLKeyStorePassword' is not allowed");

        // ssl key store type with ssl verification mode NONE
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStoreType=type", "Connection property 'SSLKeyStoreType' is not allowed");

        // trust store path with ssl verification mode NONE
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLVerification=NONE&SSLTrustStorePath=truststore.jks", "Connection property 'SSLTrustStorePath' is not allowed");

        // ssl trust store password with ssl verification mode NONE
        assertInvalid("jdbc:presto://localhost:8080?SSL=true&SSLVerification=NONE&SSLTrustStorePassword=password", "Connection property 'SSLTrustStorePassword' is not allowed");

        // key store path with ssl verification mode NONE
        assertInvalid("jdbc:presto://localhost:8080?SSLKeyStorePath=keystore.jks", "Connection property 'SSLKeyStorePath' is not allowed");

        // kerberos config without service name
        assertInvalid("jdbc:presto://localhost:8080?KerberosCredentialCachePath=/test", "Connection property 'KerberosCredentialCachePath' is not allowed");

        // invalid extra credentials
        assertInvalid("presto://localhost:8080?extraCredentials=:invalid", "Connection property 'extraCredentials' value is invalid:");
        assertInvalid("presto://localhost:8080?extraCredentials=invalid:", "Connection property 'extraCredentials' value is invalid:");
        assertInvalid("presto://localhost:8080?extraCredentials=:invalid", "Connection property 'extraCredentials' value is invalid:");

        // duplicate credential keys
        assertInvalid("presto://localhost:8080?extraCredentials=test.token.foo:bar;test.token.foo:xyz", "Connection property 'extraCredentials' value is invalid");

        // empty extra credentials
        assertInvalid("presto://localhost:8080?extraCredentials=", "Connection property 'extraCredentials' value is empty");
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'user' is required")
    public void testRequireUser()
            throws Exception
    {
        new PrestoDriverUri("jdbc:presto://localhost:8080", new Properties());
    }

    @Test(expectedExceptions = SQLException.class, expectedExceptionsMessageRegExp = "Connection property 'user' value is empty")
    public void testEmptyUser()
            throws Exception
    {
        new PrestoDriverUri("jdbc:presto://localhost:8080?user=", new Properties());
    }

    @Test
    public void testEmptyPassword()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080?password=");
        assertEquals(parameters.getProperties().getProperty("password"), "");
    }

    @Test
    public void testNonEmptyPassword()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080?password=secret");
        assertEquals(parameters.getProperties().getProperty("password"), "secret");
    }

    @Test
    public void testUriWithSocksProxy()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080?socksProxy=localhost:1234");
        assertUriPortScheme(parameters, 8080, "http");

        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(SOCKS_PROXY.getKey()), "localhost:1234");
    }

    @Test
    public void testUriWithHttpProxy()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080?httpProxy=localhost:5678");
        assertUriPortScheme(parameters, 8080, "http");

        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(HTTP_PROXY.getKey()), "localhost:5678");
    }

    @Test
    public void testUriWithoutSsl()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole");
        assertUriPortScheme(parameters, 8080, "http");
    }

    @Test
    public void testUriWithSslDisabled()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole?SSL=false");
        assertUriPortScheme(parameters, 8080, "http");
    }

    @Test
    public void testUriWithSslEnabled()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole?SSL=true");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertNull(properties.getProperty(SSL_TRUST_STORE_PATH.getKey()));
        assertNull(properties.getProperty(SSL_TRUST_STORE_PASSWORD.getKey()));
    }

    @Test
    public void testUriWithSslDisabledUsing443()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:443/blackhole?SSL=false");
        assertUriPortScheme(parameters, 443, "http");
    }

    @Test
    public void testUriWithSslEnabledUsing443()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:443/blackhole");
        assertUriPortScheme(parameters, 443, "https");
    }

    @Test
    public void testUriWithSslEnabledPathOnly()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole?SSL=true&SSLTrustStorePath=truststore.jks");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(SSL_TRUST_STORE_PATH.getKey()), "truststore.jks");
        assertNull(properties.getProperty(SSL_TRUST_STORE_PASSWORD.getKey()));
    }

    @Test
    public void testUriWithSslEnabledPassword()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080/blackhole?SSL=true&SSLTrustStorePath=truststore.jks&SSLTrustStorePassword=password");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(SSL_TRUST_STORE_PATH.getKey()), "truststore.jks");
        assertEquals(properties.getProperty(SSL_TRUST_STORE_PASSWORD.getKey()), "password");
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationFull()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:443/blackhole?SSL=true&SSLVerification=FULL");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(SSL_VERIFICATION.getKey()), FULL.name());
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationCA()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:443/blackhole?SSL=true&SSLVerification=CA");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(SSL_VERIFICATION.getKey()), CA.name());
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationNONE()
            throws SQLException
    {
        PrestoDriverUri parameters = createDriverUri("presto://localhost:443/blackhole?SSL=true&SSLVerification=NONE");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(SSL_VERIFICATION.getKey()), NONE.name());
    }

    @Test
    public void testUriWithExtraCredentials()
            throws SQLException
    {
        String extraCredentials = "test.token.foo:bar;test.token.abc:xyz";
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080?extraCredentials=" + extraCredentials);
        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(EXTRA_CREDENTIALS.getKey()), extraCredentials);
    }

    @Test
    public void testUriWithClientTags()
            throws SQLException
    {
        String clientTags = "c1,c2";
        PrestoDriverUri parameters = createDriverUri("presto://localhost:8080?clientTags=" + clientTags);
        Properties properties = parameters.getProperties();
        assertEquals(properties.getProperty(CLIENT_TAGS.getKey()), clientTags);
    }

    private static void assertUriPortScheme(PrestoDriverUri parameters, int port, String scheme)
    {
        URI uri = parameters.getHttpUri();
        assertEquals(uri.getPort(), port);
        assertEquals(uri.getScheme(), scheme);
    }

    private static PrestoDriverUri createDriverUri(String url)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", "test");

        return new PrestoDriverUri(url, properties);
    }

    private static void assertInvalid(String url, String prefix)
    {
        try {
            createDriverUri(url);
            fail("expected exception");
        }
        catch (SQLException e) {
            assertNotNull(e.getMessage());
            if (!e.getMessage().startsWith(prefix)) {
                fail(format("expected:<%s> to start with <%s>", e.getMessage(), prefix));
            }
        }
    }
}
