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

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.SQLException;
import java.util.Properties;

import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.CA;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.FULL;
import static io.trino.client.uri.ConnectionProperties.SslVerificationMode.NONE;
import static io.trino.client.uri.PropertyName.CLIENT_TAGS;
import static io.trino.client.uri.PropertyName.DISABLE_COMPRESSION;
import static io.trino.client.uri.PropertyName.EXTRA_CREDENTIALS;
import static io.trino.client.uri.PropertyName.HTTP_PROXY;
import static io.trino.client.uri.PropertyName.SOCKS_PROXY;
import static io.trino.client.uri.PropertyName.SSL_TRUST_STORE_PASSWORD;
import static io.trino.client.uri.PropertyName.SSL_TRUST_STORE_PATH;
import static io.trino.client.uri.PropertyName.SSL_TRUST_STORE_TYPE;
import static io.trino.client.uri.PropertyName.SSL_USE_SYSTEM_TRUST_STORE;
import static io.trino.client.uri.PropertyName.SSL_VERIFICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTrinoUri
{
    @Test
    public void testInvalidUrls()
    {
        // missing trino: prefix
        assertInvalid("test", "Invalid Trino URL: test");

        // empty trino: url
        assertInvalid("trino:", "Empty Trino URL: trino:");

        // invalid scheme
        assertInvalid("mysql://localhost", "Invalid Trino URL: mysql://localhost");

        // missing port
        assertInvalid("trino://localhost/", "No port number specified:");

        // extra path segments
        assertInvalid("trino://localhost:8080/hive/default/abc", "Invalid path segments in URL:");

        // extra slash
        assertInvalid("trino://localhost:8080//", "Catalog name is empty:");

        // has schema but is missing catalog
        assertInvalid("trino://localhost:8080//default", "Catalog name is empty:");

        // has catalog but schema is missing
        assertInvalid("trino://localhost:8080/a//", "Schema name is empty:");

        // unrecognized property
        assertInvalid("trino://localhost:8080/hive/default?ShoeSize=13", "Unrecognized connection property 'ShoeSize'");

        // empty property
        assertInvalid("trino://localhost:8080/hive/default?SSL=", "Connection property SSL value is empty");

        // empty ssl verification property
        assertInvalid("trino://localhost:8080/hive/default?SSL=true&SSLVerification=", "Connection property SSLVerification value is empty");

        // property in url multiple times
        assertInvalid("trino://localhost:8080/blackhole?password=a&password=b", "Connection property password is in the URL multiple times");

        // property not well formed, missing '='
        assertInvalid("trino://localhost:8080/blackhole?password&user=abc", "Connection argument is not a valid connection property: 'password'");

        // property in both url and arguments
        assertInvalid("trino://localhost:8080/blackhole?user=test123", "Connection property user is both in the URL and an argument");

        // setting both socks and http proxy
        assertInvalid("trino://localhost:8080?socksProxy=localhost:1080&httpProxy=localhost:8888", "Connection property socksProxy cannot be used when httpProxy is set");
        assertInvalid("trino://localhost:8080?httpProxy=localhost:8888&socksProxy=localhost:1080", "Connection property socksProxy cannot be used when httpProxy is set");

        // invalid ssl flag
        assertInvalid("trino://localhost:8080?SSL=0", "Connection property SSL value is invalid: 0");
        assertInvalid("trino://localhost:8080?SSL=1", "Connection property SSL value is invalid: 1");
        assertInvalid("trino://localhost:8080?SSL=2", "Connection property SSL value is invalid: 2");
        assertInvalid("trino://localhost:8080?SSL=abc", "Connection property SSL value is invalid: abc");

        //invalid ssl verification mode
        assertInvalid("trino://localhost:8080?SSL=true&SSLVerification=0", "Connection property SSLVerification value is invalid: 0");
        assertInvalid("trino://localhost:8080?SSL=true&SSLVerification=abc", "Connection property SSLVerification value is invalid: abc");

        // ssl verification without ssl
        assertInvalid("trino://localhost:8080?SSLVerification=FULL", "Connection property SSLVerification requires TLS/SSL to be enabled");

        // ssl verification using port 443 without ssl
        assertInvalid("trino://localhost:443?SSLVerification=FULL", "Connection property SSLVerification requires TLS/SSL to be enabled");

        // ssl key store password without path
        assertInvalid("trino://localhost:8080?SSL=true&SSLKeyStorePassword=password", "Connection property SSLKeyStorePassword requires SSLKeyStorePath to be set");

        // ssl key store type without path
        assertInvalid("trino://localhost:8080?SSL=true&SSLKeyStoreType=type", "Connection property SSLKeyStoreType requires SSLKeyStorePath to be set");

        // ssl trust store password without path
        assertInvalid("trino://localhost:8080?SSL=true&SSLTrustStorePassword=password", "Connection property SSLTrustStorePassword requires SSLTrustStorePath to be set");

        // ssl trust store type without path
        assertInvalid("trino://localhost:8080?SSL=true&SSLTrustStoreType=type", "Connection property SSLTrustStoreType requires SSLTrustStorePath to be set or SSLUseSystemTrustStore to be enabled");

        // key store path without ssl
        assertInvalid("trino://localhost:8080?SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // key store path using port 443 without ssl
        assertInvalid("trino://localhost:443?SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // trust store path without ssl
        assertInvalid("trino://localhost:8080?SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLVerification is set to NONE");

        // trust store path using port 443 without ssl
        assertInvalid("trino://localhost:443?SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLVerification is set to NONE");

        // key store password without ssl
        assertInvalid("trino://localhost:8080?SSLKeyStorePassword=password", "Connection property SSLKeyStorePassword requires SSLKeyStorePath to be set");

        // trust store password without ssl
        assertInvalid("trino://localhost:8080?SSLTrustStorePassword=password", "Connection property SSLTrustStorePassword requires SSLTrustStorePath to be set");

        // key store path with ssl verification mode NONE
        assertInvalid("trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // ssl key store password with ssl verification mode NONE
        assertInvalid("trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStorePassword=password", "Connection property SSLKeyStorePassword requires SSLKeyStorePath to be set");

        // ssl key store type with ssl verification mode NONE
        assertInvalid("trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStoreType=type", "Connection property SSLKeyStoreType requires SSLKeyStorePath to be set");

        // trust store path with ssl verification mode NONE
        assertInvalid("trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLVerification is set to NONE");

        // ssl trust store password with ssl verification mode NONE
        assertInvalid("trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLTrustStorePassword=password", "Connection property SSLTrustStorePassword requires SSLTrustStorePath to be set");

        // key store path with ssl verification mode NONE
        assertInvalid("trino://localhost:8080?SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // use system trust store with ssl verification mode NONE
        assertInvalid("trino://localhost:8080?SSLUseSystemTrustStore=true", "Connection property SSLUseSystemTrustStore cannot be set if SSLVerification is set to NONE");

        // use system trust store with key store path
        assertInvalid("trino://localhost:8080?SSL=true&SSLUseSystemTrustStore=true&SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLUseSystemTrustStore is enabled");

        // kerberos config without service name
        assertInvalid("trino://localhost:8080?KerberosCredentialCachePath=/test", "Connection property KerberosCredentialCachePath requires KerberosRemoteServiceName to be set");

        // kerberos config with delegated kerberos
        assertInvalid("trino://localhost:8080?KerberosRemoteServiceName=test&KerberosDelegation=true&KerberosCredentialCachePath=/test", "Connection property KerberosCredentialCachePath cannot be set if KerberosDelegation is enabled");

        // invalid extra credentials
        assertInvalid("trino://localhost:8080?extraCredentials=:invalid", "Connection property extraCredentials value is invalid:");
        assertInvalid("trino://localhost:8080?extraCredentials=invalid:", "Connection property extraCredentials value is invalid:");
        assertInvalid("trino://localhost:8080?extraCredentials=:invalid", "Connection property extraCredentials value is invalid:");

        // duplicate credential keys
        assertInvalid("trino://localhost:8080?extraCredentials=test.token.foo:bar;test.token.foo:xyz", "Connection property extraCredentials value is invalid");

        // empty extra credentials
        assertInvalid("trino://localhost:8080?extraCredentials=", "Connection property extraCredentials value is empty");

        // legacy url
        assertInvalid("presto://localhost:8080", "Invalid Trino URL: presto://localhost:8080");

        // cannot set mutually exclusive properties for non-conforming clients to true
        assertInvalid("trino://localhost:8080?assumeLiteralNamesInMetadataCallsForNonConformingClients=true&assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=true",
                "Connection property assumeLiteralNamesInMetadataCallsForNonConformingClients cannot be set if assumeLiteralUnderscoreInMetadataCallsForNonConformingClients is enabled");
    }

    @Test
    public void testEmptyUser()
    {
        assertThatThrownBy(() -> TrinoUri.create("trino://localhost:8080?user=", new Properties()))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property user value is empty");
    }

    @Test
    public void testEmptyPassword()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080?password=");
        assertThat(parameters.getProperties().getProperty("password")).isEqualTo("");
    }

    @Test
    public void testNonEmptyPassword()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080?password=secret");
        assertThat(parameters.getProperties().getProperty("password")).isEqualTo("secret");
    }

    @Test
    public void testUriWithSocksProxy()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080?socksProxy=localhost:1234");
        assertUriPortScheme(parameters, 8080, "http");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SOCKS_PROXY.toString())).isEqualTo("localhost:1234");
    }

    @Test
    public void testUriWithHttpProxy()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080?httpProxy=localhost:5678");
        assertUriPortScheme(parameters, 8080, "http");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(HTTP_PROXY.toString())).isEqualTo("localhost:5678");
    }

    @Test
    public void testUriWithoutCompression()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080?disableCompression=true");
        assertThat(parameters.isCompressionDisabled()).isTrue();

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(DISABLE_COMPRESSION.toString())).isEqualTo("true");
    }

    @Test
    public void testUriWithoutSsl()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/blackhole");
        assertUriPortScheme(parameters, 8080, "http");
    }

    @Test
    public void testUriWithSslDisabled()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/blackhole?SSL=false");
        assertUriPortScheme(parameters, 8080, "http");
    }

    @Test
    public void testUriWithSslEnabled()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/blackhole?SSL=true");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PATH.toString())).isNull();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PASSWORD.toString())).isNull();
    }

    @Test
    public void testUriWithSslDisabledUsing443()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:443/blackhole?SSL=false");
        assertUriPortScheme(parameters, 443, "http");
    }

    @Test
    public void testUriWithSslEnabledUsing443()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:443/blackhole");
        assertUriPortScheme(parameters, 443, "https");
    }

    @Test
    public void testUriWithSslEnabledPathOnly()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/blackhole?SSL=true&SSLTrustStorePath=truststore.jks");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PATH.toString())).isEqualTo("truststore.jks");
        assertThat(properties.getProperty(SSL_TRUST_STORE_PASSWORD.toString())).isNull();
    }

    @Test
    public void testUriWithSslEnabledPassword()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/blackhole?SSL=true&SSLTrustStorePath=truststore.jks&SSLTrustStorePassword=password");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PATH.toString())).isEqualTo("truststore.jks");
        assertThat(properties.getProperty(SSL_TRUST_STORE_PASSWORD.toString())).isEqualTo("password");
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationFull()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:443/blackhole?SSL=true&SSLVerification=FULL");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_VERIFICATION.toString())).isEqualTo(FULL.name());
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationCA()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:443/blackhole?SSL=true&SSLVerification=CA");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_VERIFICATION.toString())).isEqualTo(CA.name());
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationNONE()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:443/blackhole?SSL=true&SSLVerification=NONE");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_VERIFICATION.toString())).isEqualTo(NONE.name());
    }

    @Test
    public void testUriWithSslEnabledSystemTrustStoreDefault()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/blackhole?SSL=true&SSLUseSystemTrustStore=true");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_USE_SYSTEM_TRUST_STORE.toString())).isEqualTo("true");
    }

    @Test
    public void testUriWithSslEnabledSystemTrustStoreOverride()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/blackhole?SSL=true&SSLTrustStoreType=Override&SSLUseSystemTrustStore=true");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_TRUST_STORE_TYPE.toString())).isEqualTo("Override");
        assertThat(properties.getProperty(SSL_USE_SYSTEM_TRUST_STORE.toString())).isEqualTo("true");
    }

    @Test
    public void testUriWithExtraCredentials()
            throws SQLException
    {
        String extraCredentials = "test.token.foo:bar;test.token.abc:xyz";
        TrinoUri parameters = createDriverUri("trino://localhost:8080?extraCredentials=" + extraCredentials);
        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(EXTRA_CREDENTIALS.toString())).isEqualTo(extraCredentials);
    }

    @Test
    public void testUriWithClientTags()
            throws SQLException
    {
        String clientTags = "c1,c2";
        TrinoUri parameters = createDriverUri("trino://localhost:8080?clientTags=" + clientTags);
        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(CLIENT_TAGS.toString())).isEqualTo(clientTags);
    }

    @Test
    public void testOptionalCatalogAndSchema()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080");
        assertThat(parameters.getCatalog()).isEmpty();
        assertThat(parameters.getSchema()).isEmpty();
    }

    @Test
    public void testOptionalSchema()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080/catalog");
        assertThat(parameters.getCatalog()).isPresent();
        assertThat(parameters.getSchema()).isEmpty();
    }

    @Test
    public void testAssumeLiteralNamesInMetadataCallsForNonConformingClients()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080?assumeLiteralNamesInMetadataCallsForNonConformingClients=true");
        assertThat(parameters.isAssumeLiteralNamesInMetadataCallsForNonConformingClients()).isTrue();
        assertThat(parameters.isAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()).isFalse();
    }

    @Test
    public void testAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()
            throws SQLException
    {
        TrinoUri parameters = createDriverUri("trino://localhost:8080?assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=true");
        assertThat(parameters.isAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()).isTrue();
        assertThat(parameters.isAssumeLiteralNamesInMetadataCallsForNonConformingClients()).isFalse();
    }

    private static void assertUriPortScheme(TrinoUri parameters, int port, String scheme)
    {
        URI uri = parameters.getHttpUri();
        assertThat(uri.getPort()).isEqualTo(port);
        assertThat(uri.getScheme()).isEqualTo(scheme);
    }

    private static TrinoUri createDriverUri(String url)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", "test");

        return TrinoUri.create(url, properties);
    }

    private static void assertInvalid(String url, String prefix)
    {
        assertThatThrownBy(() -> createDriverUri(url))
                .isInstanceOf(SQLException.class)
                .hasMessageStartingWith(prefix);
    }
}
