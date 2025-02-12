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
package io.trino.jdbc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.zone.ZoneRulesException;
import java.util.Properties;

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

public class TestTrinoDriverUri
{
    @Test
    public void testInvalidUrls()
    {
        // missing jdbc: prefix
        assertInvalid("test", "Invalid JDBC URL: test");

        // empty jdbc: url
        assertInvalid("jdbc:", "Invalid JDBC URL: jdbc:");

        // empty jdbc: url
        assertInvalid("jdbc:trino:", "Empty JDBC URL: jdbc:trino:");

        // invalid scheme
        assertInvalid("jdbc:mysql://localhost", "Invalid JDBC URL: jdbc:mysql://localhost");

        // invalid scheme
        assertInvalid("jdbc:http://localhost", "Invalid JDBC URL: jdbc:http://localhost");

        // invalid port
        assertInvalid("jdbc:trino://localhost:0/", "Invalid port number:");
        assertInvalid("jdbc:trino://localhost:70000/", "Invalid port number:");

        // extra path segments
        assertInvalid("jdbc:trino://localhost:8080/hive/default/abc", "Invalid path segments in URL:");

        // extra slash
        assertInvalid("jdbc:trino://localhost:8080//", "Catalog name is empty:");

        // has schema but is missing catalog
        assertInvalid("jdbc:trino://localhost:8080//default", "Catalog name is empty:");

        // has catalog but schema is missing
        assertInvalid("jdbc:trino://localhost:8080/a//", "Schema name is empty:");

        // unrecognized property
        assertInvalid("jdbc:trino://localhost:8080/hive/default?ShoeSize=13", "Unrecognized connection property 'ShoeSize'");

        // empty property
        assertInvalid("jdbc:trino://localhost:8080/hive/default?SSL=", "Connection property SSL value is empty");

        // empty ssl verification property
        assertInvalid("jdbc:trino://localhost:8080/hive/default?SSL=true&SSLVerification=", "Connection property SSLVerification value is empty");

        // property in url multiple times
        assertInvalid("jdbc:trino://localhost:8080/blackhole?password=a&password=b", "Connection property password is in the URL multiple times");

        // property not well formed, missing '='
        assertInvalid("jdbc:trino://localhost:8080/blackhole?password&user=abc", "Connection argument is not a valid connection property: 'password'");

        // property in both url and arguments
        assertInvalid("jdbc:trino://localhost:8080/blackhole?user=test123", "Connection property user is passed both by URL and properties");

        // setting both socks and http proxy
        assertInvalid("jdbc:trino://localhost:8080?socksProxy=localhost:1080&httpProxy=localhost:8888", "Connection property socksProxy cannot be used when httpProxy is set");
        assertInvalid("jdbc:trino://localhost:8080?httpProxy=localhost:8888&socksProxy=localhost:1080", "Connection property socksProxy cannot be used when httpProxy is set");

        // invalid ssl flag
        assertInvalid("jdbc:trino://localhost:8080?SSL=0", "Connection property SSL value is invalid: 0");
        assertInvalid("jdbc:trino://localhost:8080?SSL=1", "Connection property SSL value is invalid: 1");
        assertInvalid("jdbc:trino://localhost:8080?SSL=2", "Connection property SSL value is invalid: 2");
        assertInvalid("jdbc:trino://localhost:8080?SSL=abc", "Connection property SSL value is invalid: abc");

        //invalid ssl verification mode
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLVerification=0", "Connection property SSLVerification value is invalid: 0");
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLVerification=abc", "Connection property SSLVerification value is invalid: abc");

        // ssl verification without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLVerification=FULL", "Connection property SSLVerification requires TLS/SSL to be enabled");

        // ssl verification using port 8080 without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLVerification=FULL", "Connection property SSLVerification requires TLS/SSL to be enabled");

        // ssl key store password without path
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLKeyStorePassword=password", "Connection property SSLKeyStorePassword requires SSLKeyStorePath to be set");

        // ssl key store type without path
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLKeyStoreType=type", "Connection property SSLKeyStoreType requires SSLKeyStorePath to be set");

        // ssl trust store password without path
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLTrustStorePassword=password", "Connection property SSLTrustStorePassword requires SSLTrustStorePath to be set");

        // ssl trust store type without path
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLTrustStoreType=type", "Connection property SSLTrustStoreType requires SSLTrustStorePath to be set or SSLUseSystemTrustStore to be enabled");

        // key store path without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // key store path using port 8080 without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // trust store path without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLVerification is set to NONE");

        // trust store path using port 8080 without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLVerification is set to NONE");

        // key store password without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLKeyStorePassword=password", "Connection property SSLKeyStorePassword requires SSLKeyStorePath to be set");

        // trust store password without ssl
        assertInvalid("jdbc:trino://localhost:8080?SSLTrustStorePassword=password", "Connection property SSLTrustStorePassword requires SSLTrustStorePath to be set");

        // key store path with ssl verification mode NONE
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // ssl key store password with ssl verification mode NONE
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStorePassword=password", "Connection property SSLKeyStorePassword requires SSLKeyStorePath to be set");

        // ssl key store type with ssl verification mode NONE
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLKeyStoreType=type", "Connection property SSLKeyStoreType requires SSLKeyStorePath to be set");

        // trust store path with ssl verification mode NONE
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLVerification is set to NONE");

        // ssl trust store password with ssl verification mode NONE
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLVerification=NONE&SSLTrustStorePassword=password", "Connection property SSLTrustStorePassword requires SSLTrustStorePath to be set");

        // key store path with ssl verification mode NONE
        assertInvalid("jdbc:trino://localhost:8080?SSLKeyStorePath=keystore.jks", "Connection property SSLKeyStorePath cannot be set if SSLVerification is set to NONE");

        // use system trust store with ssl verification mode NONE
        assertInvalid("jdbc:trino://localhost:8080?SSLUseSystemTrustStore=true", "Connection property SSLUseSystemTrustStore cannot be set if SSLVerification is set to NONE");

        // use system trust store with key store path
        assertInvalid("jdbc:trino://localhost:8080?SSL=true&SSLUseSystemTrustStore=true&SSLTrustStorePath=truststore.jks", "Connection property SSLTrustStorePath cannot be set if SSLUseSystemTrustStore is enabled");

        // kerberos config without service name
        assertInvalid("jdbc:trino://localhost:8080?KerberosCredentialCachePath=/test", "Connection property KerberosCredentialCachePath requires KerberosRemoteServiceName to be set");

        // kerberos config with delegated kerberos
        assertInvalid("jdbc:trino://localhost:8080?KerberosRemoteServiceName=test&KerberosDelegation=true&KerberosCredentialCachePath=/test", "Connection property KerberosCredentialCachePath cannot be set if KerberosDelegation is enabled");

        // invalid extra credentials
        assertInvalid("jdbc:trino://localhost:8080?extraCredentials=:invalid", "Connection property extraCredentials value is invalid:");
        assertInvalid("jdbc:trino://localhost:8080?extraCredentials=invalid:", "Connection property extraCredentials value is invalid:");
        assertInvalid("jdbc:trino://localhost:8080?extraCredentials=:invalid", "Connection property extraCredentials value is invalid:");

        // duplicate credential keys
        assertInvalid("jdbc:trino://localhost:8080?extraCredentials=test.token.foo:bar;test.token.foo:xyz", "Connection property extraCredentials value is invalid");

        // empty extra credentials
        assertInvalid("jdbc:trino://localhost:8080?extraCredentials=", "Connection property extraCredentials value is empty");

        // legacy url
        assertInvalid("jdbc:presto://localhost:8080", "Invalid JDBC URL: jdbc:presto://localhost:8080");

        // cannot set mutually exclusive properties for non-conforming clients to true
        assertInvalid("jdbc:trino://localhost:8080?assumeLiteralNamesInMetadataCallsForNonConformingClients=true&assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=true",
                "Connection property assumeLiteralNamesInMetadataCallsForNonConformingClients cannot be set if assumeLiteralUnderscoreInMetadataCallsForNonConformingClients is enabled");
    }

    @Test
    public void testEmptyUser()
    {
        assertThatThrownBy(() -> TrinoDriverUri.createDriverUri("jdbc:trino://localhost:8080?user=", new Properties()))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property user value is empty");
    }

    @Test
    public void testEmptyPassword()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?password=");
        assertThat(parameters.getProperties().getProperty("password")).isEqualTo("");
    }

    @Test
    public void testNonEmptyPassword()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:443?password=secret");
        assertThat(parameters.getProperties().getProperty("password")).isEqualTo("secret");
    }

    @Test
    public void testUriWithSocksProxy()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?socksProxy=localhost:1234");
        assertUriPortScheme(parameters, 8080, "http");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SOCKS_PROXY.toString())).isEqualTo("localhost:1234");
    }

    @Test
    public void testUriWithHttpProxy()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?httpProxy=localhost:5678");
        assertUriPortScheme(parameters, 8080, "http");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(HTTP_PROXY.toString())).isEqualTo("localhost:5678");
    }

    @Test
    public void testUriWithoutCompression()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?disableCompression=true");
        assertThat(parameters.isCompressionDisabled()).isTrue();

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(DISABLE_COMPRESSION.toString())).isEqualTo("true");
    }

    @Test
    public void testUriWithoutSsl()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/blackhole");
        assertUriPortScheme(parameters, 8080, "http");
    }

    @Test
    public void testUriWithTimeout()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/?timeout=10s");
        assertThat(parameters.getTimeout()).isEqualTo(Duration.valueOf("10s"));
    }

    @Test
    public void testUriWithSslDisabled()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/blackhole?SSL=false");
        assertUriPortScheme(parameters, 8080, "http");
    }

    @Test
    public void testUriWithSslEnabled()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/blackhole?SSL=true");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PATH.toString())).isNull();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PASSWORD.toString())).isNull();
    }

    @Test
    public void testSqlPath()
            throws SQLException
    {
        assertInvalid("jdbc:trino://localhost:8080?path=catalog.schema.whatever", "Connection property 'path' has invalid syntax, should be [catalog].[schema] or [schema]");
        assertInvalid("jdbc:trino://localhost:8080", properties("path", "catalog.schema.whatever"), "Connection property 'path' has invalid syntax, should be [catalog].[schema] or [schema]");

        assertThat(createDriverUri("jdbc:trino://localhost:8080?path=catalog.schema").getPath()).hasValue(ImmutableList.of("catalog.schema"));
        assertThat(createDriverUri("jdbc:trino://localhost:8080?path=schema,schema2").getPath()).hasValue(ImmutableList.of("schema", "schema2"));

        assertThat(createDriverUri("jdbc:trino://localhost:8080", properties("path", "catalog.schema,schema2")).getPath()).hasValue(ImmutableList.of("catalog.schema", "schema2"));
        assertThat(createDriverUri("jdbc:trino://localhost:8080", properties("path", "schema")).getPath()).hasValue(ImmutableList.of("schema"));
    }

    @Test
    public void testUriWithSslDisabledUsing443()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:443/blackhole?SSL=false");
        assertUriPortScheme(parameters, 443, "http");
    }

    @Test
    public void testUriWithSslEnabledUsing443()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:443/blackhole");
        assertUriPortScheme(parameters, 443, "https");
    }

    @Test
    public void testUriWithSslEnabledPathOnly()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/blackhole?SSL=true&SSLTrustStorePath=truststore.jks");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PATH.toString())).isEqualTo("truststore.jks");
        assertThat(properties.getProperty(SSL_TRUST_STORE_PASSWORD.toString())).isNull();
    }

    @Test
    public void testUriWithSslEnabledPassword()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/blackhole?SSL=true&SSLTrustStorePath=truststore.jks&SSLTrustStorePassword=password");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_TRUST_STORE_PATH.toString())).isEqualTo("truststore.jks");
        assertThat(properties.getProperty(SSL_TRUST_STORE_PASSWORD.toString())).isEqualTo("password");
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationFull()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:443/blackhole?SSL=true&SSLVerification=FULL");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_VERIFICATION.toString())).isEqualTo("FULL");
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationCA()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:443/blackhole?SSL=true&SSLVerification=CA");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_VERIFICATION.toString())).isEqualTo("CA");
    }

    @Test
    public void testUriWithSslEnabledUsing443SslVerificationNONE()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:443/blackhole?SSL=true&SSLVerification=NONE");
        assertUriPortScheme(parameters, 443, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_VERIFICATION.toString())).isEqualTo("NONE");
    }

    @Test
    public void testUriWithSslEnabledSystemTrustStoreDefault()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/blackhole?SSL=true&SSLUseSystemTrustStore=true");
        assertUriPortScheme(parameters, 8080, "https");

        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(SSL_USE_SYSTEM_TRUST_STORE.toString())).isEqualTo("true");
    }

    @Test
    public void testUriWithSslEnabledSystemTrustStoreOverride()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/blackhole?SSL=true&SSLTrustStoreType=Override&SSLUseSystemTrustStore=true");
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
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?extraCredentials=" + extraCredentials);
        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(EXTRA_CREDENTIALS.toString())).isEqualTo(extraCredentials);
    }

    @Test
    public void testUriWithClientTags()
            throws SQLException
    {
        String clientTags = "c1,c2";
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?clientTags=" + clientTags);
        Properties properties = parameters.getProperties();
        assertThat(properties.getProperty(CLIENT_TAGS.toString())).isEqualTo(clientTags);
    }

    @Test
    public void testOptionalCatalogAndSchema()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080");
        assertThat(parameters.getCatalog()).isEmpty();
        assertThat(parameters.getSchema()).isEmpty();
    }

    @Test
    public void testOptionalSchema()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080/catalog");
        assertThat(parameters.getCatalog()).isPresent();
        assertThat(parameters.getSchema()).isEmpty();
    }

    @Test
    public void testAssumeLiteralNamesInMetadataCallsForNonConformingClients()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?assumeLiteralNamesInMetadataCallsForNonConformingClients=true");
        assertThat(parameters.isAssumeLiteralNamesInMetadataCallsForNonConformingClients()).isTrue();
        assertThat(parameters.isAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()).isFalse();
    }

    @Test
    public void testAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()
            throws SQLException
    {
        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?assumeLiteralUnderscoreInMetadataCallsForNonConformingClients=true");
        assertThat(parameters.isAssumeLiteralUnderscoreInMetadataCallsForNonConformingClients()).isTrue();
        assertThat(parameters.isAssumeLiteralNamesInMetadataCallsForNonConformingClients()).isFalse();
    }

    @Test
    public void testTimezone()
            throws SQLException
    {
        TrinoDriverUri defaultParameters = createDriverUri("jdbc:trino://localhost:8080");
        assertThat(defaultParameters.getTimeZone()).isEqualTo(ZoneId.systemDefault());

        TrinoDriverUri parameters = createDriverUri("jdbc:trino://localhost:8080?timezone=Asia/Kolkata");
        assertThat(parameters.getTimeZone()).isEqualTo(ZoneId.of("Asia/Kolkata"));

        TrinoDriverUri offsetParameters = createDriverUri("jdbc:trino://localhost:8080?timezone=UTC+05:30");
        assertThat(offsetParameters.getTimeZone()).isEqualTo(ZoneId.of("UTC+05:30"));

        assertThatThrownBy(() -> createDriverUri("jdbc:trino://localhost:8080?timezone=Asia/NOT_FOUND"))
                .isInstanceOf(SQLException.class)
                .hasMessage("Connection property timezone value is invalid: Asia/NOT_FOUND")
                .hasRootCauseInstanceOf(ZoneRulesException.class)
                .hasRootCauseMessage("Unknown time-zone ID: Asia/NOT_FOUND");
    }

    @Test
    public void testDefaultPorts()
            throws SQLException
    {
        TrinoDriverUri uri = createDriverUri("jdbc:trino://localhost");
        assertThat(uri.getHttpUri()).isEqualTo(URI.create("http://localhost:80"));

        TrinoDriverUri secureUri = createDriverUri("jdbc:trino://localhost?SSL=true");
        assertThat(secureUri.getHttpUri()).isEqualTo(URI.create("https://localhost:443"));
    }

    @Test
    public void testAValidateConnection()
            throws SQLException
    {
        TrinoDriverUri uri = createDriverUri("jdbc:trino://localhost:8080");
        assertThat(uri.isValidateConnection()).isFalse();
        uri = createDriverUri("jdbc:trino://localhost:8080?validateConnection=true");
        assertThat(uri.isValidateConnection()).isTrue();
        uri = createDriverUri("jdbc:trino://localhost:8080?validateConnection=false");
        assertThat(uri.isValidateConnection()).isFalse();
    }

    private static void assertUriPortScheme(TrinoDriverUri parameters, int port, String scheme)
    {
        URI uri = parameters.getHttpUri();
        assertThat(uri.getPort()).isEqualTo(port);
        assertThat(uri.getScheme()).isEqualTo(scheme);
    }

    private static TrinoDriverUri createDriverUri(String url)
            throws SQLException
    {
        return createDriverUri(url, properties("user", "test"));
    }

    private static TrinoDriverUri createDriverUri(String url, Properties properties)
            throws SQLException
    {
        return TrinoDriverUri.createDriverUri(url, properties);
    }

    private static Properties properties(String key, String value)
    {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        return properties;
    }

    private static void assertInvalid(String url, String prefix)
    {
        assertThatThrownBy(() -> createDriverUri(url))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining(prefix);
    }

    private static void assertInvalid(String url, Properties properties, String prefix)
    {
        assertThatThrownBy(() -> createDriverUri(url, properties))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining(prefix);
    }
}
