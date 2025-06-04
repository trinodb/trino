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

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.client.ClientSelectedRole;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Optional;

import static io.trino.client.ClientSelectedRole.Type.ROLE;
import static io.trino.client.uri.ConnectionProperties.APPLICATION_NAME_PREFIX;
import static io.trino.client.uri.ConnectionProperties.ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS;
import static io.trino.client.uri.ConnectionProperties.ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS;
import static io.trino.client.uri.ConnectionProperties.CATALOG;
import static io.trino.client.uri.ConnectionProperties.DISABLE_COMPRESSION;
import static io.trino.client.uri.ConnectionProperties.HTTP_LOGGING_LEVEL;
import static io.trino.client.uri.ConnectionProperties.HTTP_PROXY;
import static io.trino.client.uri.ConnectionProperties.LOCALE;
import static io.trino.client.uri.ConnectionProperties.PASSWORD;
import static io.trino.client.uri.ConnectionProperties.RESOURCE_ESTIMATES;
import static io.trino.client.uri.ConnectionProperties.ROLES;
import static io.trino.client.uri.ConnectionProperties.SESSION_PROPERTIES;
import static io.trino.client.uri.ConnectionProperties.SESSION_USER;
import static io.trino.client.uri.ConnectionProperties.SOCKS_PROXY;
import static io.trino.client.uri.ConnectionProperties.SSL;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_PASSWORD;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_PATH;
import static io.trino.client.uri.ConnectionProperties.SSL_KEY_STORE_TYPE;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_PASSWORD;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_PATH;
import static io.trino.client.uri.ConnectionProperties.SSL_TRUST_STORE_TYPE;
import static io.trino.client.uri.ConnectionProperties.SSL_VERIFICATION;
import static io.trino.client.uri.ConnectionProperties.TIMEOUT;
import static io.trino.client.uri.ConnectionProperties.TIMEZONE;
import static io.trino.client.uri.ConnectionProperties.USER;
import static io.trino.client.uri.LoggingLevel.HEADERS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoUriBuilder
{
    @Test
    public void testSetSimpleValues()
    {
        assertRoundTrip(USER, "marian", "marian");
        assertRoundTrip(PASSWORD, "password", "password");
        assertRoundTrip(CATALOG, "test", "test");
        assertRoundTrip(SESSION_USER, "test", "test");
        assertRoundTrip(SOCKS_PROXY, HostAndPort.fromParts("proxy", 443), "proxy:443");
        assertRoundTrip(HTTP_PROXY, HostAndPort.fromParts("proxy", 443), "proxy:443");
        assertRoundTrip(APPLICATION_NAME_PREFIX, "prefix", "prefix");
        assertRoundTrip(DISABLE_COMPRESSION, true, "true");
        assertRoundTrip(ASSUME_LITERAL_NAMES_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS, true, "true");
        assertRoundTrip(ASSUME_LITERAL_UNDERSCORE_IN_METADATA_CALLS_FOR_NON_CONFORMING_CLIENTS, true, "true");
        assertRoundTrip(SSL, true, "true");
        assertRoundTrip(TIMEOUT, Duration.valueOf("30s"), "30.00s");
        assertRoundTrip(HTTP_LOGGING_LEVEL, HEADERS, "HEADERS");
        assertRoundTrip(LOCALE, Locale.ENGLISH, "en");
        assertRoundTrip(TIMEZONE, ZoneId.of("Europe/Warsaw"), "Europe/Warsaw");
    }

    @Test
    public void testSetSimpleDependentValues()
    {
        assertRoundTrip(SSL_VERIFICATION, ConnectionProperties.SslVerificationMode.CA, "CA", "https://localhost:443?SSL=true");
        assertRoundTrip(SSL_KEY_STORE_TYPE, "jks", "jks", "https://localhost:443?SSLKeyStorePath=/tmp/file&SSLVerification=CA&SSL=true");
        assertRoundTrip(SSL_KEY_STORE_PASSWORD, "password", "password", "https://localhost:443?SSLKeyStorePath=/tmp/file&SSLVerification=CA&SSL=true");
        assertRoundTrip(SSL_KEY_STORE_PATH, "/tmp/path", "/tmp/path", "https://localhost:443?SSLVerification=CA&SSL=true");
        assertRoundTrip(SSL_TRUST_STORE_TYPE, "jks", "jks", "https://localhost:443?SSLTrustStorePath=/tmp/file&SSLVerification=CA&SSL=true");
        assertRoundTrip(SSL_TRUST_STORE_PASSWORD, "password", "password", "https://localhost:443?SSLTrustStorePath=/tmp/file&SSLVerification=CA&SSL=true");
        assertRoundTrip(SSL_TRUST_STORE_PATH, "/tmp/path", "/tmp/path", "https://localhost:443?SSLVerification=CA&SSL=true");
    }

    @Test
    public void testSetRoles()
    {
        assertRoundTrip(
                ROLES,
                ImmutableMap.of("catalog1", ClientSelectedRole.valueOf("NONE"), "catalog2", ClientSelectedRole.valueOf("ALL"), "catalog3", new ClientSelectedRole(ROLE, Optional.of("public"))),
                "catalog1:none;catalog2:all;catalog3:public");
    }

    @Test
    public void testSetSessionProperties()
    {
        assertRoundTrip(
                SESSION_PROPERTIES,
                ImmutableMap.of("session_key1", "session_value1", "session_key2", "session_value2", "catalog.session_key3", "session_value3"),
                "session_key1:session_value1;session_key2:session_value2;catalog.session_key3:session_value3");
    }

    @Test
    public void testSetResourceEstimates()
    {
        assertRoundTrip(
                RESOURCE_ESTIMATES,
                ImmutableMap.of("EXECUTION_TIME", "10d", "CPU_TIME", "10d", "PEAK_MEMORY", "10G"),
                "EXECUTION_TIME:10d;CPU_TIME:10d;PEAK_MEMORY:10G");
    }

    public <T, V> void assertRoundTrip(ConnectionProperty<V, T> property, T value, V expectedSerialized)
    {
        assertRoundTrip(property, value, expectedSerialized, "https://localhost:443");
    }

    public <T, V> void assertRoundTrip(ConnectionProperty<V, T> property, T value, V expectedSerialized, String connectionUri)
    {
        TrinoUri uri = TrinoUri.builder()
                .setUri(URI.create(connectionUri))
                .setProperty(property, value)
                .build();

        Object actualValue = uri.getProperties().get(property.getKey());
        assertThat(actualValue).isEqualTo(expectedSerialized);
        assertThat(property.encodeValue(value)).isEqualTo(expectedSerialized);
        assertThat(property.decodeValue(expectedSerialized)).isEqualTo(value);
    }
}
