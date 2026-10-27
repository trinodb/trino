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
package io.trino.plugin.kafka.schema.confluent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.kafka.security.KafkaEndpointIdentificationAlgorithm.DISABLED;
import static io.trino.plugin.kafka.security.KafkaKeystoreTruststoreType.PKCS12;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Connects the schema registry client, configured from {@link ConfluentSchemaRegistrySecurityConfig},
 * to in-process registries that require HTTP basic authentication, TLS and client certificates.
 */
@TestInstance(PER_CLASS)
public class TestConfluentSchemaRegistrySecurity
{
    private static final String USER = "trino";
    private static final String PASSWORD = "secret";
    private static final String STORE_PASSWORD = "secret";
    private static final int SCHEMA_ID = 1;
    private static final String SCHEMA =
            """
            {"type": "record", "name": "test", "fields": [{"name": "col_1", "type": "string"}]}
            """;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private HttpServer server;
    private String baseUrl;
    // serves a certificate for 127.0.0.1
    private HttpsServer tlsServer;
    private String tlsBaseUrl;
    // serves a certificate for another host name
    private HttpsServer otherHostTlsServer;
    private String otherHostTlsBaseUrl;
    // requires a client certificate signed by the trust store
    private HttpsServer clientAuthTlsServer;
    private String clientAuthTlsBaseUrl;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/", this::handle);
        server.start();
        baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        tlsServer = startTlsServer("confluent/registry-keystore.p12", Optional.empty());
        tlsBaseUrl = "https://127.0.0.1:" + tlsServer.getAddress().getPort();
        otherHostTlsServer = startTlsServer("confluent/registry-other-host-keystore.p12", Optional.empty());
        otherHostTlsBaseUrl = "https://127.0.0.1:" + otherHostTlsServer.getAddress().getPort();
        clientAuthTlsServer = startTlsServer("confluent/registry-keystore.p12", Optional.of("confluent/registry-truststore.p12"));
        clientAuthTlsBaseUrl = "https://127.0.0.1:" + clientAuthTlsServer.getAddress().getPort();
    }

    @AfterAll
    public void tearDown()
    {
        server.stop(0);
        tlsServer.stop(0);
        otherHostTlsServer.stop(0);
        clientAuthTlsServer.stop(0);
    }

    @Test
    public void testBasicAuth()
            throws Exception
    {
        assertSchemaFound(createClient(baseUrl, basicAuthConfig()));
    }

    @Test
    public void testMissingCredentials()
    {
        assertThatThrownBy(() -> createClient(baseUrl, new ConfluentSchemaRegistrySecurityConfig()).getSchemaById(SCHEMA_ID))
                .isInstanceOfSatisfying(RestClientException.class, e -> assertThat(e.getStatus()).isEqualTo(401));
    }

    @Test
    public void testWrongCredentials()
    {
        ConfluentSchemaRegistrySecurityConfig config = new ConfluentSchemaRegistrySecurityConfig()
                .setBasicAuthUser(USER)
                .setBasicAuthPassword("wrong");
        assertThatThrownBy(() -> createClient(baseUrl, config).getSchemaById(SCHEMA_ID))
                .isInstanceOfSatisfying(RestClientException.class, e -> assertThat(e.getStatus()).isEqualTo(401));
    }

    @Test
    public void testTlsWithTruststore()
            throws Exception
    {
        assertSchemaFound(createClient(tlsBaseUrl, tlsConfig()));
    }

    @Test
    public void testTlsWithoutTruststore()
    {
        // the registry certificate is not signed by a certificate authority of the default trust store
        assertThatThrownBy(() -> createClient(tlsBaseUrl, basicAuthConfig()).getSchemaById(SCHEMA_ID))
                .isInstanceOf(SSLException.class)
                .hasMessageContaining("PKIX path building failed");
    }

    @Test
    public void testTlsHostNameVerification()
            throws Exception
    {
        // the certificate is trusted but issued for another host name
        assertThatThrownBy(() -> createClient(otherHostTlsBaseUrl, tlsConfig()).getSchemaById(SCHEMA_ID))
                .isInstanceOf(SSLException.class)
                .hasMessageContaining("No subject alternative names matching IP address 127.0.0.1 found");

        assertSchemaFound(createClient(otherHostTlsBaseUrl, tlsConfig().setEndpointIdentificationAlgorithm(DISABLED)));
    }

    @Test
    public void testTlsClientCertificate()
            throws Exception
    {
        assertSchemaFound(createClient(clientAuthTlsBaseUrl, clientCertificateConfig()));
    }

    @Test
    public void testTlsWithoutRequiredClientCertificate()
    {
        // the exception depends on whether the client reads the certificate_required alert before writing its request
        assertThatThrownBy(() -> createClient(clientAuthTlsBaseUrl, tlsConfig()).getSchemaById(SCHEMA_ID))
                .isInstanceOf(IOException.class);
    }

    @Test
    public void testClientFromConnectorConfiguration()
            throws Exception
    {
        // the client the connector creates combines the properties of all providers
        ConfluentSchemaRegistryConfig registryConfig = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistryUrls(ImmutableSet.of(clientAuthTlsBaseUrl));
        try (SchemaRegistryClient client = ConfluentModule.createSchemaRegistryClient(
                registryConfig,
                ImmutableSet.of(new AvroSchemaProvider()),
                ImmutableSet.of(new SchemaRegistryClientTtlProvider(registryConfig), new SchemaRegistryClientSecurityProvider(clientCertificateConfig())),
                getClass().getClassLoader())) {
            assertSchemaFound(client);
        }
    }

    @Test
    public void testHostNameVerifiedByClientFromConnectorConfiguration()
            throws Exception
    {
        // the connector passes the endpoint identification algorithm, so the client does not accept any host name
        ConfluentSchemaRegistryConfig registryConfig = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistryUrls(ImmutableSet.of(otherHostTlsBaseUrl));
        try (SchemaRegistryClient client = ConfluentModule.createSchemaRegistryClient(
                registryConfig,
                ImmutableSet.of(new AvroSchemaProvider()),
                ImmutableSet.of(new SchemaRegistryClientTtlProvider(registryConfig), new SchemaRegistryClientSecurityProvider(tlsConfig())),
                getClass().getClassLoader())) {
            assertThatThrownBy(() -> client.getSchemaById(SCHEMA_ID))
                    .isInstanceOf(SSLException.class)
                    .hasMessageContaining("No subject alternative names matching IP address 127.0.0.1 found");
        }
    }

    private static void assertSchemaFound(SchemaRegistryClient client)
            throws IOException, RestClientException
    {
        ParsedSchema schema = client.getSchemaById(SCHEMA_ID);
        assertThat(schema.schemaType()).isEqualTo("AVRO");
        assertThat(schema.name()).isEqualTo("test");
    }

    private static ConfluentSchemaRegistrySecurityConfig basicAuthConfig()
    {
        return new ConfluentSchemaRegistrySecurityConfig()
                .setBasicAuthUser(USER)
                .setBasicAuthPassword(PASSWORD);
    }

    private static ConfluentSchemaRegistrySecurityConfig tlsConfig()
    {
        return basicAuthConfig()
                .setTruststoreLocation(resourcePath("confluent/registry-truststore.p12"))
                .setTruststorePassword(STORE_PASSWORD)
                .setTruststoreType(PKCS12);
    }

    private static ConfluentSchemaRegistrySecurityConfig clientCertificateConfig()
    {
        return tlsConfig()
                .setKeystoreLocation(resourcePath("confluent/registry-keystore.p12"))
                .setKeystorePassword(STORE_PASSWORD)
                .setKeystoreType(PKCS12);
    }

    private static SchemaRegistryClient createClient(String url, ConfluentSchemaRegistrySecurityConfig config)
    {
        return new CachedSchemaRegistryClient(
                ImmutableList.of(url),
                10,
                ImmutableList.of(new AvroSchemaProvider()),
                ImmutableMap.<String, Object>builder()
                        .putAll(config.getSchemaRegistryClientProperties())
                        .put("max.retries", "0")
                        .buildOrThrow());
    }

    private void handle(HttpExchange exchange)
            throws IOException
    {
        String expectedAuthorization = "Basic " + Base64.getEncoder().encodeToString((USER + ":" + PASSWORD).getBytes(UTF_8));
        if (!expectedAuthorization.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
            respond(exchange, 401, ImmutableMap.of("error_code", 401, "message", "Unauthorized"));
            return;
        }
        if (exchange.getRequestURI().getPath().equals("/schemas/ids/" + SCHEMA_ID)) {
            respond(exchange, 200, ImmutableMap.of("schemaType", "AVRO", "schema", SCHEMA));
            return;
        }
        respond(exchange, 404, ImmutableMap.of("error_code", 40403, "message", "Schema not found"));
    }

    private void respond(HttpExchange exchange, int status, Map<String, Object> body)
            throws IOException
    {
        byte[] bytes = objectMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().add("Content-Type", "application/vnd.schemaregistry.v1+json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private HttpsServer startTlsServer(String keystoreResource, Optional<String> clientTruststoreResource)
            throws IOException
    {
        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(loadKeyStore(keystoreResource), STORE_PASSWORD.toCharArray());
            TrustManager[] trustManagers = null;
            if (clientTruststoreResource.isPresent()) {
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(loadKeyStore(clientTruststoreResource.get()));
                trustManagers = trustManagerFactory.getTrustManagers();
            }
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagers, null);
            HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
            httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext)
            {
                @Override
                public void configure(HttpsParameters parameters)
                {
                    SSLParameters sslParameters = sslContext.getDefaultSSLParameters();
                    sslParameters.setNeedClientAuth(clientTruststoreResource.isPresent());
                    parameters.setSSLParameters(sslParameters);
                }
            });
            httpsServer.createContext("/", this::handle);
            httpsServer.start();
            return httpsServer;
        }
        catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }

    private static KeyStore loadKeyStore(String resource)
            throws IOException, GeneralSecurityException
    {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Resources.getResource(resource).openStream()) {
            keyStore.load(in, STORE_PASSWORD.toCharArray());
        }
        return keyStore;
    }

    private static String resourcePath(String resource)
    {
        try {
            return Path.of(Resources.getResource(resource).toURI()).toString();
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
