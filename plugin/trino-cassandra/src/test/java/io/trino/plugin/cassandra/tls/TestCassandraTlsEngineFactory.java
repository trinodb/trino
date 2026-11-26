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
package io.trino.plugin.cassandra.tls;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.common.io.Resources.getResource;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestCassandraTlsEngineFactory
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-cassandra-tls-%s"));

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    void testHostnameVerificationEnabledByDefault()
            throws Exception
    {
        SSLEngine sslEngine = createSslEngine(tlsConfig());

        assertThat(sslEngine.getUseClientMode()).isTrue();
        assertThat(sslEngine.getPeerHost()).isEqualTo("cassandra.example.com");
        assertThat(sslEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
    }

    @Test
    void testHostnameVerificationDisabled()
            throws Exception
    {
        SSLEngine sslEngine = createSslEngine(tlsConfig().setVerifyHostnames(false));

        assertThat(sslEngine.getUseClientMode()).isTrue();
        assertThat(sslEngine.getSSLParameters().getEndpointIdentificationAlgorithm()).isNull();
    }

    @Test
    void testCertificateMatchingHost()
            throws Exception
    {
        try (SSLServerSocket serverSocket = createServerSocket("localhost.pem")) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            connect(serverSocket.getLocalPort(), tlsConfig());
            serverHandshake.get(10, SECONDS);
        }
    }

    @Test
    void testCertificateForDifferentHost()
            throws Exception
    {
        try (SSLServerSocket serverSocket = createServerSocket("differenthost.pem")) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            assertThat(connect(serverSocket.getLocalPort(), tlsConfig()))
                    .rootCause()
                    .isInstanceOf(CertificateException.class)
                    .hasMessage("No subject alternative DNS name matching localhost found.");
            // The server sees either the alert sent by the client or the closed connection
            assertThatThrownBy(() -> serverHandshake.get(10, SECONDS))
                    .isInstanceOf(ExecutionException.class);
        }
    }

    @Test
    void testCertificateForDifferentHostWithoutHostnameVerification()
            throws Exception
    {
        try (SSLServerSocket serverSocket = createServerSocket("differenthost.pem")) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            connect(serverSocket.getLocalPort(), tlsConfig().setVerifyHostnames(false));
            serverHandshake.get(10, SECONDS);
        }
    }

    @Test
    void testDefaultTrustAnchors()
            throws Exception
    {
        // Without a key store and a trust store the connection still uses TLS, and the certificate of the test CA is not trusted
        try (SSLServerSocket serverSocket = createServerSocket("localhost.pem")) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            assertThat(connect(serverSocket.getLocalPort(), new CassandraTlsConfig()))
                    .rootCause()
                    .hasMessage("unable to find valid certification path to requested target");
            assertThatThrownBy(() -> serverHandshake.get(10, SECONDS))
                    .isInstanceOf(ExecutionException.class);
        }
    }

    @Test
    void testCertificateForIpAddress()
            throws Exception
    {
        // An address without a host name, like a node discovered by the driver. Verifying the certificate against a reverse DNS lookup of the address, such as
        // "localhost", would reject the certificate, which is valid for the IP address only.
        InetAddress address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        try (SSLServerSocket serverSocket = createServerSocket("iponly.pem", address)) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            connect(new InetSocketAddress(address, serverSocket.getLocalPort()), List.of(), tlsConfig());
            serverHandshake.get(10, SECONDS);
        }
    }

    @Test
    void testCertificateForIpAddressWithResolvedHostName()
            throws Exception
    {
        // The result of a reverse lookup is stored in the address, where it cannot be told apart from a host name the address was created with
        InetAddress address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        assumeThat(address.getHostName()).isNotEqualTo("127.0.0.1");
        assertThat(new InetSocketAddress(address, 0).getHostString()).isNotEqualTo("127.0.0.1");
        try (SSLServerSocket serverSocket = createServerSocket("iponly.pem", address)) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            connect(new InetSocketAddress(address, serverSocket.getLocalPort()), List.of(), tlsConfig());
            serverHandshake.get(10, SECONDS);
        }
    }

    @Test
    void testCertificateForReverseDnsNameOfIpAddress()
            throws Exception
    {
        // The certificate is valid for "localhost" only, which is the reverse DNS name of the IP address
        assumeThat(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}).getHostName()).isEqualTo("localhost");
        InetAddress address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        try (SSLServerSocket serverSocket = createServerSocket("localhost.pem", address)) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            assertThat(connect(new InetSocketAddress(address, serverSocket.getLocalPort()), List.of(), tlsConfig()))
                    .rootCause()
                    .isInstanceOf(CertificateException.class)
                    .hasMessage("No subject alternative names matching IP address 127.0.0.1 found");
            // The server sees either the alert or the closed connection, depending on timing
            assertThatThrownBy(() -> serverHandshake.get(10, SECONDS))
                    .isInstanceOf(ExecutionException.class);
        }
    }

    @Test
    void testCertificateForReverseDnsNameOfIpAddressWithReverseDns()
            throws Exception
    {
        assumeThat(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}).getHostName()).isEqualTo("localhost");
        InetAddress address = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});
        try (SSLServerSocket serverSocket = createServerSocket("localhost.pem", address)) {
            Future<?> serverHandshake = acceptHandshake(serverSocket);
            connect(new InetSocketAddress(address, serverSocket.getLocalPort()), List.of(), tlsConfig().setUseReverseDns(true));
            serverHandshake.get(10, SECONDS);
        }
    }

    private static SSLServerSocket createServerSocket(String keyStore)
            throws Exception
    {
        return createServerSocket(keyStore, InetAddress.getByName("localhost"));
    }

    private static SSLServerSocket createServerSocket(String keyStore, InetAddress address)
            throws Exception
    {
        SSLContext serverContext = createSSLContext(Optional.of(resource(keyStore)), Optional.empty(), Optional.empty(), Optional.empty());
        return (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0, 1, address);
    }

    // The server completes the TLS handshake and closes the connection, as it does not speak the Cassandra protocol
    private Future<?> acceptHandshake(SSLServerSocket serverSocket)
    {
        return executor.submit(() -> {
            try (Socket socket = serverSocket.accept()) {
                ((SSLSocket) socket).startHandshake();
            }
            return null;
        });
    }

    // Returns the failure of the only connection attempt. The connection fails even after a successful TLS handshake, as the server is not a Cassandra node.
    private static Throwable connect(int port, CassandraTlsConfig config)
    {
        return connect(new InetSocketAddress("localhost", port), List.of("localhost"), config);
    }

    private static Throwable connect(InetSocketAddress contactPoint, List<String> contactPointHostNames, CassandraTlsConfig config)
    {
        try (CqlSession _ = CqlSession.builder()
                .withSslEngineFactory(CassandraTlsEngineFactory.createSslEngineFactory(config, contactPointHostNames))
                .addContactPoint(contactPoint)
                .withLocalDatacenter("datacenter1")
                .build()) {
            throw new AssertionError("Connected to a server which is not a Cassandra node");
        }
        catch (AllNodesFailedException e) {
            return e.getAllErrors().values().stream()
                    .flatMap(Collection::stream)
                    .collect(onlyElement());
        }
    }

    private static CassandraTlsConfig tlsConfig()
            throws URISyntaxException
    {
        return new CassandraTlsConfig().setTruststorePath(resource("ca.crt"));
    }

    private static SSLEngine createSslEngine(CassandraTlsConfig config)
            throws Exception
    {
        try (SslEngineFactory sslEngineFactory = CassandraTlsEngineFactory.createSslEngineFactory(config, List.of("cassandra.example.com"))) {
            return sslEngineFactory.newSslEngine(new DefaultEndPoint(InetSocketAddress.createUnresolved("cassandra.example.com", 9042)));
        }
    }

    private static File resource(String name)
            throws URISyntaxException
    {
        return new File(getResource("cassandra-tls/" + name).toURI());
    }
}
