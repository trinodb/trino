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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TTransport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URISyntaxException;
import java.security.cert.CertificateException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.google.common.io.Resources.getResource;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
final class TestTransport
{
    private static final int TIMEOUT_MILLIS = 10_000;

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-metastore-tls-%s"));

    @AfterAll
    void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    void testCertificateMatchingHost()
            throws Exception
    {
        try (SSLServerSocket serverSocket = startServer("localhost.pem");
                TTransport transport = connect(serverSocket.getLocalPort())) {
            assertThat(transport.isOpen()).isTrue();
        }
    }

    @Test
    void testCertificateForDifferentHost()
            throws Exception
    {
        try (SSLServerSocket serverSocket = startServer("differenthost.pem")) {
            assertThatThrownBy(() -> connect(serverSocket.getLocalPort()))
                    .hasMessageContaining("localhost:" + serverSocket.getLocalPort())
                    .rootCause()
                    .isInstanceOf(CertificateException.class)
                    .hasMessageContaining("No subject alternative DNS name matching localhost found");
        }
    }

    private SSLServerSocket startServer(String keyStore)
            throws Exception
    {
        SSLContext serverContext = createSSLContext(Optional.of(resource(keyStore)), Optional.empty(), Optional.empty(), Optional.empty());
        SSLServerSocket serverSocket = (SSLServerSocket) serverContext.getServerSocketFactory().createServerSocket(0, 1, InetAddress.getByName("localhost"));
        executor.submit(() -> {
            try (Socket socket = serverSocket.accept()) {
                ((SSLSocket) socket).startHandshake();
                // hold the connection open until the client closes it
                socket.getInputStream().read();
            }
            catch (IOException _) {
                // expected when the client rejects the certificate
            }
        });
        return serverSocket;
    }

    private static TTransport connect(int port)
            throws Exception
    {
        SSLContext clientContext = createSSLContext(Optional.empty(), Optional.empty(), Optional.of(resource("ca.crt")), Optional.empty());
        return Transport.create(
                HostAndPort.fromParts("localhost", port),
                Optional.of(clientContext),
                Optional.empty(),
                TIMEOUT_MILLIS,
                TIMEOUT_MILLIS,
                new NoHiveMetastoreAuthentication(),
                Optional.empty());
    }

    private static File resource(String name)
            throws URISyntaxException
    {
        return new File(getResource("metastore-tls/" + name).toURI());
    }
}
