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
package io.trino.tests.product.tls;

import io.trino.tests.product.tls.TlsEnvironment.NodeEndpoint;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseTestTls
{
    @Test
    void testHttpPortIsClosed(TlsEnvironment env)
            throws Exception
    {
        List<String> activeNodeIds = new ArrayList<>();
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement();
                ResultSet nodes = statement.executeQuery("SELECT node_id FROM system.runtime.nodes WHERE state = 'active'")) {
            while (nodes.next()) {
                activeNodeIds.add(nodes.getString(1));
            }

            assertThat(activeNodeIds).containsExactlyInAnyOrder(
                    "trino-coordinator",
                    "trino-worker-1",
                    "trino-worker-2");

            try (ResultSet result = statement.executeQuery("SELECT count(*) FROM tpch.tiny.lineitem")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getLong(1)).isEqualTo(60_175);
                assertThat(result.next()).isFalse();
            }
        }

        for (NodeEndpoint node : env.getNodes()) {
            assertThat(isPortOpen(node.host(), node.httpsPort()))
                    .as("HTTPS port for %s", node.nodeId())
                    .isTrue();
            assertThat(isHttpServerAvailable(node.host(), node.httpPort()))
                    .as("HTTP port for %s", node.nodeId())
                    .isFalse();
        }
    }

    private static boolean isPortOpen(String host, int port)
            throws IOException
    {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1_000);
            return true;
        }
        catch (ConnectException | SocketTimeoutException e) {
            return false;
        }
    }

    private static boolean isHttpServerAvailable(String host, int port)
            throws IOException
    {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1_000);
            socket.setSoTimeout(1_000);
            socket.getOutputStream().write("GET /v1/info HTTP/1.0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
            return socket.getInputStream().read() >= 0;
        }
        catch (SocketException | SocketTimeoutException e) {
            // A mapped Docker port can accept before discovering that no process is listening in the container.
            return false;
        }
    }
}
