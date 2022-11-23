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
package io.trino.server;

import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.trino.server.testing.TestingTrinoServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.failuredetector.HeartbeatFailureDetector.Stats;
import static org.testng.Assert.assertTrue;

public class TestNodeResource
{
    private TestingTrinoServer server;
    private HttpClient client;

    @BeforeClass
    public void setup()
    {
        server = TestingTrinoServer.create();
        client = new JettyHttpClient();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        closeAll(server, client);
        server = null;
        client = null;
    }

    @Test
    public void testGetAllNodes()
    {
        List<Stats> nodes = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/node"))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(Stats.class)));

        // we only have one node and the list never contains the current node
        assertTrue(nodes.isEmpty());
    }

    @Test
    public void testGetFailedNodes()
    {
        List<Stats> nodes = client.execute(
                prepareGet()
                        .setUri(server.resolve("/v1/node/failed"))
                        .setHeader(TRINO_HEADERS.requestUser(), "unknown")
                        .build(),
                createJsonResponseHandler(listJsonCodec(Stats.class)));

        assertTrue(nodes.isEmpty());
    }
}
