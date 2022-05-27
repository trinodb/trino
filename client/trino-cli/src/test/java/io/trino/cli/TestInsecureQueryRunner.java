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

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Optional;

import static com.google.common.io.Resources.getResource;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.trino.cli.ClientOptions.OutputFormat.CSV;
import static io.trino.cli.TerminalUtils.getTerminal;
import static io.trino.cli.TestQueryRunner.createClientSession;
import static io.trino.cli.TestQueryRunner.createQueryRunner;
import static io.trino.cli.TestQueryRunner.createResults;
import static io.trino.cli.TestQueryRunner.createTrinoUri;
import static io.trino.cli.TestQueryRunner.nullPrintStream;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestInsecureQueryRunner
{
    private MockWebServer server;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        server = new MockWebServer();
        SSLContext sslContext = buildTestSslContext();
        server.useHttps(sslContext.getSocketFactory(), false);
        server.start();
    }

    @AfterMethod(alwaysRun = true)
    public void teardown()
            throws Exception
    {
        server.close();
        server = null;
    }

    @Test
    public void testInsecureConnection()
            throws Exception
    {
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));
        server.enqueue(new MockResponse()
                .addHeader(CONTENT_TYPE, "application/json")
                .setBody(createResults(server)));

        QueryRunner queryRunner = createQueryRunner(createTrinoUri(server, true), createClientSession(server));

        try (Query query = queryRunner.startQuery("query with insecure mode")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false);
        }

        assertEquals(server.takeRequest().getPath(), "/v1/statement");
    }

    private SSLContext buildTestSslContext()
            throws Exception
    {
        // Load self-signed certificate
        char[] serverKeyStorePassword = "insecure-ssl-test".toCharArray();
        KeyStore serverKeyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream in = getResource(getClass(), "/insecure-ssl-test.jks").openStream()) {
            serverKeyStore.load(in, serverKeyStorePassword);
        }

        String kmfAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(kmfAlgorithm);
        kmf.init(serverKeyStore, serverKeyStorePassword);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(kmfAlgorithm);
        trustManagerFactory.init(serverKeyStore);
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(kmf.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
        return sslContext;
    }
}
