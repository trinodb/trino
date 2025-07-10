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

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.junit5.StartStop;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestInsecureQueryRunner
{
    @StartStop
    private final MockWebServer server = new MockWebServer();

    @BeforeEach
    public void setup()
            throws Exception
    {
        SSLContext sslContext = buildTestSslContext();
        server.useHttps(sslContext.getSocketFactory());
    }

    @Test
    public void testInsecureConnection()
            throws Exception
    {
        server.enqueue(new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(createResults(server))
                .build());
        server.enqueue(new MockResponse.Builder()
                .addHeader(CONTENT_TYPE, "application/json")
                .body(createResults(server))
                .build());

        QueryRunner queryRunner = createQueryRunner(createTrinoUri(server, true), createClientSession(server));

        try (Query query = queryRunner.startQuery("query with insecure mode")) {
            query.renderOutput(getTerminal(), nullPrintStream(), nullPrintStream(), CSV, Optional.of(""), false, false);
        }

        assertThat(server.takeRequest().getUrl().encodedPath()).isEqualTo("/v1/statement");
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
