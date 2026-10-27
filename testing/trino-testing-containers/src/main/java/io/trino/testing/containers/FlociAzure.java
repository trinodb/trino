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
package io.trino.testing.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public final class FlociAzure
        extends GenericContainer<FlociAzure>
{
    // Port, account, and key match Floci-AZ's documented Azurite-compatible defaults.
    // See https://floci.io/floci-az/getting-started/quick-start/
    public static final String FLOCI_AZURE_IMAGE = "floci/floci-az:0.15.0";
    public static final String FLOCI_AZURE_HIERARCHICAL_ACCOUNT = "devstoreaccount1";
    public static final String FLOCI_AZURE_FLAT_ACCOUNT = "flataccount";
    public static final String FLOCI_AZURE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    public static final String FLOCI_AZURE_TENANT_ID = "00000000-0000-0000-0000-000000000002";
    public static final String FLOCI_AZURE_CLIENT_ID = "11111111-1111-1111-1111-111111111111";
    public static final String FLOCI_AZURE_CLIENT_SECRET = "floci-az-dev-secret";

    private static final int FLOCI_AZURE_PORT = 4577;

    public FlociAzure()
    {
        super(DockerImageName.parse(FLOCI_AZURE_IMAGE));
        withEnv("FLOCI_AZ_TLS_ENABLED", "true");
        withEnv("FLOCI_AZ_TLS_HTTPS_PORT", "0");
        addExposedPort(FLOCI_AZURE_PORT);
        waitingFor(Wait.forHttp("/health").forPort(FLOCI_AZURE_PORT));
    }

    public URI getEndpoint()
    {
        return URI.create("https://%s:%s".formatted(getHost(), getMappedPort(FLOCI_AZURE_PORT)));
    }

    public X509Certificate getTlsCertificate()
            throws IOException, CertificateException
    {
        URI certificateEndpoint = URI.create("http://%s:%s/_floci/tls-cert".formatted(getHost(), getMappedPort(FLOCI_AZURE_PORT)));
        HttpURLConnection connection = (HttpURLConnection) certificateEndpoint.toURL().openConnection();
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(5_000);
        try {
            int responseCode = connection.getResponseCode();
            if (responseCode != HttpURLConnection.HTTP_OK) {
                throw new IOException("Floci Azure TLS certificate endpoint returned HTTP " + responseCode);
            }
            try (InputStream input = connection.getInputStream()) {
                return (X509Certificate) CertificateFactory.getInstance("X.509")
                        .generateCertificate(new ByteArrayInputStream(input.readAllBytes()));
            }
        }
        finally {
            connection.disconnect();
        }
    }
}
