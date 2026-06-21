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
package io.trino.plugin.exasol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.trino.plugin.jdbc.BaseJdbcConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class ExasolTlsCertificateFingerprintProvider
{
    private static final int DEFAULT_TLS_PORT = 8563;
    private static final int SOCKET_TIMEOUT_MILLIS = 10_000;

    private final HostAndPort hostAndPort;
    private final CertificateReader certificateReader;
    private volatile String fingerprint;

    @Inject
    public ExasolTlsCertificateFingerprintProvider(BaseJdbcConfig config)
    {
        this(extractHostAndPort(config.getConnectionUrl()), new SslCertificateReader());
    }

    @VisibleForTesting
    ExasolTlsCertificateFingerprintProvider(HostAndPort hostAndPort, CertificateReader certificateReader)
    {
        this.hostAndPort = requireNonNull(hostAndPort, "hostAndPort is null");
        this.certificateReader = requireNonNull(certificateReader, "certificateReader is null");
    }

    public String getCertificateFingerprint()
    {
        String currentFingerprint = fingerprint;
        if (currentFingerprint == null) {
            synchronized (this) {
                currentFingerprint = fingerprint;
                if (currentFingerprint == null) {
                    currentFingerprint = toSha256Fingerprint(certificateReader.read(hostAndPort.getHost(), hostAndPort.getPortOrDefault(DEFAULT_TLS_PORT)));
                    fingerprint = currentFingerprint;
                }
            }
        }
        return currentFingerprint;
    }

    @VisibleForTesting
    static HostAndPort extractHostAndPort(String jdbcUrl)
    {
        requireNonNull(jdbcUrl, "jdbcUrl is null");

        String normalizedUrl = jdbcUrl;
        if (normalizedUrl.startsWith("jdbc:exa-worker:")) {
            normalizedUrl = normalizedUrl.substring("jdbc:exa-worker:".length());
        }
        else if (normalizedUrl.startsWith("jdbc:exa:")) {
            normalizedUrl = normalizedUrl.substring("jdbc:exa:".length());
        }
        else {
            throw new IllegalArgumentException("Unsupported Exasol JDBC URL: " + jdbcUrl);
        }

        int parametersStart = normalizedUrl.indexOf(';');
        if (parametersStart >= 0) {
            normalizedUrl = normalizedUrl.substring(0, parametersStart);
        }

        int schemaSeparator = normalizedUrl.indexOf('/');
        if (schemaSeparator >= 0) {
            normalizedUrl = normalizedUrl.substring(0, schemaSeparator);
        }

        int clusterSeparator = normalizedUrl.indexOf(',');
        if (clusterSeparator >= 0) {
            normalizedUrl = normalizedUrl.substring(0, clusterSeparator);
        }

        int rangeSeparator = normalizedUrl.indexOf("..");
        if (rangeSeparator >= 0) {
            normalizedUrl = normalizedUrl.substring(0, rangeSeparator);
        }

        return HostAndPort.fromString(normalizedUrl).withDefaultPort(DEFAULT_TLS_PORT);
    }

    @VisibleForTesting
    static String toSha256Fingerprint(X509Certificate certificate)
    {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(String.format(Locale.ENGLISH, "%02X", value));
            }
            return hex.toString();
        }
        catch (GeneralSecurityException e) {
            throw new RuntimeException("Failed to calculate Exasol TLS certificate fingerprint", e);
        }
    }

    interface CertificateReader
    {
        X509Certificate read(String host, int port);
    }

    private static class SslCertificateReader
            implements CertificateReader
    {
        @Override
        public X509Certificate read(String host, int port)
        {
            try {
                SSLSocketFactory socketFactory = createTrustAllSslContext().getSocketFactory();
                try (SSLSocket socket = (SSLSocket) socketFactory.createSocket()) {
                    socket.connect(new InetSocketAddress(host, port), SOCKET_TIMEOUT_MILLIS);
                    socket.setSoTimeout(SOCKET_TIMEOUT_MILLIS);
                    socket.startHandshake();
                    Certificate certificate = socket.getSession().getPeerCertificates()[0];
                    return (X509Certificate) certificate;
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Failed to retrieve Exasol TLS certificate from %s:%s".formatted(host, port), e);
            }
        }

        private SSLContext createTrustAllSslContext()
                throws GeneralSecurityException
        {
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[] {new TrustAllX509TrustManager()}, null);
            return sslContext;
        }
    }

    private static class TrustAllX509TrustManager
            implements X509TrustManager
    {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public X509Certificate[] getAcceptedIssuers()
        {
            return new X509Certificate[0];
        }
    }
}
