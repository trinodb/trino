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

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Base64;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.Objects.requireNonNull;

/**
 * Generates a self-signed TLS certificate suitable for testing HTTPS servers
 * such as {@link MinioTls}. The resulting directory contains the PEM-encoded
 * certificate and private key (which can be mounted into a container), and a
 * PKCS#12 trust store usable by the JVM's default SSL context.
 */
public final class TlsCertificate
        implements AutoCloseable
{
    private static final String DEFAULT_ALIAS = "trino-testing";
    private static final String DEFAULT_PASSWORD = "changeit";
    private static final String PUBLIC_CERTIFICATE_FILE = "public.crt";
    private static final String PRIVATE_KEY_FILE = "private.key";
    private static final String TRUST_STORE_FILE = "truststore.p12";

    private final Path directory;
    private final String password;
    private final boolean ownsDirectory;
    private PreviousTrustStoreSettings previousTrustStore;
    private SSLContext previousDefaultSslContext;

    private TlsCertificate(Path directory, String password, boolean ownsDirectory)
    {
        this.directory = requireNonNull(directory, "directory is null");
        this.password = requireNonNull(password, "password is null");
        this.ownsDirectory = ownsDirectory;
    }

    /**
     * Generate a new self-signed certificate valid for {@code localhost}/127.0.0.1
     * into a fresh temporary directory that is deleted on {@link #close()}.
     */
    public static TlsCertificate generate()
            throws Exception
    {
        Path directory = Files.createTempDirectory("trino-tls");
        try {
            generateInternal(directory, DEFAULT_ALIAS, DEFAULT_PASSWORD);
        }
        catch (Exception | Error e) {
            MoreFiles.deleteRecursively(directory, RecursiveDeleteOption.ALLOW_INSECURE);
            throw e;
        }
        return new TlsCertificate(directory, DEFAULT_PASSWORD, true);
    }

    public Path publicCertificate()
    {
        return directory.resolve(PUBLIC_CERTIFICATE_FILE);
    }

    public Path privateKey()
    {
        return directory.resolve(PRIVATE_KEY_FILE);
    }

    public Path trustStore()
    {
        return directory.resolve(TRUST_STORE_FILE);
    }

    public String trustStorePassword()
    {
        return password;
    }

    /**
     * Install this certificate as the JVM default trust store so that HTTPS
     * connections to a server presenting it succeed. Any previous values are
     * captured and automatically restored by {@link #close()}, preventing the
     * JVM-wide trust store configuration from leaking into subsequent tests.
     *
     * <p>In addition to setting the {@code javax.net.ssl.trustStore} system
     * properties (which are only consulted on first SSL initialization), this
     * also installs a freshly-built {@link SSLContext} as the JVM default via
     * {@link SSLContext#setDefault(SSLContext)}. Without that override, an
     * earlier test in a forked-but-reused JVM (Surefire {@code reuseForks=true})
     * may have already locked in the original JRE {@code cacerts} as the
     * default {@code SSLContext}, causing later HTTPS clients (e.g. AWS SDK
     * v2 Apache HTTP client) to fail PKIX validation against this self-signed
     * certificate.
     */
    public void installAsDefaultTrustStore()
    {
        if (previousTrustStore == null) {
            previousTrustStore = PreviousTrustStoreSettings.capture();
        }

        // Capture the existing default SSLContext BEFORE mutating system
        // properties. If nothing has initialized SSL yet, this call lazily
        // initializes the default using the still-unmodified system
        // properties, so the captured context represents the JVM's pristine
        // defaults and can be safely restored on close().
        SSLContext previous;
        try {
            previous = SSLContext.getDefault();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to read existing default SSLContext", e);
        }
        if (previousDefaultSslContext == null) {
            previousDefaultSslContext = previous;
        }

        System.setProperty("javax.net.ssl.trustStore", trustStore().toString());
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword());
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");

        SSLContext.setDefault(buildSslContext());
    }

    @Override
    public void close()
            throws IOException
    {
        if (previousDefaultSslContext != null) {
            SSLContext.setDefault(previousDefaultSslContext);
            previousDefaultSslContext = null;
        }
        if (previousTrustStore != null) {
            previousTrustStore.restore();
            previousTrustStore = null;
        }
        if (ownsDirectory) {
            MoreFiles.deleteRecursively(directory, RecursiveDeleteOption.ALLOW_INSECURE);
        }
    }

    private SSLContext buildSslContext()
    {
        try {
            KeyStore trustStore = KeyStore.getInstance("PKCS12");
            try (InputStream in = Files.newInputStream(trustStore())) {
                trustStore.load(in, password.toCharArray());
            }
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustManagerFactory.getTrustManagers(), null);
            return context;
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to build SSLContext from " + trustStore(), e);
        }
    }

    private static void generateInternal(Path directory, String alias, String password)
            throws Exception
    {
        Path keyStorePath = directory.resolve("keystore.p12");

        Process process = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair",
                "-alias",
                alias,
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-validity",
                "1",
                "-keystore",
                keyStorePath.toString(),
                "-storetype",
                "PKCS12",
                "-storepass",
                password,
                "-keypass",
                password,
                "-dname",
                "CN=localhost",
                "-ext",
                "SAN=DNS:localhost,IP:127.0.0.1")
                .inheritIO()
                .start();
        int exitCode = process.waitFor();
        checkArgument(exitCode == 0, "keytool failed with exit code %s", exitCode);

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(keyStorePath)) {
            keyStore.load(in, password.toCharArray());
        }

        Certificate certificate = keyStore.getCertificate(alias);
        writePem(directory.resolve(PUBLIC_CERTIFICATE_FILE),
                "CERTIFICATE",
                certificate.getEncoded());

        PrivateKey privateKey = (PrivateKey) keyStore.getKey(alias, password.toCharArray());
        writePem(directory.resolve(PRIVATE_KEY_FILE),
                "PRIVATE KEY",
                privateKey.getEncoded());

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry(alias, certificate);
        try (OutputStream out = Files.newOutputStream(directory.resolve(TRUST_STORE_FILE), CREATE_NEW)) {
            trustStore.store(out, password.toCharArray());
        }
    }

    private static void writePem(Path target, String type, byte[] contents)
            throws IOException
    {
        String pem = "-----BEGIN " + type + "-----\n" +
                Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(contents) +
                "\n-----END " + type + "-----\n";
        Files.writeString(target, pem, CREATE_NEW);
    }

    private record PreviousTrustStoreSettings(String trustStore, String password, String type)
    {
        private static PreviousTrustStoreSettings capture()
        {
            return new PreviousTrustStoreSettings(
                    System.getProperty("javax.net.ssl.trustStore"),
                    System.getProperty("javax.net.ssl.trustStorePassword"),
                    System.getProperty("javax.net.ssl.trustStoreType"));
        }

        void restore()
        {
            restoreProperty("javax.net.ssl.trustStore", trustStore);
            restoreProperty("javax.net.ssl.trustStorePassword", password);
            restoreProperty("javax.net.ssl.trustStoreType", type);
        }

        private static void restoreProperty(String key, String value)
        {
            if (value == null) {
                System.clearProperty(key);
            }
            else {
                System.setProperty(key, value);
            }
        }
    }
}
