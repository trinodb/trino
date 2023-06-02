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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.security.pem.PemReader;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient.TransportSupplier;
import io.trino.spi.NodeManager;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Collections.list;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class DefaultThriftMetastoreClientFactory
        implements ThriftMetastoreClientFactory
{
    private final Optional<SSLContext> sslContext;
    private final Optional<HostAndPort> socksProxy;
    private final int connectTimeoutMillis;
    private final int readTimeoutMillis;
    private final HiveMetastoreAuthentication metastoreAuthentication;
    private final String hostname;
    private final Optional<ThriftHttpContext> thriftHttpContext;

    private final MetastoreSupportsDateStatistics metastoreSupportsDateStatistics = new MetastoreSupportsDateStatistics();
    private final AtomicInteger chosenGetTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenTableParamAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllViewsPerDatabaseAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterTransactionalTableAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenAlterPartitionsAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllTablesAlternative = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger chosenGetAllViewsAlternative = new AtomicInteger(Integer.MAX_VALUE);

    public DefaultThriftMetastoreClientFactory(
            Optional<SSLContext> sslContext,
            Optional<HostAndPort> socksProxy,
            Duration connectTimeout,
            Duration readTimeout,
            HiveMetastoreAuthentication metastoreAuthentication,
            String hostname,
            Optional<ThriftHttpContext> thriftHttpContext)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.connectTimeoutMillis = toIntExact(connectTimeout.toMillis());
        this.readTimeoutMillis = toIntExact(readTimeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
        this.thriftHttpContext = requireNonNull(thriftHttpContext, "thriftHttpContext is null");
    }

    @Inject
    public DefaultThriftMetastoreClientFactory(
            ThriftMetastoreConfig config,
            ThriftHttpMetastoreConfig httpMetastoreConfig,
            HiveMetastoreAuthentication metastoreAuthentication,
            NodeManager nodeManager)
    {
        this(
                buildSslContext(
                        config.isTlsEnabled(),
                        Optional.ofNullable(config.getKeystorePath()),
                        Optional.ofNullable(config.getKeystorePassword()),
                        config.getTruststorePath(),
                        Optional.ofNullable(config.getTruststorePassword())),
                Optional.ofNullable(config.getSocksProxy()),
                config.getConnectTimeout(),
                config.getReadTimeout(),
                metastoreAuthentication,
                nodeManager.getCurrentNode().getHost(),
                buildThriftHttpContext(httpMetastoreConfig));
    }

    @Override
    public ThriftMetastoreClient create(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        return create(() -> getTransportSupplier(uri, delegationToken), hostname);
    }

    private TTransport getTransportSupplier(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        switch (uri.getScheme().toLowerCase(ENGLISH)) {
            case "thrift" -> {
                return createTransport(HostAndPort.fromParts(uri.getHost(), uri.getPort()), delegationToken);
            }
            case "http", "https" -> {
                return createHttpTransport(uri, thriftHttpContext.orElseThrow(() -> new IllegalArgumentException("Thrift http context is not set")));
            }
            default -> throw new IllegalArgumentException("Invalid metastore uri scheme " + uri.getScheme());
        }
    }

    private TTransport createHttpTransport(URI uri, ThriftHttpContext httpThriftContext)
            throws TTransportException
    {
        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        if (sslContext.isPresent()) {
            checkArgument(uri.getScheme().toLowerCase(ENGLISH).equals("https"), "URI must be https when setting SSLContext");
            checkArgument(httpThriftContext.token.isPresent(), "'hive.metastore.http.client.bearer-token' must be set when using https URI");
            SSLConnectionSocketFactory socketFactory = new SSLConnectionSocketFactory(sslContext.get(), new DefaultHostnameVerifier(null));
            Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("https", socketFactory)
                    .build();
            httpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
            httpClientBuilder.addRequestInterceptorFirst((httpRequest, entityDetails, httpContext) -> {
                httpRequest.addHeader(HttpHeaders.AUTHORIZATION, "Bearer " + httpThriftContext.token.get());
            });
        }

        httpClientBuilder.addRequestInterceptorFirst((httpRequest, entityDetails, httpContext) -> {
            httpThriftContext.additionalHeaders().forEach(httpRequest::addHeader);
        });
        httpClientBuilder.setDefaultRequestConfig(RequestConfig.custom().setResponseTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS).build());
        return new THttpClient(uri.toString(), httpClientBuilder.build());
    }

    protected ThriftMetastoreClient create(TransportSupplier transportSupplier, String hostname)
            throws TTransportException
    {
        return new ThriftHiveMetastoreClient(
                transportSupplier,
                hostname,
                metastoreSupportsDateStatistics,
                chosenGetTableAlternative,
                chosenTableParamAlternative,
                chosenGetAllTablesAlternative,
                chosenGetAllViewsPerDatabaseAlternative,
                chosenGetAllViewsAlternative,
                chosenAlterTransactionalTableAlternative,
                chosenAlterPartitionsAlternative);
    }

    private TTransport createTransport(HostAndPort address, Optional<String> delegationToken)
            throws TTransportException
    {
        return Transport.create(address, sslContext, socksProxy, connectTimeoutMillis, readTimeoutMillis, metastoreAuthentication, delegationToken);
    }

    private static Optional<SSLContext> buildSslContext(
            boolean tlsEnabled,
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            File trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (!tlsEnabled) {
            return Optional.empty();
        }

        try {
            // load KeyStore if configured and get KeyManagers
            KeyManager[] keyManagers = null;
            char[] keyManagerPassword = new char[0];
            if (keyStorePath.isPresent()) {
                KeyStore keyStore;
                try {
                    keyStore = PemReader.loadKeyStore(keyStorePath.get(), keyStorePath.get(), keyStorePassword);
                }
                catch (IOException | GeneralSecurityException e) {
                    keyManagerPassword = keyStorePassword.map(String::toCharArray).orElse(null);
                    keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    try (InputStream in = new FileInputStream(keyStorePath.get())) {
                        keyStore.load(in, keyManagerPassword);
                    }
                }
                validateCertificates(keyStore);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, keyManagerPassword);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            // load TrustStore
            KeyStore trustStore = loadTrustStore(trustStorePath, trustStorePassword);

            // create TrustManagerFactory
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);

            // get X509TrustManager
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }

            // create SSLContext
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(keyManagers, trustManagers, null);
            return Optional.of(sslContext);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static KeyStore loadTrustStore(File trustStorePath, Optional<String> trustStorePassword)
            throws IOException, GeneralSecurityException
    {
        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            // attempt to read the trust store as a PEM file
            List<X509Certificate> certificateChain = PemReader.readCertificateChain(trustStorePath);
            if (!certificateChain.isEmpty()) {
                trustStore.load(null, null);
                for (X509Certificate certificate : certificateChain) {
                    X500Principal principal = certificate.getSubjectX500Principal();
                    trustStore.setCertificateEntry(principal.getName(), certificate);
                }
                return trustStore;
            }
        }
        catch (IOException | GeneralSecurityException e) {
        }

        try (InputStream in = new FileInputStream(trustStorePath)) {
            trustStore.load(in, trustStorePassword.map(String::toCharArray).orElse(null));
        }
        return trustStore;
    }

    private static void validateCertificates(KeyStore keyStore)
            throws GeneralSecurityException
    {
        for (String alias : list(keyStore.aliases())) {
            if (!keyStore.isKeyEntry(alias)) {
                continue;
            }
            Certificate certificate = keyStore.getCertificate(alias);
            if (!(certificate instanceof X509Certificate)) {
                continue;
            }

            try {
                ((X509Certificate) certificate).checkValidity();
            }
            catch (CertificateExpiredException e) {
                throw new CertificateExpiredException("KeyStore certificate is expired: " + e.getMessage());
            }
            catch (CertificateNotYetValidException e) {
                throw new CertificateNotYetValidException("KeyStore certificate is not yet valid: " + e.getMessage());
            }
        }
    }

    private record ThriftHttpContext(Optional<String> token, Map<String, String> additionalHeaders)
    {
        private ThriftHttpContext
        {
            requireNonNull(additionalHeaders, "additionalHeaders is null");
            additionalHeaders = ImmutableMap.copyOf(additionalHeaders);
        }
    }

    @VisibleForTesting
    public static Optional<ThriftHttpContext> buildThriftHttpContext(ThriftHttpMetastoreConfig config)
    {
        if (config.getAdditionalHeaders().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ThriftHttpContext(
                config.getHttpBearerToken(),
                config.getAdditionalHeaders()));
    }
}
