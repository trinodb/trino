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
package io.prestosql.plugin.mongodb;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import io.airlift.security.pem.PemReader;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Singleton;
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
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.mongodb.MongoErrorCode.MONGO_SSL_INITIALIZATION_FAILURE;
import static java.util.Collections.list;
import static java.util.Objects.requireNonNull;

public class MongoClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MongoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MongoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSinkProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MongoClientConfig.class);
    }

    @Singleton
    @Provides
    public static MongoSession createMongoSession(TypeManager typeManager, MongoClientConfig config)
    {
        requireNonNull(config, "config is null");

        MongoClientOptions.Builder options = MongoClientOptions.builder();

        options.connectionsPerHost(config.getConnectionsPerHost())
                .connectTimeout(config.getConnectionTimeout())
                .socketTimeout(config.getSocketTimeout())
                .socketKeepAlive(config.getSocketKeepAlive())
                .sslEnabled(config.getTlsEnabled())
                .maxWaitTime(config.getMaxWaitTime())
                .maxConnectionIdleTime(config.getMaxConnectionIdleTime())
                .minConnectionsPerHost(config.getMinConnectionsPerHost())
                .readPreference(config.getReadPreference().getReadPreference())
                .writeConcern(config.getWriteConcern().getWriteConcern());

        if (config.getRequiredReplicaSetName() != null) {
            options.requiredReplicaSetName(config.getRequiredReplicaSetName());
        }
        if (config.getTlsEnabled()) {
            buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword())
                    .ifPresent(options::sslContext);
        }

        MongoClient client = new MongoClient(config.getSeeds(), config.getCredentials(), options.build());

        return new MongoSession(
                typeManager,
                client,
                config);
    }

    private static Optional<SSLContext> buildSslContext(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        if (keystorePath.isEmpty() && truststorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            // load KeyStore if configured and get KeyManagers
            KeyStore keystore = null;
            KeyManager[] keyManagers = null;
            if (keystorePath.isPresent()) {
                char[] keyManagerPassword;
                try {
                    // attempt to read the key store as a PEM file
                    keystore = PemReader.loadKeyStore(keystorePath.get(), keystorePath.get(), keystorePassword);
                    // for PEM encoded keys, the password is used to decrypt the specific key (and does not protect the keystore itself)
                    keyManagerPassword = new char[0];
                }
                catch (IOException | GeneralSecurityException ignored) {
                    keyManagerPassword = keystorePassword.map(String::toCharArray).orElse(null);

                    keystore = KeyStore.getInstance(KeyStore.getDefaultType());
                    try (InputStream in = new FileInputStream(keystorePath.get())) {
                        keystore.load(in, keyManagerPassword);
                    }
                }
                validateCertificates(keystore);
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keystore, keyManagerPassword);
                keyManagers = keyManagerFactory.getKeyManagers();
            }

            // load TrustStore if configured, otherwise use KeyStore
            KeyStore truststore = keystore;
            if (truststorePath.isPresent()) {
                truststore = loadTrustStore(truststorePath.get(), truststorePassword);
            }

            // create TrustManagerFactory
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(truststore);

            // get X509TrustManager
            TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
            if (trustManagers.length != 1 || !(trustManagers[0] instanceof X509TrustManager)) {
                throw new RuntimeException("Unexpected default trust managers:" + Arrays.toString(trustManagers));
            }
            // create SSLContext
            SSLContext result = SSLContext.getInstance("SSL");
            result.init(keyManagers, trustManagers, null);
            return Optional.of(result);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new PrestoException(MONGO_SSL_INITIALIZATION_FAILURE, e);
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
        catch (IOException | GeneralSecurityException ignored) {
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
}
