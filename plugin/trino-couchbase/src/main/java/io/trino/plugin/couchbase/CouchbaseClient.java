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
package io.trino.plugin.couchbase;

import com.couchbase.client.core.env.Authenticator;
import com.couchbase.client.core.env.CertificateAuthenticator;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Scope;
import io.airlift.security.pem.PemReader;
import jakarta.inject.Inject;

import java.io.File;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CouchbaseClient
{
    private final CouchbaseConfig config;
    private final Cluster cluster;

    @Inject
    public CouchbaseClient(CouchbaseConfig config)
    {
        this.config = config;
        try {
            if (config.getTlsKey() != null) {
                PrivateKey key;
                Optional<String> password = Optional.ofNullable(config.getTlsKeyPassword());
                List<X509Certificate> keyCertChain = new ArrayList<>();
                if (new File(config.getTlsKey()).exists()) {
                    // load from file
                    key = PemReader.loadPrivateKey(new File(config.getTlsKey()), password);
                }
                else {
                    // try loading from string
                    key = PemReader.loadPrivateKey(config.getTlsKey(), password);
                }
                if (config.getTlsCertificate() != null) {
                    KeyStore tlsKeyStore = PemReader.loadTrustStore(new File(config.getTlsCertificate()));
                    tlsKeyStore.aliases().asIterator().forEachRemaining(alias -> {
                        try {
                            for (Certificate cert : tlsKeyStore.getCertificateChain(alias)) {
                                if (cert instanceof X509Certificate) {
                                    keyCertChain.add((X509Certificate) cert);
                                }
                            }
                        }
                        catch (KeyStoreException e) {
                            throw new RuntimeException("Failed to load TLS certificates", e);
                        }
                    });
                }
                Authenticator authenticator = CertificateAuthenticator.fromKey(
                        key, password.orElse(""), keyCertChain);
                cluster = Cluster.connect(
                        config.getCluster(),
                        ClusterOptions.clusterOptions(authenticator)
                                .environment(env -> {
                                    env.securityConfig(security -> {
                                        if (config.getTlsCertificate() != null) {
                                            security.trustCertificate(Path.of(config.getTlsCertificate()));
                                        }
                                    });
                                    env.timeoutConfig(timeout -> {
                                        timeout.kvTimeout(config.getTimeouts());
                                        timeout.queryTimeout(config.getTimeouts());
                                    });
                                }));
            }
            else {
                cluster = Cluster.connect(config.getCluster(), config.getUsername(), config.getPassword());
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to instantiate Couchbase client", e);
        }
    }

    public Bucket getBucket()
    {
        return cluster.bucket(config.getBucket());
    }

    public Scope getScope()
    {
        return getBucket().scope(config.getScope());
    }
}
