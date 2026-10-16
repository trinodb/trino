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
import com.couchbase.client.java.env.ClusterEnvironment;
import io.airlift.security.pem.PemReader;
import io.trino.spi.TrinoException;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class CouchbaseClient
        implements Closeable
{
    private final CouchbaseConfig config;
    private final Cluster cluster;

    public CouchbaseClient(CouchbaseConfig config)
    {
        this.config = requireNonNull(config, "config is null");
        this.cluster = createCluster();
    }

    private Cluster createCluster()
    {
        try {
            ClusterOptions options = createClusterOptions();
            if (options == null) {
                return Cluster.connect(config.getCluster(), config.getUsername(), config.getPassword());
            }
            return Cluster.connect(config.getCluster(), options);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to instantiate Couchbase client", e);
        }
    }

    private ClusterOptions createClusterOptions()
    {
        if (config.getTlsKey() == null) {
            return null;
        }

        try {
            PrivateKey key;
            Optional<String> password = Optional.ofNullable(config.getTlsKeyPassword());
            if (new File(config.getTlsKey()).exists()) {
                // load from file
                key = PemReader.loadPrivateKey(new File(config.getTlsKey()), password);
            }
            else {
                // try loading from string
                key = PemReader.loadPrivateKey(config.getTlsKey(), password);
            }
            List<X509Certificate> keyCertChain = new ArrayList<>();
            if (config.getTlsCertificate() != null) {
                var tlsKeyStore = PemReader.loadTrustStore(new File(config.getTlsCertificate()));
                tlsKeyStore.aliases().asIterator().forEachRemaining(alias -> {
                    try {
                        for (Certificate cert : tlsKeyStore.getCertificateChain(alias)) {
                            if (cert instanceof X509Certificate x509Cert) {
                                keyCertChain.add(x509Cert);
                            }
                        }
                    }
                    catch (KeyStoreException e) {
                        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load TLS certificates", e);
                    }
                });
            }
            Authenticator authenticator = CertificateAuthenticator.fromKey(
                    key, password.orElse(""), keyCertChain);
            return clusterOptions(authenticator);
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load TLS key material", e);
        }
    }

    private ClusterOptions clusterOptions(Authenticator authenticator)
    {
        return ClusterOptions.clusterOptions(authenticator)
                .environment(this::configureEnvironment);
    }

    private void configureEnvironment(ClusterEnvironment.Builder env)
    {
        env.securityConfig(security -> {
            if (config.getTlsCertificate() != null) {
                security.trustCertificate(Path.of(config.getTlsCertificate()));
            }
        });
        env.timeoutConfig(timeout -> {
            timeout.kvTimeout(config.getTimeouts().toJavaTime());
            timeout.queryTimeout(config.getTimeouts().toJavaTime());
        });
    }

    public Bucket getBucket()
    {
        return cluster.bucket(config.getBucket());
    }

    public Scope getScope()
    {
        return getBucket().scope(config.getScope());
    }

    @Override
    public void close()
    {
        cluster.close();
    }
}
