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
import com.couchbase.client.core.env.PasswordAuthenticator;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.security.pem.PemReader;
import io.trino.spi.TrinoException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
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
            ClusterOptions options = ClusterOptions.clusterOptions(createAuthenticator(config))
                    .environment(this::configureEnvironment);
            return Cluster.connect(config.getCluster(), options);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to instantiate Couchbase client", e);
        }
    }

    @VisibleForTesting
    static Authenticator createAuthenticator(CouchbaseConfig config)
    {
        if (config.getTlsKey() == null) {
            return PasswordAuthenticator.create(config.getUsername(), config.getPassword());
        }

        try {
            Optional<String> keyPassword = Optional.ofNullable(config.getTlsKeyPassword());
            PrivateKey key;
            File keyFile = new File(config.getTlsKey());
            if (keyFile.exists()) {
                key = PemReader.loadPrivateKey(keyFile, keyPassword);
            }
            else {
                // the property holds the PEM-encoded key itself rather than a path to it
                key = PemReader.loadPrivateKey(config.getTlsKey(), keyPassword);
            }
            return CertificateAuthenticator.fromKey(key, keyPassword.orElse(""), loadClientCertificateChain(config));
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to load TLS key material", e);
        }
    }

    /**
     * Loads the chain presented to the cluster during mutual TLS. This is the client's own
     * certificate, not the cluster root certificate from {@code couchbase.tls-certificate},
     * which is only used as a trust anchor.
     */
    @VisibleForTesting
    static List<X509Certificate> loadClientCertificateChain(CouchbaseConfig config)
            throws IOException, GeneralSecurityException
    {
        if (config.getTlsClientCertificate() == null) {
            return ImmutableList.of();
        }
        return PemReader.readCertificateChain(new File(config.getTlsClientCertificate()));
    }

    private void configureEnvironment(ClusterEnvironment.Builder env)
    {
        env.securityConfig(security -> {
            // a trust anchor or a client key only makes sense over TLS, regardless of the connection string scheme
            if (config.getTlsCertificate() != null || config.getTlsKey() != null) {
                security.enableTls(true);
            }
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
