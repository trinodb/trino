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
package io.trino.plugin.cassandra.tls;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.google.common.net.InetAddresses;
import io.trino.spi.TrinoException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.cassandra.CassandraErrorCode.CASSANDRA_SSL_INITIALIZATION_FAILURE;
import static java.util.Locale.ENGLISH;

final class CassandraTlsEngineFactory
{
    private CassandraTlsEngineFactory() {}

    static SslEngineFactory createSslEngineFactory(CassandraTlsConfig config, List<String> contactPoints)
    {
        // The context is always created, so that enabling TLS never results in a plaintext connection.
        // Without a key store and a trust store it uses the default trust anchors of the JVM.
        SSLContext sslContext = buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTruststorePath(), config.getTruststorePassword());
        // CqlSessionBuilder.withSslContext cannot enable host name verification, so the factory is created explicitly
        if (config.isUseReverseDns()) {
            return new ProgrammaticSslEngineFactory(sslContext, null, config.isVerifyHostnames());
        }
        return new NoReverseDnsSslEngineFactory(sslContext, config.isVerifyHostnames(), contactPoints);
    }

    private static SSLContext buildSslContext(
            Optional<File> keystorePath,
            Optional<String> keystorePassword,
            Optional<File> truststorePath,
            Optional<String> truststorePassword)
    {
        try {
            return createSSLContext(keystorePath, keystorePassword, truststorePath, truststorePassword);
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(CASSANDRA_SSL_INITIALIZATION_FAILURE, e);
        }
    }

    /// Verify against the contact point host name or the IP address, never a reverse DNS lookup.
    /// Contact points are matched by configured name, as the JDK also stores reverse lookup results in an address.
    private static class NoReverseDnsSslEngineFactory
            implements SslEngineFactory
    {
        private final ProgrammaticSslEngineFactory delegate;
        private final Set<String> contactPointHostNames;

        private NoReverseDnsSslEngineFactory(SSLContext sslContext, boolean requireHostnameValidation, List<String> contactPoints)
        {
            this.delegate = new ProgrammaticSslEngineFactory(sslContext, null, requireHostnameValidation);
            this.contactPointHostNames = contactPoints.stream()
                    .filter(contactPoint -> !InetAddresses.isInetAddress(contactPoint))
                    .map(contactPoint -> contactPoint.toLowerCase(ENGLISH))
                    .collect(toImmutableSet());
        }

        @Override
        public SSLEngine newSslEngine(EndPoint remoteEndpoint)
        {
            if (remoteEndpoint.resolve() instanceof InetSocketAddress address && address.getAddress() != null && !isContactPointHostName(address)) {
                // An unresolved address returns its host string from getHostName(), without a lookup
                InetSocketAddress peer = InetSocketAddress.createUnresolved(address.getAddress().getHostAddress(), address.getPort());
                return delegate.newSslEngine(new EndPoint()
                {
                    @Override
                    public SocketAddress resolve()
                    {
                        return peer;
                    }

                    @Override
                    public String asMetricPrefix()
                    {
                        return remoteEndpoint.asMetricPrefix();
                    }
                });
            }
            return delegate.newSslEngine(remoteEndpoint);
        }

        private boolean isContactPointHostName(InetSocketAddress address)
        {
            return contactPointHostNames.contains(address.getHostString().toLowerCase(ENGLISH));
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
