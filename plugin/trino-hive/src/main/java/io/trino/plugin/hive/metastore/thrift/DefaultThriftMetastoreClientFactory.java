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

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient.TransportSupplier;
import io.trino.spi.NodeManager;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static java.lang.Math.toIntExact;
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
            String hostname)
    {
        this.sslContext = requireNonNull(sslContext, "sslContext is null");
        this.socksProxy = requireNonNull(socksProxy, "socksProxy is null");
        this.connectTimeoutMillis = toIntExact(connectTimeout.toMillis());
        this.readTimeoutMillis = toIntExact(readTimeout.toMillis());
        this.metastoreAuthentication = requireNonNull(metastoreAuthentication, "metastoreAuthentication is null");
        this.hostname = requireNonNull(hostname, "hostname is null");
    }

    @Inject
    public DefaultThriftMetastoreClientFactory(
            ThriftMetastoreConfig config,
            HiveMetastoreAuthentication metastoreAuthentication,
            NodeManager nodeManager)
    {
        this(
                buildSslContext(
                        config.isTlsEnabled(),
                        Optional.ofNullable(config.getKeystorePath()),
                        Optional.ofNullable(config.getKeystorePassword()),
                        Optional.ofNullable(config.getTruststorePath()),
                        Optional.ofNullable(config.getTruststorePassword())),
                Optional.ofNullable(config.getSocksProxy()),
                config.getConnectTimeout(),
                config.getReadTimeout(),
                metastoreAuthentication,
                nodeManager.getCurrentNode().getHost());
    }

    @Override
    public ThriftMetastoreClient create(HostAndPort address, Optional<String> delegationToken)
            throws TTransportException
    {
        return create(() -> createTransport(address, delegationToken), hostname);
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
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (!tlsEnabled) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
