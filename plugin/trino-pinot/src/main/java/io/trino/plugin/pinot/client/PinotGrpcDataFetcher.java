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
package io.trino.plugin.pinot.client;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import io.trino.plugin.pinot.PinotErrorCode;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.PinotSplit;
import io.trino.plugin.pinot.query.PinotProxyGrpcRequestBuilder;
import io.trino.spi.connector.ConnectorSession;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response.MetadataKeys;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response.ResponseType;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Boolean.FALSE;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.config.GrpcConfig.CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE;
import static org.apache.pinot.common.config.GrpcConfig.CONFIG_USE_PLAIN_TEXT;
import static org.apache.pinot.common.config.GrpcConfig.GRPC_TLS_PREFIX;

public class PinotGrpcDataFetcher
        implements PinotDataFetcher
{
    private final PinotSplit split;
    private final PinotGrpcServerQueryClient pinotGrpcClient;
    private final String query;
    private long readTimeNanos;
    private Iterator<PinotDataTableWithSize> responseIterator;
    private boolean isPinotDataFetched;
    private final RowCountChecker rowCountChecker;
    private long estimatedMemoryUsageInBytes;

    public PinotGrpcDataFetcher(PinotGrpcServerQueryClient pinotGrpcClient, PinotSplit split, String query, RowCountChecker rowCountChecker)
    {
        this.pinotGrpcClient = requireNonNull(pinotGrpcClient, "pinotGrpcClient is null");
        this.split = requireNonNull(split, "split is null");
        this.query = requireNonNull(query, "query is null");
        this.rowCountChecker = requireNonNull(rowCountChecker, "rowCountChecker is null");
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getMemoryUsageBytes()
    {
        return estimatedMemoryUsageInBytes;
    }

    @Override
    public boolean endOfData()
    {
        return !responseIterator.hasNext();
    }

    @Override
    public boolean isDataFetched()
    {
        return isPinotDataFetched;
    }

    @Override
    public void fetchData()
    {
        long startTimeNanos = System.nanoTime();
        String serverHost = split.getSegmentHost().orElseThrow(() -> new PinotException(PinotErrorCode.PINOT_INVALID_PQL_GENERATED, Optional.empty(), "Expected the segment split to contain the host"));
        this.responseIterator = pinotGrpcClient.queryPinot(null, query, serverHost, split.getSegments());
        readTimeNanos += System.nanoTime() - startTimeNanos;
        isPinotDataFetched = true;
    }

    @Override
    public PinotDataTableWithSize getNextDataTable()
    {
        PinotDataTableWithSize dataTableWithSize = responseIterator.next();
        estimatedMemoryUsageInBytes = dataTableWithSize.getEstimatedSizeInBytes();
        rowCountChecker.checkTooManyRows(dataTableWithSize.getDataTable());
        checkExceptions(dataTableWithSize.getDataTable(), split, query);
        return dataTableWithSize;
    }

    public static class Factory
            implements PinotDataFetcher.Factory
    {
        private final PinotGrpcServerQueryClient queryClient;
        private final int limitForSegmentQueries;
        private final Closer closer = Closer.create();

        @Inject
        public Factory(PinotHostMapper pinotHostMapper, PinotGrpcServerQueryClientConfig pinotGrpcServerQueryClientConfig, GrpcQueryClientFactory grpcQueryClientFactory)
        {
            requireNonNull(pinotHostMapper, "pinotHostMapper is null");
            this.limitForSegmentQueries = pinotGrpcServerQueryClientConfig.getMaxRowsPerSplitForSegmentQueries();
            this.queryClient = new PinotGrpcServerQueryClient(pinotHostMapper, pinotGrpcServerQueryClientConfig, grpcQueryClientFactory, closer);
        }

        @PreDestroy
        public void shutdown()
                throws IOException
        {
            closer.close();
        }

        @Override
        public PinotDataFetcher create(ConnectorSession session, String query, PinotSplit split)
        {
            return new PinotGrpcDataFetcher(queryClient, split, query, new RowCountChecker(limitForSegmentQueries, query));
        }

        @Override
        public int getRowLimit()
        {
            return limitForSegmentQueries;
        }
    }

    public interface GrpcQueryClientFactory
    {
        GrpcQueryClient create(HostAndPort hostAndPort);
    }

    public static class PlainTextGrpcQueryClientFactory
            implements GrpcQueryClientFactory
    {
        private final GrpcConfig config;

        @Inject
        public PlainTextGrpcQueryClientFactory(PinotGrpcServerQueryClientConfig grpcClientConfig)
        {
            requireNonNull(grpcClientConfig, "grpcClientConfig is null");
            this.config = new GrpcConfig(ImmutableMap.<String, Object>builder()
                    .put(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, String.valueOf(grpcClientConfig.getMaxInboundMessageSize().toBytes()))
                    .put(CONFIG_USE_PLAIN_TEXT, String.valueOf(grpcClientConfig.isUsePlainText()))
                    .buildOrThrow());
        }

        @Override
        public GrpcQueryClient create(HostAndPort hostAndPort)
        {
            return new GrpcQueryClient(hostAndPort.getHost(), hostAndPort.getPort(), config);
        }
    }

    public static class TlsGrpcQueryClientFactory
            implements GrpcQueryClientFactory
    {
        // Extracted from org.apache.pinot.common.utils.TlsUtils
        private static final String KEYSTORE_TYPE = GRPC_TLS_PREFIX + "." + "keystore.type";
        private static final String KEYSTORE_PATH = GRPC_TLS_PREFIX + "." + "keystore.path";
        private static final String KEYSTORE_PASSWORD = GRPC_TLS_PREFIX + "." + "keystore.password";
        private static final String TRUSTSTORE_TYPE = GRPC_TLS_PREFIX + "." + "truststore.type";
        private static final String TRUSTSTORE_PATH = GRPC_TLS_PREFIX + "." + "truststore.path";
        private static final String TRUSTSTORE_PASSWORD = GRPC_TLS_PREFIX + "." + "truststore.password";
        private static final String SSL_PROVIDER = GRPC_TLS_PREFIX + "." + "ssl.provider";

        private final GrpcConfig config;

        @Inject
        public TlsGrpcQueryClientFactory(PinotGrpcServerQueryClientConfig grpcClientConfig, PinotGrpcServerQueryClientTlsConfig tlsConfig)
        {
            requireNonNull(grpcClientConfig, "grpcClientConfig is null");
            requireNonNull(tlsConfig, "tlsConfig is null");
            ImmutableMap.Builder<String, Object> tlsConfigBuilder = ImmutableMap.<String, Object>builder()
                    .put(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, String.valueOf(grpcClientConfig.getMaxInboundMessageSize().toBytes()))
                    .put(CONFIG_USE_PLAIN_TEXT, FALSE.toString());

            if (tlsConfig.getKeystorePath().isPresent()) {
                tlsConfigBuilder.put(KEYSTORE_TYPE, tlsConfig.getKeystoreType());
                tlsConfigBuilder.put(KEYSTORE_PATH, tlsConfig.getKeystorePath().get());
                tlsConfig.getKeystorePassword().ifPresent(password -> tlsConfigBuilder.put(KEYSTORE_PASSWORD, password));
            }
            if (tlsConfig.getTruststorePath().isPresent()) {
                tlsConfigBuilder.put(TRUSTSTORE_TYPE, tlsConfig.getTruststoreType());
                tlsConfigBuilder.put(TRUSTSTORE_PATH, tlsConfig.getTruststorePath().get());
                tlsConfig.getTruststorePassword().ifPresent(password -> tlsConfigBuilder.put(TRUSTSTORE_PASSWORD, password));
            }
            tlsConfigBuilder.put(SSL_PROVIDER, tlsConfig.getSslProvider());

            this.config = new GrpcConfig(tlsConfigBuilder.buildOrThrow());
        }

        @Override
        public GrpcQueryClient create(HostAndPort hostAndPort)
        {
            return new GrpcQueryClient(hostAndPort.getHost(), hostAndPort.getPort(), config);
        }
    }

    public static class PinotGrpcServerQueryClient
    {
        private final PinotHostMapper pinotHostMapper;
        private final Map<HostAndPort, GrpcQueryClient> clientCache = new ConcurrentHashMap<>();
        private final int grpcPort;
        private final GrpcQueryClientFactory grpcQueryClientFactory;
        private final Optional<String> proxyUri;
        private final Closer closer;

        private PinotGrpcServerQueryClient(PinotHostMapper pinotHostMapper, PinotGrpcServerQueryClientConfig pinotGrpcServerQueryClientConfig, GrpcQueryClientFactory grpcQueryClientFactory, Closer closer)
        {
            this.pinotHostMapper = requireNonNull(pinotHostMapper, "pinotHostMapper is null");
            requireNonNull(pinotGrpcServerQueryClientConfig, "pinotGrpcServerQueryClientConfig is null");
            this.grpcPort = pinotGrpcServerQueryClientConfig.getGrpcPort();
            this.grpcQueryClientFactory = requireNonNull(grpcQueryClientFactory, "grpcQueryClientFactory is null");
            this.closer = requireNonNull(closer, "closer is null");
            this.proxyUri = pinotGrpcServerQueryClientConfig.getProxyUri();
        }

        public Iterator<PinotDataTableWithSize> queryPinot(ConnectorSession session, String query, String serverHost, List<String> segments)
        {
            HostAndPort mappedHostAndPort = pinotHostMapper.getServerGrpcHostAndPort(serverHost, grpcPort);
            // GrpcQueryClient does not implement Closeable. The idle timeout is 30 minutes (grpc default).
            GrpcQueryClient client = clientCache.computeIfAbsent(mappedHostAndPort, hostAndPort -> {
                GrpcQueryClient queryClient = proxyUri.isPresent() ? grpcQueryClientFactory.create(HostAndPort.fromString(proxyUri.get())) : grpcQueryClientFactory.create(hostAndPort);
                closer.register(queryClient::close);
                return queryClient;
            });
            PinotProxyGrpcRequestBuilder grpcRequestBuilder = new PinotProxyGrpcRequestBuilder()
                    .setSql(query)
                    .setSegments(segments)
                    .setEnableStreaming(true);

            if (proxyUri.isPresent()) {
                grpcRequestBuilder.setHostName(mappedHostAndPort.getHost()).setPort(grpcPort);
            }
            Server.ServerRequest serverRequest = grpcRequestBuilder.build();
            return new ResponseIterator(client.submit(serverRequest));
        }

        public static class ResponseIterator
                extends AbstractIterator<PinotDataTableWithSize>
        {
            private final Iterator<Server.ServerResponse> responseIterator;

            public ResponseIterator(Iterator<Server.ServerResponse> responseIterator)
            {
                this.responseIterator = requireNonNull(responseIterator, "responseIterator is null");
            }

            @Override
            protected PinotDataTableWithSize computeNext()
            {
                if (!responseIterator.hasNext()) {
                    return endOfData();
                }
                Server.ServerResponse response = responseIterator.next();
                String responseType = response.getMetadataMap().get(MetadataKeys.RESPONSE_TYPE);
                if (responseType.equals(ResponseType.METADATA)) {
                    return endOfData();
                }
                ByteBuffer buffer = response.getPayload().asReadOnlyByteBuffer();
                try {
                    return new PinotDataTableWithSize(DataTableFactory.getDataTable(buffer), buffer.remaining());
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }
}
