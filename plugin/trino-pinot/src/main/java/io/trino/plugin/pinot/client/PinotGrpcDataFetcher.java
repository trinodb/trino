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
import io.trino.spi.connector.ConnectorSession;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response.MetadataKeys;
import org.apache.pinot.spi.utils.CommonConstants.Query.Response.ResponseType;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

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

import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.utils.grpc.GrpcQueryClient.Config.CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE;
import static org.apache.pinot.common.utils.grpc.GrpcQueryClient.Config.CONFIG_USE_PLAIN_TEXT;
import static org.apache.pinot.common.utils.grpc.GrpcQueryClient.Config.GRPC_TLS_PREFIX;

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
            requireNonNull(pinotGrpcServerQueryClientConfig, "pinotGrpcServerQueryClientConfig is null");
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
        private final GrpcQueryClient.Config config;

        @Inject
        public PlainTextGrpcQueryClientFactory(PinotGrpcServerQueryClientConfig grpcClientConfig)
        {
            requireNonNull(grpcClientConfig, "grpcClientConfig is null");
            this.config = new GrpcQueryClient.Config(ImmutableMap.<String, Object>builder()
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
        private static final String KEYSTORE_TYPE = "keystore.type";
        private static final String KEYSTORE_PATH = "keystore.path";
        private static final String KEYSTORE_PASSWORD = "keystore.password";
        private static final String TRUSTSTORE_TYPE = "truststore.type";
        private static final String TRUSTSTORE_PATH = "truststore.path";
        private static final String TRUSTSTORE_PASSWORD = "truststore.password";
        private static final String SSL_PROVIDER = "ssl.provider";

        private final GrpcQueryClient.Config config;

        @Inject
        public TlsGrpcQueryClientFactory(PinotGrpcServerQueryClientConfig grpcClientConfig, PinotGrpcServerQueryClientTlsConfig tlsConfig)
        {
            requireNonNull(grpcClientConfig, "grpcClientConfig is null");
            requireNonNull(tlsConfig, "tlsConfig is null");
            this.config = new GrpcQueryClient.Config(ImmutableMap.<String, Object>builder()
                    .put(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, String.valueOf(grpcClientConfig.getMaxInboundMessageSize().toBytes()))
                    .put(CONFIG_USE_PLAIN_TEXT, String.valueOf(grpcClientConfig.isUsePlainText()))
                    .put(GRPC_TLS_PREFIX + "." + KEYSTORE_TYPE, tlsConfig.getKeystoreType())
                    .put(GRPC_TLS_PREFIX + "." + KEYSTORE_PATH, tlsConfig.getKeystorePath())
                    .put(GRPC_TLS_PREFIX + "." + KEYSTORE_PASSWORD, tlsConfig.getKeystorePassword())
                    .put(GRPC_TLS_PREFIX + "." + TRUSTSTORE_TYPE, tlsConfig.getTruststoreType())
                    .put(GRPC_TLS_PREFIX + "." + TRUSTSTORE_PATH, tlsConfig.getTruststorePath())
                    .put(GRPC_TLS_PREFIX + "." + TRUSTSTORE_PASSWORD, tlsConfig.getTruststorePassword())
                    .put(GRPC_TLS_PREFIX + "." + SSL_PROVIDER, tlsConfig.getSslProvider())
                    .buildOrThrow());
        }

        @Override
        public GrpcQueryClient create(HostAndPort hostAndPort)
        {
            return new GrpcQueryClient(hostAndPort.getHost(), hostAndPort.getPort(), config);
        }
    }

    public static class PinotGrpcServerQueryClient
    {
        private static final CalciteSqlCompiler REQUEST_COMPILER = new CalciteSqlCompiler();

        private final PinotHostMapper pinotHostMapper;
        private final Map<HostAndPort, GrpcQueryClient> clientCache = new ConcurrentHashMap<>();
        private final int grpcPort;
        private final GrpcQueryClientFactory grpcQueryClientFactory;
        private final Closer closer;

        private PinotGrpcServerQueryClient(PinotHostMapper pinotHostMapper, PinotGrpcServerQueryClientConfig pinotGrpcServerQueryClientConfig, GrpcQueryClientFactory grpcQueryClientFactory, Closer closer)
        {
            this.pinotHostMapper = requireNonNull(pinotHostMapper, "pinotHostMapper is null");
            requireNonNull(pinotGrpcServerQueryClientConfig, "pinotGrpcServerQueryClientConfig is null");
            this.grpcPort = pinotGrpcServerQueryClientConfig.getGrpcPort();
            this.grpcQueryClientFactory = requireNonNull(grpcQueryClientFactory, "grpcQueryClientFactory is null");
            this.closer = requireNonNull(closer, "closer is null");
        }

        public Iterator<PinotDataTableWithSize> queryPinot(ConnectorSession session, String query, String serverHost, List<String> segments)
        {
            HostAndPort mappedHostAndPort = pinotHostMapper.getServerGrpcHostAndPort(serverHost, grpcPort);
            // GrpcQueryClient does not implement Closeable. The idle timeout is 30 minutes (grpc default).
            GrpcQueryClient client = clientCache.computeIfAbsent(mappedHostAndPort, hostAndPort -> {
                GrpcQueryClient queryClient = grpcQueryClientFactory.create(hostAndPort);
                closer.register(queryClient::close);
                return queryClient;
            });
            BrokerRequest brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(query);
            GrpcRequestBuilder requestBuilder = new GrpcRequestBuilder()
                    .setSql(query)
                    .setSegments(segments)
                    .setEnableStreaming(true)
                    .setBrokerRequest(brokerRequest);
            return new ResponseIterator(client.submit(requestBuilder.build()));
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
