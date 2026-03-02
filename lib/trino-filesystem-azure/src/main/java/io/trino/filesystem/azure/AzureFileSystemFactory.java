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
package io.trino.filesystem.azure;

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.tracing.opentelemetry.OpenTelemetryTracingOptions;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.TracingOptions;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import reactor.netty.resources.ConnectionProvider;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class AzureFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final ExecutorService uploadExecutor = newCachedThreadPool(daemonThreadsNamed("azure-upload-%s"));

    private final AzureAuth auth;
    private final String endpoint;
    private final DataSize readBlockSize;
    private final DataSize writeBlockSize;
    private final int maxWriteConcurrency;
    private final DataSize maxSingleUploadSize;
    private final TracingOptions tracingOptions;
    private final HttpClient httpClient;
    private final ConnectionProvider connectionProvider;
    private final EventLoopGroup eventLoopGroup;
    private final boolean multipart;
    private final HttpPipelinePolicy concurrencyPolicy;

    @Inject
    public AzureFileSystemFactory(OpenTelemetry openTelemetry, AzureAuth azureAuth, AzureFileSystemConfig config)
    {
        this(openTelemetry,
                azureAuth,
                config.getEndpoint(),
                config.getReadBlockSize(),
                config.getWriteBlockSize(),
                config.getMaxWriteConcurrency(),
                config.getMaxSingleUploadSize(),
                config.getMaxHttpRequests(),
                config.getMaxHttpConnections(),
                config.getApplicationId(),
                config.isMultipartWriteEnabled());
    }

    public AzureFileSystemFactory(
            OpenTelemetry openTelemetry,
            AzureAuth azureAuth,
            String endpoint,
            DataSize readBlockSize,
            DataSize writeBlockSize,
            int maxWriteConcurrency,
            DataSize maxSingleUploadSize,
            int maxHttpRequests,
            int maxHttpConnections,
            String applicationId,
            boolean multipart)
    {
        this.auth = requireNonNull(azureAuth, "azureAuth is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.readBlockSize = requireNonNull(readBlockSize, "readBlockSize is null");
        this.writeBlockSize = requireNonNull(writeBlockSize, "writeBlockSize is null");
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.maxSingleUploadSize = requireNonNull(maxSingleUploadSize, "maxSingleUploadSize is null");
        this.tracingOptions = new OpenTelemetryTracingOptions().setOpenTelemetry(openTelemetry);
        this.connectionProvider = ConnectionProvider.create(applicationId, maxHttpConnections);
        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        HttpClientOptions clientOptions = new HttpClientOptions();
        clientOptions.setTracingOptions(tracingOptions);
        clientOptions.setApplicationId(applicationId);
        clientOptions.setMaximumConnectionPoolSize(maxHttpConnections);
        httpClient = createAzureHttpClient(connectionProvider, eventLoopGroup, clientOptions);
        this.multipart = multipart;
        this.concurrencyPolicy = new ConcurrencyLimitHttpPipelinePolicy(maxHttpRequests);
    }

    @PreDestroy
    public void destroy()
    {
        uploadExecutor.shutdown();

        if (connectionProvider != null) {
            connectionProvider.dispose();
        }
        if (eventLoopGroup != null) {
            try {
                eventLoopGroup.shutdownGracefully().get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException _) {
                // ignored
            }
        }
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new AzureFileSystem(httpClient, concurrencyPolicy, uploadExecutor, tracingOptions, auth, endpoint, readBlockSize, writeBlockSize, maxWriteConcurrency, maxSingleUploadSize, multipart);
    }

    public static HttpClient createAzureHttpClient(ConnectionProvider connectionProvider, EventLoopGroup eventLoopGroup, HttpClientOptions clientOptions)
    {
        return new NettyAsyncHttpClientBuilder()
                .proxy(clientOptions.getProxyOptions())
                .configuration(clientOptions.getConfiguration())
                .connectTimeout(clientOptions.getConnectTimeout())
                .writeTimeout(clientOptions.getWriteTimeout())
                .readTimeout(clientOptions.getReadTimeout())
                .responseTimeout(clientOptions.getResponseTimeout())
                .connectionProvider(connectionProvider)
                .eventLoopGroup(eventLoopGroup)
                .build();
    }
}
