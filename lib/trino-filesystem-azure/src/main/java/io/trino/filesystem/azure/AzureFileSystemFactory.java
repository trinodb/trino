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
import com.azure.core.tracing.opentelemetry.OpenTelemetryTracingOptions;
import com.azure.core.util.HttpClientOptions;
import com.azure.core.util.TracingOptions;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.netty.channel.nio.NioEventLoopGroup;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import reactor.netty.resources.ConnectionProvider;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AzureFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final AzureAuth auth;
    private final String endpoint;
    private final DataSize readBlockSize;
    private final DataSize writeBlockSize;
    private final int maxWriteConcurrency;
    private final DataSize maxSingleUploadSize;
    private final TracingOptions tracingOptions;
    private final HttpClient httpClient;
    private final ConnectionProvider connectionProvider;

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
                config.getMaxHttpRequests());
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
            String applicationId)
    {
        this.auth = requireNonNull(azureAuth, "azureAuth is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.readBlockSize = requireNonNull(readBlockSize, "readBlockSize is null");
        this.writeBlockSize = requireNonNull(writeBlockSize, "writeBlockSize is null");
        checkArgument(maxWriteConcurrency >= 0, "maxWriteConcurrency is negative");
        this.maxWriteConcurrency = maxWriteConcurrency;
        this.maxSingleUploadSize = requireNonNull(maxSingleUploadSize, "maxSingleUploadSize is null");
        this.tracingOptions = new OpenTelemetryTracingOptions().setOpenTelemetry(openTelemetry);
        this.connectionProvider = ConnectionProvider.create(applicationId, maxHttpRequests);
        HttpClientOptions clientOptions = new HttpClientOptions();
        clientOptions.setTracingOptions(tracingOptions);
        clientOptions.setMaximumConnectionPoolSize(maxHttpRequests);
        httpClient = createAzureHttpClient(connectionProvider, clientOptions);
    }

    @PreDestroy
    public void destroy()
    {
        if (connectionProvider != null) {
            connectionProvider.dispose();
        }
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new AzureFileSystem(httpClient, tracingOptions, auth, endpoint, readBlockSize, writeBlockSize, maxWriteConcurrency, maxSingleUploadSize);
    }

    public static HttpClient createAzureHttpClient(ConnectionProvider connectionProvider, HttpClientOptions clientOptions)
    {
        Integer poolSize = clientOptions.getMaximumConnectionPoolSize();
        // By default, OkHttp uses a maximum idle connection count of 5.
        int maximumConnectionPoolSize = (poolSize != null && poolSize > 0) ? poolSize : 5;

        return new NettyAsyncHttpClientBuilder()
                .proxy(clientOptions.getProxyOptions())
                .configuration(clientOptions.getConfiguration())
                .connectTimeout(clientOptions.getConnectTimeout())
                .writeTimeout(clientOptions.getWriteTimeout())
                .readTimeout(clientOptions.getReadTimeout())
                .connectionProvider(connectionProvider)
                .eventLoopGroup(new NioEventLoopGroup(maximumConnectionPoolSize)) // default is 2 * availableProcessors
                .build();
    }
}
