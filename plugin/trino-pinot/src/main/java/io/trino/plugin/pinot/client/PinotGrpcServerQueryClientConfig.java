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

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;

import java.util.Optional;

import static org.apache.pinot.common.config.GrpcConfig.DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE;

public class PinotGrpcServerQueryClientConfig
{
    private int maxRowsPerSplitForSegmentQueries = Integer.MAX_VALUE - 1;
    private int grpcPort = 8090;
    private DataSize maxInboundMessageSize = DataSize.ofBytes(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE);
    private boolean usePlainText = true;
    private Optional<String> proxyUri = Optional.empty();

    public int getMaxRowsPerSplitForSegmentQueries()
    {
        return maxRowsPerSplitForSegmentQueries;
    }

    @Config("pinot.max-rows-per-split-for-segment-queries")
    public PinotGrpcServerQueryClientConfig setMaxRowsPerSplitForSegmentQueries(int maxRowsPerSplitForSegmentQueries)
    {
        this.maxRowsPerSplitForSegmentQueries = maxRowsPerSplitForSegmentQueries;
        return this;
    }

    public int getGrpcPort()
    {
        return grpcPort;
    }

    @Config("pinot.grpc.port")
    public PinotGrpcServerQueryClientConfig setGrpcPort(int grpcPort)
    {
        this.grpcPort = grpcPort;
        return this;
    }

    public DataSize getMaxInboundMessageSize()
    {
        return maxInboundMessageSize;
    }

    @Config("pinot.grpc.max-inbound-message-size")
    public PinotGrpcServerQueryClientConfig setMaxInboundMessageSize(DataSize maxInboundMessageSize)
    {
        this.maxInboundMessageSize = maxInboundMessageSize;
        return this;
    }

    public boolean isUsePlainText()
    {
        return usePlainText;
    }

    @Config("pinot.grpc.use-plain-text")
    public PinotGrpcServerQueryClientConfig setUsePlainText(boolean usePlainText)
    {
        this.usePlainText = usePlainText;
        return this;
    }

    public Optional<String> getProxyUri()
    {
        return proxyUri;
    }

    @Config("pinot.grpc.proxy-uri")
    public PinotGrpcServerQueryClientConfig setProxyUri(String proxyUri)
    {
        if (proxyUri != null && !proxyUri.isEmpty()) {
            this.proxyUri = Optional.of(proxyUri);
        }
        return this;
    }
}
