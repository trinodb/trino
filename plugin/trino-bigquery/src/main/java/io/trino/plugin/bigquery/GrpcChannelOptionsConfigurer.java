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
package io.trino.plugin.bigquery;

import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

public class GrpcChannelOptionsConfigurer
        implements BigQueryGrpcOptionsConfigurer
{
    private final ChannelPoolSettings channelPoolSettings;

    @Inject
    public GrpcChannelOptionsConfigurer(BigQueryRpcConfig rpcConfig)
    {
        this.channelPoolSettings = ChannelPoolSettings.builder()
                .setInitialChannelCount(rpcConfig.getRpcInitialChannelCount())
                .setMinChannelCount(rpcConfig.getRpcMinChannelCount())
                .setMaxChannelCount(rpcConfig.getRpcMaxChannelCount())
                .setMinRpcsPerChannel(rpcConfig.getMinRpcPerChannel())
                .setMaxRpcsPerChannel(rpcConfig.getMaxRpcPerChannel())
                .build();
    }

    @Override
    public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
    {
        return builder;
    }

    @Override
    public InstantiatingGrpcChannelProvider.Builder configure(InstantiatingGrpcChannelProvider.Builder channelBuilder, ConnectorSession session)
    {
        return channelBuilder.setChannelPoolSettings(channelPoolSettings);
    }
}
