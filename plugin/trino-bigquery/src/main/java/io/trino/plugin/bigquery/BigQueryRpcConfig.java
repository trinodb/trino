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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigHidden;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

public class BigQueryRpcConfig
{
    private static final int MAX_RPC_CONNECTIONS = 1024;

    private int rpcInitialChannelCount = 1;
    private int rpcMinChannelCount = 1;
    private int rpcMaxChannelCount = 1;
    private int minRpcPerChannel;
    private int maxRpcPerChannel = Integer.MAX_VALUE;

    @Min(1)
    @Max(MAX_RPC_CONNECTIONS)
    public int getRpcInitialChannelCount()
    {
        return rpcInitialChannelCount;
    }

    @ConfigHidden
    @Config("bigquery.channel-pool.initial-size")
    public BigQueryRpcConfig setRpcInitialChannelCount(int rpcInitialChannelCount)
    {
        this.rpcInitialChannelCount = rpcInitialChannelCount;
        return this;
    }

    @Min(1)
    @Max(MAX_RPC_CONNECTIONS)
    public int getRpcMinChannelCount()
    {
        return rpcMinChannelCount;
    }

    @ConfigHidden
    @Config("bigquery.channel-pool.min-size")
    public BigQueryRpcConfig setRpcMinChannelCount(int rpcMinChannelCount)
    {
        this.rpcMinChannelCount = rpcMinChannelCount;
        return this;
    }

    @Min(1)
    @Max(MAX_RPC_CONNECTIONS)
    public int getRpcMaxChannelCount()
    {
        return rpcMaxChannelCount;
    }

    @ConfigHidden
    @Config("bigquery.channel-pool.max-size")
    public BigQueryRpcConfig setRpcMaxChannelCount(int rpcMaxChannelCount)
    {
        this.rpcMaxChannelCount = rpcMaxChannelCount;
        return this;
    }

    @Min(0)
    public int getMinRpcPerChannel()
    {
        return minRpcPerChannel;
    }

    @ConfigHidden
    @Config("bigquery.channel-pool.min-rpc-per-channel")
    public BigQueryRpcConfig setMinRpcPerChannel(int minRpcPerChannel)
    {
        this.minRpcPerChannel = minRpcPerChannel;
        return this;
    }

    @Min(1)
    public int getMaxRpcPerChannel()
    {
        return maxRpcPerChannel;
    }

    @ConfigHidden
    @Config("bigquery.channel-pool.max-rpc-per-channel")
    public BigQueryRpcConfig setMaxRpcPerChannel(int maxRpcPerChannel)
    {
        this.maxRpcPerChannel = maxRpcPerChannel;
        return this;
    }
}
