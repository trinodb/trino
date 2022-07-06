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
package io.trino.plugin.mysql.debezium;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.trino.spi.HostAddress;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class DebeziumConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    private Set<HostAddress> nodes = ImmutableSet.of();
    private String serverName;

    @Size(min = 1)
    public Set<HostAddress> getNodes()
    {
        return nodes;
    }

    @Config("debezium.kafka.nodes")
    public DebeziumConfig setNodes(String nodes)
    {
        this.nodes = parseNodes(nodes);
        return this;
    }

    @NotNull
    public String getServerName()
    {
        return serverName;
    }

    @Config("debezium.server.name")
    public DebeziumConfig setServerName(String serverName)
    {
        this.serverName = serverName;
        return this;
    }

    private static Set<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return StreamSupport.stream(splitter.split(nodes).spliterator(), false)
                .map(DebeziumConfig::toHostAddress)
                .collect(toImmutableSet());
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }
}
