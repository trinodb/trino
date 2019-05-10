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
package io.prestosql.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates the shard
 */
public class ElasticsearchShard
{
    private final String index;
    private final String type;
    private final int id;
    private final String host;
    private final int port;

    @JsonCreator
    public ElasticsearchShard(
            @JsonProperty("index") String index,
            @JsonProperty("type") String type,
            @JsonProperty("id") int id,
            @JsonProperty("host") String host,
            @JsonProperty("port") int port)
    {
        this.index = requireNonNull(index, "index is null");
        this.type = requireNonNull(type, "type is null");
        this.id = id;
        this.host = requireNonNull(host, "host is null");
        this.port = requireNonNull(port, "port is null");
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public int getId()
    {
        return id;
    }

    @JsonProperty
    public String getHost()
    {
        return host;
    }

    @JsonProperty
    public int getPort()
    {
        return port;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ElasticsearchShard)) {
            return false;
        }

        ElasticsearchShard that = (ElasticsearchShard) o;
        return id == that.id &&
                port == that.port &&
                Objects.equals(index, that.index) &&
                Objects.equals(type, that.type) &&
                Objects.equals(host, that.host);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(index, type, id, host, port);
    }
}
