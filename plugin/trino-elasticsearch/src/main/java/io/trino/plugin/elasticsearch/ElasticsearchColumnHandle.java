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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class ElasticsearchColumnHandle
        implements ColumnHandle
{
    private final String name;
    private final Type type;
    private final DecoderDescriptor decoderDescriptor;
    private final boolean supportsPredicates;

    @JsonCreator
    public ElasticsearchColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("decoderDescriptor") DecoderDescriptor decoderDescriptor,
            @JsonProperty("supportsPredicates") boolean supportsPredicates)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.decoderDescriptor = requireNonNull(decoderDescriptor, "decoderDescriptor is null");
        this.supportsPredicates = supportsPredicates;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public DecoderDescriptor getDecoderDescriptor()
    {
        return decoderDescriptor;
    }

    @JsonProperty
    public boolean isSupportsPredicates()
    {
        return supportsPredicates;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, decoderDescriptor, supportsPredicates);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ElasticsearchColumnHandle other = (ElasticsearchColumnHandle) obj;
        return this.supportsPredicates == other.supportsPredicates &&
                Objects.equals(this.getName(), other.getName()) &&
                Objects.equals(this.getType(), other.getType()) &&
                Objects.equals(this.getDecoderDescriptor(), other.getDecoderDescriptor());
    }

    @Override
    public String toString()
    {
        return getName() + "::" + getType();
    }
}
