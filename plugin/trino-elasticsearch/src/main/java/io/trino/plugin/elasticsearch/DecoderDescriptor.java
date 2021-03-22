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
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.elasticsearch.decoders.DecoderType;
import io.trino.spi.type.Type;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class DecoderDescriptor
{
    private final String name;
    private final Map<String, DecoderDescriptor> children;
    private final DecoderType decoderType;

    public static final String ARRAY_ELEMENT_KEY = "_array$element";

    @JsonCreator
    public DecoderDescriptor(
            @JsonProperty("name") String name,
            @JsonProperty("children") Map<String, DecoderDescriptor> children,
            @JsonProperty("decoderType") DecoderType decoderType)
    {
        this.name = requireNonNull(name, "name is null");
        this.children = requireNonNull(children, "children is null");
        this.decoderType = requireNonNull(decoderType, "decoderType is null");
    }

    public static DecoderDescriptor primitiveDecoderDescriptor(String name, Type type)
    {
        return new DecoderDescriptor(name, ImmutableMap.of(), DecoderType.of(type));
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Map<String, DecoderDescriptor> getChildren()
    {
        return children;
    }

    @JsonProperty
    public DecoderType getDecoderType()
    {
        return decoderType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, children, decoderType);
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

        DecoderDescriptor other = (DecoderDescriptor) obj;
        return Objects.equals(this.getName(), other.getName()) &&
                Objects.equals(this.getChildren(), other.getChildren()) &&
                Objects.equals(this.getDecoderType(), other.getDecoderType());
    }

    @Override
    public String toString()
    {
        return name + ": " + decoderType;
    }
}
