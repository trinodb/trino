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
package io.prestosql.plugin.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.plugin.kafka.encoder.EncoderColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class KafkaColumnHandle
        implements EncoderColumnHandle, DecoderColumnHandle
{
    /**
     * Column Name
     */
    private final String name;

    /**
     * Column type
     */
    private final Type type;

    /**
     * Mapping hint for the codec. Can be null.
     */
    private final String mapping;

    /**
     * Data format to use (selects the codec). Can be null.
     */
    private final String dataFormat;

    /**
     * Additional format hint for the selected codec. Selects a codec subtype (e.g. which timestamp codec).
     */
    private final String formatHint;

    /**
     * True if the key codec should be used, false if the message codec should be used.
     */
    private final boolean keyCodec;

    /**
     * True if the column should be hidden.
     */
    private final boolean hidden;

    /**
     * True if the column is internal to the connector and not defined by a topic definition.
     */
    private final boolean internal;

    @JsonCreator
    public KafkaColumnHandle(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("formatHint") String formatHint,
            @JsonProperty("keyCodec") boolean keyCodec,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("internal") boolean internal)
    {
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.mapping = mapping;
        this.dataFormat = dataFormat;
        this.formatHint = formatHint;
        this.keyCodec = keyCodec;
        this.hidden = hidden;
        this.internal = internal;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    @JsonProperty
    public String getMapping()
    {
        return mapping;
    }

    @Override
    @JsonProperty
    public String getDataFormat()
    {
        return dataFormat;
    }

    @Override
    @JsonProperty
    public String getFormatHint()
    {
        return formatHint;
    }

    @JsonProperty
    public boolean isKeyCodec()
    {
        return keyCodec;
    }

    @JsonProperty
    public boolean isHidden()
    {
        return hidden;
    }

    @Override
    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setHidden(hidden)
                .build();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, mapping, dataFormat, formatHint, keyCodec, hidden, internal);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KafkaColumnHandle other = (KafkaColumnHandle) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.mapping, other.mapping) &&
                Objects.equals(this.dataFormat, other.dataFormat) &&
                Objects.equals(this.formatHint, other.formatHint) &&
                Objects.equals(this.keyCodec, other.keyCodec) &&
                Objects.equals(this.hidden, other.hidden) &&
                Objects.equals(this.internal, other.internal);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("mapping", mapping)
                .add("dataFormat", dataFormat)
                .add("formatHint", formatHint)
                .add("keyCodec", keyCodec)
                .add("hidden", hidden)
                .add("internal", internal)
                .toString();
    }
}
