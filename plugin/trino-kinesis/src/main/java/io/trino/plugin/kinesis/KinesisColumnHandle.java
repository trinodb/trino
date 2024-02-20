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
package io.trino.plugin.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

//TODO: Use Optional for nullable fields, changes to be done across Kafka and Redis too
public class KinesisColumnHandle
        implements DecoderColumnHandle
{
    private final int ordinalPosition;
    private final String name;
    private final Type type;
    private final String mapping;

    private final String dataFormat; // Data format to use (selects the decoder). Can be null.
    private final String formatHint; // Additional format hint for the selected decoder. Selects a decoder subtype (e.g. which timestamp decoder).
    private final boolean hidden;
    private final boolean internal;

    //TODO: use Optional and check that Optional wrapper passed in is not null, across Kafka and Redis too
    @JsonCreator
    public KinesisColumnHandle(
            @JsonProperty("ordinalPosition") int ordinalPosition,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("formatHint") String formatHint,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("internal") boolean internal)
    {
        this.ordinalPosition = ordinalPosition;
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.mapping = mapping;
        this.dataFormat = dataFormat;
        this.formatHint = formatHint;
        this.hidden = hidden;
        this.internal = internal;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
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
    @Nullable
    @JsonProperty
    public String getMapping()
    {
        return mapping;
    }

    @Override
    @Nullable
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
        return Objects.hash(ordinalPosition, name, type, mapping, dataFormat, formatHint, hidden, internal);
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

        KinesisColumnHandle other = (KinesisColumnHandle) obj;
        return Objects.equals(this.ordinalPosition, other.ordinalPosition) &&
                Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.mapping, other.mapping) &&
                Objects.equals(this.dataFormat, other.dataFormat) &&
                Objects.equals(this.formatHint, other.formatHint) &&
                Objects.equals(this.hidden, other.hidden) &&
                Objects.equals(this.internal, other.internal);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("ordinalPosition", ordinalPosition)
                .add("name", name)
                .add("type", type)
                .add("mapping", mapping)
                .add("dataFormat", dataFormat)
                .add("formatHint", formatHint)
                .add("hidden", hidden)
                .add("internal", internal)
                .toString();
    }
}
