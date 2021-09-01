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
package io.trino.plugin.pulsar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_DATA_FORMAT;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_FORMAT_HINT;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_HANDLE_TYPE;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_INTERNAL;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_MAPPING;
import static io.trino.plugin.pulsar.PulsarColumnMetadata.PROPERTY_KEY_NAME_CASE_SENSITIVE;
import static java.util.Objects.requireNonNull;

public class PulsarColumnHandle
        implements DecoderColumnHandle
{
    private final String catalogName;

    private final String name;

    private final Type type;

    private final boolean hidden;

    private final boolean internal;

    private HandleKeyValueType handleKeyValueType;

    /**
     * {@link PulsarColumnMetadata.DecoderExtraInfo#mapping}.
     */
    private String mapping;
    /**
     * {@link PulsarColumnMetadata.DecoderExtraInfo#dataFormat}.
     */
    private String dataFormat;

    /**
     * {@link PulsarColumnMetadata.DecoderExtraInfo#formatHint}.
     */
    private String formatHint;

    /**
     * Column Handle keyValue type, used for keyValue schema.
     */
    public enum HandleKeyValueType
    {
        /**
         * The handle not for keyValue schema.
         */
        NONE,
        /**
         * The key schema handle for keyValue schema.
         */
        KEY,
        /**
         * The value schema handle for keyValue schema.
         */
        VALUE
    }

    @JsonCreator
    public PulsarColumnHandle(
            @JsonProperty("catalogName") String catalogName,
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("hidden") boolean hidden,
            @JsonProperty("internal") boolean internal,
            @JsonProperty("mapping") String mapping,
            @JsonProperty("dataFormat") String dataFormat,
            @JsonProperty("formatHint") String formatHint,
            @JsonProperty("handleKeyValueType") Optional<HandleKeyValueType> handleKeyValueType)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.hidden = hidden;
        this.internal = internal;
        this.mapping = mapping;
        this.dataFormat = dataFormat;
        this.formatHint = formatHint;
        this.handleKeyValueType = handleKeyValueType.isPresent() ? handleKeyValueType.get() : HandleKeyValueType.NONE;
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @Override
    @JsonProperty
    public String getName()
    {
        return name;
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
    public Type getType()
    {
        return type;
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

    @Override
    @JsonProperty
    public String getFormatHint()
    {
        return formatHint;
    }

    @JsonProperty
    public HandleKeyValueType getHandleKeyValueType()
    {
        return handleKeyValueType;
    }

    @JsonIgnore
    public boolean isKey()
    {
        return Objects.equals(handleKeyValueType, HandleKeyValueType.KEY);
    }

    @JsonIgnore
    public boolean isValue()
    {
        return Objects.equals(handleKeyValueType, HandleKeyValueType.VALUE);
    }

    ColumnMetadata getColumnMetadata()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put(PROPERTY_KEY_NAME_CASE_SENSITIVE, name);
        properties.put(PROPERTY_KEY_INTERNAL, internal);
        properties.put(PROPERTY_KEY_HANDLE_TYPE, handleKeyValueType);
        properties.put(PROPERTY_KEY_MAPPING, mapping);
        properties.put(PROPERTY_KEY_DATA_FORMAT, dataFormat);
        properties.put(PROPERTY_KEY_FORMAT_HINT, formatHint);
        return ColumnMetadata.builder()
                .setName(name)
                .setType(type)
                .setHidden(hidden)
                .setProperties(properties)
                .build();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PulsarColumnHandle)) {
            return false;
        }
        PulsarColumnHandle that = (PulsarColumnHandle) o;
        return isHidden() == that.isHidden() &&
                isInternal() == that.isInternal() &&
                Objects.equals(getCatalogName(), that.getCatalogName()) &&
                Objects.equals(getName(), that.getName()) &&
                Objects.equals(getType(), that.getType()) &&
                getHandleKeyValueType() == that.getHandleKeyValueType() &&
                Objects.equals(getMapping(), that.getMapping()) &&
                Objects.equals(getDataFormat(), that.getDataFormat()) &&
                Objects.equals(getFormatHint(), that.getFormatHint());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getCatalogName(), getName(), getType(), isHidden(), isInternal(), getHandleKeyValueType(), getMapping(), getDataFormat(), getFormatHint());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogName", catalogName)
                .add("name", name)
                .add("type", type)
                .add("hidden", hidden)
                .add("internal", internal)
                .add("handleKeyValueType", handleKeyValueType)
                .add("mapping", mapping)
                .add("dataFormat", dataFormat)
                .add("formatHint", formatHint)
                .toString();
    }
}
