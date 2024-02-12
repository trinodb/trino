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
package io.trino.client.spooling;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static java.util.Objects.requireNonNull;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InlineSegment.class, name = "inline"),
        @JsonSubTypes.Type(value = SpooledSegment.class, name = "spooled")})
public abstract class Segment
{
    private final DataAttributes metadata;

    public Segment(DataAttributes metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @JsonProperty("metadata")
    public Map<String, Object> getJsonMetadata()
    {
        return metadata.attributes;
    }

    @JsonIgnore
    public DataAttributes getMetadata()
    {
        return metadata;
    }

    @JsonIgnore
    public long getOffset()
    {
        return getRequiredAttribute(ROW_OFFSET, Long.class);
    }

    @JsonIgnore
    public long getRowsCount()
    {
        return getRequiredAttribute(ROWS_COUNT, Long.class);
    }

    @JsonIgnore
    public int getSegmentSize()
    {
        return getRequiredAttribute(SEGMENT_SIZE, Integer.class);
    }

    public <T> Optional<T> getAttribute(DataAttribute name, Class<T> clazz)
    {
        return Optional.ofNullable(metadata.get(name, clazz));
    }

    public <T> T getRequiredAttribute(DataAttribute name, Class<T> clazz)
    {
        return getAttribute(name, clazz)
                .orElseThrow(() -> new IllegalArgumentException("Missing required attribute: " + name.attributeName()));
    }

    public static Segment inlined(byte[] data, DataAttributes attributes)
    {
        return new InlineSegment(data, attributes);
    }

    public static Segment spooled(URI segmentUri, DataAttributes attributes, Map<String, List<String>> headers)
    {
        return new SpooledSegment(segmentUri, attributes, headers);
    }
}
